package org.apache.hadoop.ipc;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.metrics.RpcRateLimiterMetrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;

import org.apache.hadoop.security.bzl.util.BzlHttpUtils;
import org.apache.hadoop.top.TopConf;
import org.apache.hadoop.top.metrics.TopMetrics;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpException;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_ENABLE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_LOCAL_CONFIG_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_LOCAL_CONFIG_ENABLE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_MISMATCH_REJECT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_MISMATCH_REJECT_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_URL;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT_DEFAULT;

public class RpcRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(RpcRateLimiter.class);
  private static final Logger
      LOG_MISMATCH = LoggerFactory.getLogger(RpcRateLimiter.class.getName() + ".mismatch");

  private static final RpcRateLimiter INSTANCE = new RpcRateLimiter();
  List<LimitCondition> conditionList = new ArrayList<>();
  private final ReentrantReadWriteLock mReadWriteLock =
      new ReentrantReadWriteLock();
  private final Lock mReadLock  = mReadWriteLock.readLock();
  private final Lock mWriteLock = mReadWriteLock.writeLock();
  RpcRateLimiterMetrics rpcRateLimiterMetrics;
  private final TopMetrics successTopMetrics;
  private final TopMetrics refusedTopMetrics;


  private RpcRateLimiter() {
    rpcRateLimiterMetrics = RpcRateLimiterMetrics.create();

    Configuration conf = new Configuration();
    TopConf topConf = new TopConf(conf);
    this.successTopMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
    if (DefaultMetricsSystem.instance().getSource(
        "rpcRateLimiterSuccessTopMetrics") == null) {
      DefaultMetricsSystem.instance().register("rpcRateLimiterSuccessTopMetrics",
          "Top N operations by user with Subnet with sucesses", successTopMetrics);
    }

    this.refusedTopMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
    if (DefaultMetricsSystem.instance().getSource(
        "rpcRateLimiterRefusedTopMetrics") == null) {
      DefaultMetricsSystem.instance().register("rpcRateLimiterRefusedTopMetrics",
          "Top N operations by user with Subnet with refused", refusedTopMetrics);
    }

    new RefreshRpcRateLimitThread().start();

    new RpcRateLimiterGlobalEnableThread().start();
  }

  void readLock() {
    mReadLock.lock();
  }

  void readUnlock() {
    mReadLock.unlock();
  }

  void writeLock() {
    mWriteLock.lock();
  }

  void writeUnlock() {
    mWriteLock.unlock();
  }

  public static RpcRateLimiter getInstance() {
    return INSTANCE;
  }

  private String getRealClientIp() {
    CallerContext callerContext = CallerContext.getCurrent();
    String clientIp = null;
    if (callerContext != null) {
      LOG.debug("CallerContext context is : {}", callerContext.getContext());
      clientIp = callerContext.getClientIpStr();
    }
    return clientIp;
  }

  public void rateLimit(String protocolName, String methodName, String ip, String user)
      throws Exception {
    if (!RpcRateLimiterGlobalEnableThread.isGlobalEnable()) {
      return;
    }

    if (!BzlDynamicConfiguration.getInstance()
        .getBoolean(IPC_SERVER_RATE_LIMIT_ENABLE, IPC_SERVER_RATE_LIMIT_ENABLE_DEFAULT)) {
      return;
    }

    long start = Time.monotonicNowNanos();
    String clientIp = getRealClientIp();
    if (clientIp != null) {
      ip = clientIp;
    }

    LimitCondition limitCondition = null;
    try {
      readLock();
      try {
        if (conditionList.size() == 0) {
          return;
        }
        limitCondition = matchLimitCondition(protocolName, methodName, ip, user);
      } finally {
        readUnlock();
        rpcRateLimiterMetrics.addRpcLimitConditionReadLock(Time.monotonicNowNanos() - start);
      }
      if (limitCondition == null) {
        long limitConditionNullStart = Time.monotonicNowNanos();
        LOG_MISMATCH.debug("{},{},{},{}", ip, user,
            protocolName, methodName);
        if (BzlDynamicConfiguration.getInstance()
            .getBoolean(IPC_SERVER_RATE_LIMIT_MISMATCH_REJECT,
                IPC_SERVER_RATE_LIMIT_MISMATCH_REJECT_DEFAULT)) {
          rpcRateLimiterMetrics.addRpcRateLimitMismatch(
              Time.monotonicNowNanos() - limitConditionNullStart);
          throw new RpcServerException("The request is refused for security reasons.");
        }
        rpcRateLimiterMetrics.addRpcRateLimitMismatch(
            Time.monotonicNowNanos() - limitConditionNullStart);
        return;
      }

      long tryAcquireStart = Time.monotonicNowNanos();
      limitCondition.tryAcquire(user, methodName);
      rpcRateLimiterMetrics.addRpcRateLimitTryAcquire(Time.monotonicNowNanos() - tryAcquireStart);
    } finally {
      rpcRateLimiterMetrics.addRpcRateLimit(Time.monotonicNowNanos() - start);
    }
  }

  private LimitCondition matchLimitCondition(String protocolName, String methodName, String ip,
                                             String user) {
    for (LimitCondition limitCondition : conditionList) {
      if (limitCondition.match(protocolName, methodName, ip, user)) {
        return limitCondition;
      }
    }
    return null;
  }

  private class LimitCondition implements Comparable<LimitCondition> {
    private String protocolName;
    private String methodName;
    private String subNet;
    private String user;
    private String qps;
    private  SubnetUtils subnetUtils;
    private ConcurrentHashMap<String, RateLimiterExtension> userRateLimiterExtension;

    public LimitCondition(String protocolName, String methodName, String subNet, String user,
                          String qps) {
      this.protocolName = protocolName;
      this.methodName = methodName;
      this.subNet = subNet;
      this.subnetUtils = new SubnetUtils(subNet);
      this.subnetUtils.setInclusiveHostCount(true);
      this.user = user;
      this.qps = qps;
      this.userRateLimiterExtension =  new ConcurrentHashMap<>();
    }

    public boolean match(String protocolName, String methodName, String ip, String user) {
      return checkClientMatchRule(this.protocolName, protocolName) &&
          checkClientMatchRule(this.methodName, methodName) && checkIp(ip) &&
          checkClientMatchRule(this.user, user);
    }

    private boolean checkIp(String ip) {
      return subnetUtils.getInfo().isInRange(ip);
    }

    private boolean checkClientMatchRule(String rule, String client) {
      if (rule.equals("*") || rule.equals(client)) {
        return true;
      }
      return false;
    }

    private void tryAcquire(String user ,String methodName) throws Exception {
      userRateLimiterExtension.putIfAbsent(user, new RateLimiterExtension(qps));
      userRateLimiterExtension.get(user).tryAcquire(this.subNet, user ,methodName);
    }

    @Override
    public int compareTo(LimitCondition o) {
      if (this.qps.equals("*")) {
        return 1;
      }
      if (o.qps.equals("*")) {
        return -1;
      }

      if (Double.valueOf(this.qps) > Double.valueOf(o.qps)) {
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public String toString() {
      return "LimitCondition{" +
          "protocolName='" + protocolName + '\'' +
          ", methodName='" + methodName + '\'' +
          ", subNet='" + subNet + '\'' +
          ", user='" + user + '\'' +
          ", qps='" + qps + '\'' +
          ", userRateLimiterExtension=" + userRateLimiterExtension +
          '}';
    }
  }

  private class RateLimiterExtension {
    String qps;
    RateLimiter rateLimiter;

    public RateLimiterExtension(String qps) {
      this.qps = qps;

      if (!qps.equals("0") && !qps.equals("*")) {
        rateLimiter = RateLimiter.create(Double.valueOf(qps));
      }
    }

    private void tryAcquire(String subNet, String user, String methodName) throws Exception {
      if (qps.equals("0")) {
        rpcRateLimiterMetrics.incrRpcRateLimitRefusedNum();
        refusedTopMetrics.report(subNet + "_" + user, methodName);
        throw new RpcServerException("The request is refused for security reasons.");
      }
      if (qps.equals("*")) {
        successTopMetrics.report(subNet + "_" + user, methodName);
        return;
      }

      boolean bool = rateLimiter.tryAcquire(BzlDynamicConfiguration.getInstance()
          .getLong(IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT,
              IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT_DEFAULT), TimeUnit.MICROSECONDS);
      if (!bool) {
        rpcRateLimiterMetrics.incrRpcRateLimitSuppressedNum();
        refusedTopMetrics.report(subNet + "_" + user, methodName);
        throw new RetriableException("Rpc rate limitation is reached for security reasons.");
      }
      successTopMetrics.report(subNet + "_" + user, methodName);
    }

    @Override
    public String toString() {
      return "RateLimiterExtension{" +
          "qps='" + qps + '\'' +
          ", rateLimiter=" + rateLimiter +
          '}';
    }
  }

  public class RefreshRpcRateLimitThread extends Thread {
    public RefreshRpcRateLimitThread() {
      this.setName("RefreshRpcRateLimitThread");
      this.setDaemon(true);
    }

    @Override
    public void run() {
      LOG.info("RefreshRpcRateLimitThread start.");

      String oldValue = null;
      while (true) {
        if (BzlDynamicConfiguration.getInstance()
            .getBoolean(IPC_SERVER_RATE_LIMIT_ENABLE, IPC_SERVER_RATE_LIMIT_ENABLE_DEFAULT)) {
          String newValue = null;
          if (BzlDynamicConfiguration.getInstance()
              .getBoolean(IPC_SERVER_RATE_LIMIT_LOCAL_CONFIG_ENABLE,
                  IPC_SERVER_RATE_LIMIT_LOCAL_CONFIG_ENABLE_DEFAULT)) {
            newValue = BzlDynamicConfiguration.getInstance()
                .get(IPC_SERVER_RATE_LIMIT_RULES, IPC_SERVER_RATE_LIMIT_RULES_DEFAULT);
          } else {
            // If requests URL failed , newValue will be null.
            // If successes but the rules is empty, newValue will be ""
            try {
              newValue = getRateLimiterRules();
            } catch (Exception e) {
              LOG.warn("getRateLimterRules throw Exception:", e);
              newValue = null;
            }
          }

          if (newValue != null && !newValue.equals(oldValue)) {
            try {
              List<LimitCondition> list = getRateLimitList(newValue);
              long start = Time.monotonicNowNanos();
              writeLock();
              try {
                conditionList.clear();
                conditionList.addAll(list);
              } finally {
                writeUnlock();
                rpcRateLimiterMetrics.addRpcLimitConditionWriteLock(
                    Time.monotonicNowNanos() - start);
              }
              LOG.info("The {} has changed. Details is {}", IPC_SERVER_RATE_LIMIT_RULES, list);
              oldValue = newValue;
            } catch (Exception e) {
              LOG.error("Apply new ratelimit rules to conditionList throw exception:", e);
              rpcRateLimiterMetrics.addRpcRateLimitParsingFormatFailures();
            }
          }
        }

        try {
          Thread.sleep(BzlDynamicConfiguration.getInstance()
              .getLong(IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD,
                  IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT));
        } catch (InterruptedException e) {
          LOG.warn("RefreshRpcRateLimitThread catch InterruptedException:", e);
          Thread.currentThread().interrupt();
        }
      }
    }


    private List<LimitCondition> getRateLimitList(String rateLimitConfig) {
      List<LimitCondition> list = new ArrayList<>();
      if (rateLimitConfig == null || rateLimitConfig.equals("")) {
        return list;
      }
      String rateLimits[] = rateLimitConfig.split(";");

      for (int i = 0; i < rateLimits.length; i++) {
        String rateLimit[] = rateLimits[i].split(":");
        if (rateLimit.length != 2) {
          LOG.error("Wrong config for {}. Detail is {}", IPC_SERVER_RATE_LIMIT_RULES,
              rateLimits[i]);
          rpcRateLimiterMetrics.addRpcRateLimitParsingFormatFailures();
          continue;
        }

        String limitKeys[] = rateLimit[0].split(",");
        String qps = rateLimit[1];
        if (limitKeys.length != 4 ||
            StringUtils.isBlank(limitKeys[0]) ||
            StringUtils.isBlank(limitKeys[1]) || !checkSubNetFormat(limitKeys[2]) ||
            StringUtils.isBlank(limitKeys[3]) ||
            !checkQpsFormat(qps)) {
          LOG.error("Wrong config for {}. Detail is {}", IPC_SERVER_RATE_LIMIT_RULES,
              rateLimits[i]);
          rpcRateLimiterMetrics.addRpcRateLimitParsingFormatFailures();
          continue;
        }

        list.add(new LimitCondition(limitKeys[0], limitKeys[1], limitKeys[2], limitKeys[3], qps));
        rpcRateLimiterMetrics.addRpcRateLimitParsingFormatSuccesses();
      }
      Collections.sort(list);
      return list;
    }

    private boolean checkSubNetFormat(String subNet) {
      if (subNet == null) {
        return false;
      }

      String subNetParts[] = subNet.split("/");
      if (subNetParts.length != 2) {
        return false;
      }

      String ip = subNetParts[0];
      String mask = subNetParts[1];

      if (!(InetAddresses.isInetAddress(ip) && ip.contains("."))) {
        return false;
      }

      if (!mask.chars().allMatch(Character::isDigit)) {
        return false;
      }

      int maskInt = Integer.valueOf(mask);
      if (!(maskInt >= 0 && maskInt <= 32)) {
        return false;
      }

      return true;
    }

    private boolean checkQpsFormat(String qps) {
      return StringUtils.isNotBlank(qps) && ((StringUtils.isNumeric(qps) &&
          Double.parseDouble(qps) >= 1.0) || qps.equals("*") || qps.equals("0"));
    }

    public String getRateLimiterRules() throws HttpException, IOException, URISyntaxException {
      String traceId = UUID.randomUUID().toString();
      String url = BzlDynamicConfiguration.getInstance().get(IPC_SERVER_RATE_LIMIT_RULES_URL, "");
      URI uri = new URIBuilder(url).setParameter("traceId", traceId).build();

      try {
        String json = BzlHttpUtils.doGet(uri, traceId, "[BDH]rpcRateLimiter");
        rpcRateLimiterMetrics.addRpcRateLimitFetchSuccesses();

        String rpcRateLimiterRules = parseJson(json);
        return rpcRateLimiterRules;
      } catch (Exception e) {
        rpcRateLimiterMetrics.addRpcRateLimitFetchFailures();
        throw e;
      }
    }

    private String parseJson(String json) {
      if (json == null) {
        return null;
      }
      Gson gson = new Gson();
      Map<String, String>
          rs = gson.fromJson(json, Map.class);
      return rs.get("data");
    }
    
  }

  @VisibleForTesting
  public RefreshRpcRateLimitThread getRefreshRpcRateLimitThread() {
     return  new RefreshRpcRateLimitThread();
  }
}
