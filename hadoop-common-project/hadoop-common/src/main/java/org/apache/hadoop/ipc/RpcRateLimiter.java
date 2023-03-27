package org.apache.hadoop.ipc;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.ipc.metrics.RpcRateLimiterMetrics;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_ENABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_ENABLE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT_DEFAULT;

public class RpcRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(RpcRateLimiter.class);
  private static final RpcRateLimiter INSTANCE = new RpcRateLimiter();
  List<LimitCondition> conditionList = new CopyOnWriteArrayList<>();
  RpcRateLimiterMetrics rpcRateLimiterMetrics;

  private RpcRateLimiter() {
    rpcRateLimiterMetrics = RpcRateLimiterMetrics.create();
    new RefreshRpcRateLimitThread().start();
  }

  public static RpcRateLimiter getInstance() {
    return INSTANCE;
  }

  public void rateLimit(String protocolName, String methodName, String ip, String user)
      throws Exception {
    if (!BzlDynamicConfiguration.getInstance()
        .getBoolean(IPC_SERVER_RATE_LIMIT_ENABLE, IPC_SERVER_RATE_LIMIT_ENABLE_DEFAULT)) {
      return;
    }

    long start = Time.monotonicNowNanos();

    try {
      LimitCondition limitCondition = matchLimitCondition(protocolName, methodName, ip, user);
      if (limitCondition == null) {
        return;
      }
      limitCondition.tryAcquire();
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
    private RateLimiterExtension rateLimiterExtension;

    public LimitCondition(String protocolName, String methodName, String subNet, String user,
                          String qps) {
      this.protocolName = protocolName;
      this.methodName = methodName;
      this.subNet = subNet;
      this.subnetUtils = new SubnetUtils(subNet);
      this.subnetUtils.setInclusiveHostCount(true);
      this.user = user;
      this.qps = qps;
      this.rateLimiterExtension = new RateLimiterExtension(qps);
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

    private void tryAcquire() throws Exception {
      rateLimiterExtension.tryAcquire();
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
          ", rateLimiterExtension=" + rateLimiterExtension +
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

    private void tryAcquire() throws Exception {
      if (qps.equals("0")) {
        rpcRateLimiterMetrics.incrRpcRateLimitRefusedNum();
        throw new RpcServerException("The request is refused for security reasons.");
      }
      if (qps.equals("*")) {
        return;
      }

      boolean bool = rateLimiter.tryAcquire(BzlDynamicConfiguration.getInstance()
          .getLong(IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT,
              IPC_SERVER_RATE_LIMIT_TRYACQUIRE_TIMEOUT_DEFAULT), TimeUnit.MICROSECONDS);
      if (!bool) {
        rpcRateLimiterMetrics.incrRpcRateLimitSuppressedNum();
        throw new RetriableException("Rpc rate limitation is reached for security reasons.");
      }
    }

    @Override
    public String toString() {
      return "RateLimiterExtension{" +
          "qps='" + qps + '\'' +
          ", rateLimiter=" + rateLimiter +
          '}';
    }
  }

  private class RefreshRpcRateLimitThread extends Thread {
    private RefreshRpcRateLimitThread() {
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
          String newValue = BzlDynamicConfiguration.getInstance()
              .get(IPC_SERVER_RATE_LIMIT_RULES, IPC_SERVER_RATE_LIMIT_RULES_DEFAULT);

          if ((oldValue == null) || (newValue != null && !oldValue.equals(newValue))) {
            try {
              List<LimitCondition> list = getRateLimitList(newValue);
              if (list.size() > 0) {
                conditionList.clear();
                conditionList.addAll(list);
                LOG.info(
                    "The {} has changed. Details is {}", IPC_SERVER_RATE_LIMIT_RULES,
                    list);
                oldValue = newValue;
              }
            } catch (Exception e) {
              LOG.error("RefreshRpcRateLimitThread throw exception. The detail is {}.",
                  e.getMessage());
            }
          }
        }

        try {
          Thread.sleep(BzlDynamicConfiguration.getInstance()
              .getLong(IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD,
                  IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT));
        } catch (InterruptedException e) {
          LOG.warn("RefreshRpcRateLimitThread InterruptedException. The detail is {}.",
              e.getMessage());
          Thread.currentThread().interrupt();
        }
      }
    }


    private List<LimitCondition> getRateLimitList(String rateLimitConfig) {
      List<LimitCondition> list = new ArrayList<>();
      String rateLimits[] = rateLimitConfig.split(";");

      for (int i = 0; i < rateLimits.length; i++) {
        String rateLimit[] = rateLimits[i].split(":");
        if (rateLimit.length != 2) {
          LOG.error("Wrong config for {}. Detail is {}", IPC_SERVER_RATE_LIMIT_RULES,
              rateLimits[i]);
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
          continue;
        }

        list.add(new LimitCondition(limitKeys[0], limitKeys[1], limitKeys[2], limitKeys[3], qps));
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
  }
}
