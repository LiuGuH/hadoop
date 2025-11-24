package org.apache.hadoop.security.bzl.util;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean.PasswordData;
import org.apache.hadoop.security.bzl.bean.BzlGlobalEnableBean;
import org.apache.http.HttpException;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class BzlJsonUtils {
  static final Logger LOG = LoggerFactory.getLogger(BzlJsonUtils.class);

  public static List<BzlPasswordBean.PasswordData> getGroupPasswordList(
      String url, String bzlAuthUrlAc,
      String bzlAuthUrlSk, RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics,
      String logPrefix) throws Exception {
    String traceId = UUID.randomUUID().toString();
    try {
      URI uri = BzlHttpUtils.buildGroupUserQueryUri(url,
          bzlAuthUrlAc, bzlAuthUrlSk, traceId);
      String context = BzlHttpUtils.doGet(uri, traceId, logPrefix);

      List<BzlPasswordBean.PasswordData> list = parseGroupPasswordListData(context, rpcBzlTokenPasswordFetcherMetrics, traceId, logPrefix);
      rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordFetchSuccesses();

      return list;
    } catch (Exception e) {
      LOG.warn("{}: request {} failed traceid is {}, throw exception:", logPrefix,
          url, traceId, e);
      rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordFetchFailures();
      throw e;
    }
  }

  private static List<BzlPasswordBean.PasswordData> parseGroupPasswordListData(String context,
      RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics, String traceId, String logPrefix)
      throws HttpException {
    Gson gson = new Gson();
    BzlPasswordBean bean = gson.fromJson(context, BzlPasswordBean.class);
    if (bean.getMeta().getCode() != 0) {
      LOG.warn("{}: context code is {}, errorMsg is {}. TraceId is {}.", logPrefix,
          bean.getMeta().getCode(), bean.getMeta().getErrorMsg(), traceId);
      throw new HttpException(
          String.format("Bzl Http Meta code is %d, Error msg is %s,TraceId is %s.",
              bean.getMeta().getCode(), bean.getMeta().getErrorMsg(), traceId));
    }

    List<BzlPasswordBean.PasswordData> list = bean.getData();
    // Remove item which is empty groupAccount or empty Password
    Iterator<PasswordData> iterator = list.iterator();
    while (iterator.hasNext()) {
      BzlPasswordBean.PasswordData passwordData = iterator.next();
      if (passwordData.groupAccountIsEmpty()) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordFetchAccountEmpty();
        LOG.debug("BzlTokenPasswordFetcher found GroupAccount is empty.");
        iterator.remove();
        continue;
      }

      if (passwordData.allPasswordIsEmpty()) {
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordFetchPasswordEmpty();
        LOG.debug("BzlTokenPasswordFetcher found GroupAccount {} has empty password.",
            passwordData.getGroupAccount());
        iterator.remove();
      }
    }

    // Check system account all exist
    if (!BzlAuthFileUtils.checkSystemAccount(list)) {
      list.clear();
      rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordFetchMissingSystemAccount();
    }

    return list;
  }

  public static String getBzlGlobalEnableFromHttp(String url, String logPrefix)
      throws HttpException, URISyntaxException, IOException {
    String traceId = UUID.randomUUID().toString();
    URI uri = new URIBuilder(url)
        .setParameter("traceId", traceId)
        .build();
    String context = BzlHttpUtils.doGet(uri, traceId, logPrefix);

    return parseBzlGlobalEnableData(context, traceId, logPrefix);
  }

  private static String parseBzlGlobalEnableData(String context, String traceId, String logPrefix)
      throws HttpException {
    Gson gson = new Gson();
    BzlGlobalEnableBean bean = gson.fromJson(context, BzlGlobalEnableBean.class);
    if (bean.getMeta().getCode() != 0) {
      LOG.warn("{}: getBzlGlobalEnableData context code is {}, errorMsg is {}. TraceId is {}.",
          logPrefix, bean.getMeta().getCode(), bean.getMeta().getErrorMsg(), traceId);
      throw new HttpException(
          String.format("Bzl Http Meta code is %d, Error msg is %s, TraceId is %s.",
              bean.getMeta().getCode(), bean.getMeta().getErrorMsg(), traceId));
    }

    String enable = bean.getData();

    return enable;
  }
}