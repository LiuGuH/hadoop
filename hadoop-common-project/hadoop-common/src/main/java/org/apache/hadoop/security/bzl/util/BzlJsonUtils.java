package org.apache.hadoop.security.bzl.util;

import com.google.gson.Gson;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean;
import org.apache.hadoop.security.bzl.bean.BzlWhiteListBean;

import java.util.List;

import static org.apache.hadoop.security.bzl.util.BzlHttpUtils.getJsonFromHttp;

public class BzlJsonUtils {

  public static String getTokenPasswordContext(String bzlAuthUrlEndpoint,
                                               String bzlAuthUrlPasswordApi,
                                               String bzlAuthUrlWhiteListApi, String bzlAuthUrlAc,
                                               String bzlAuthUrlSk,
                                               RpcBzlTokenPasswordFetcherMetrics rpcBzlDatastarMetrics) {
    StringBuffer stb = new StringBuffer();
    String password =
        getPassword(bzlAuthUrlEndpoint, bzlAuthUrlPasswordApi, bzlAuthUrlAc, bzlAuthUrlSk,
            rpcBzlDatastarMetrics);
    if (password.length() == 0) {
      return stb.toString();
    }
    stb.append(password);

    String whiteList =
        getWhiteList(bzlAuthUrlEndpoint, bzlAuthUrlWhiteListApi, bzlAuthUrlAc, bzlAuthUrlSk,
            rpcBzlDatastarMetrics);
    stb.append(whiteList);
    return stb.toString();
  }

  private static String getPassword(String bzlAuthUrlEndpoint, String bzlAuthUrlPasswordApi,
                                    String bzlAuthUrlAc, String bzlAuthUrlSk,
                                    RpcBzlTokenPasswordFetcherMetrics rpcBzlDatastarMetrics) {
    Gson gson = new Gson();
    StringBuffer stb = new StringBuffer();
    String jsonPasswordData =
        getJsonFromHttp(bzlAuthUrlEndpoint, bzlAuthUrlPasswordApi, bzlAuthUrlAc, bzlAuthUrlSk,
            rpcBzlDatastarMetrics);
    if (jsonPasswordData != null) {
      BzlPasswordBean bean = gson.fromJson(jsonPasswordData, BzlPasswordBean.class);

      if (bean.getMeta().getCode() != 0) {
        return stb.toString();
      }

      List<BzlPasswordBean.PasswordData> list = bean.getData();
      for (BzlPasswordBean.PasswordData data : list) {
        stb.append(data.getGroupAccount() + "="
            + data.getGroupPassword() + ","
            + data.getExpiringGroupPassword() + "\n");
      }
    }
    return stb.toString();
  }

  private static String getWhiteList(String bzlAuthUrlEndpoint, String bzlAuthUrlWhiteListApi,
                                     String bzlAuthUrlAc, String bzlAuthUrlSk,
                                     RpcBzlTokenPasswordFetcherMetrics rpcBzlDatastarMetrics) {
    Gson gson = new Gson();
    StringBuffer stb = new StringBuffer();
    String jsonWhiteData =
        getJsonFromHttp(bzlAuthUrlEndpoint, bzlAuthUrlWhiteListApi, bzlAuthUrlAc, bzlAuthUrlSk,
            rpcBzlDatastarMetrics);
    if (jsonWhiteData != null) {
      BzlWhiteListBean whitebean = gson.fromJson(jsonWhiteData, BzlWhiteListBean.class);

      if (whitebean.getMeta().getCode() != 0) {
        return new StringBuffer().toString();
      }

      List<String> whitelist = whitebean.getData();
      if (whitelist != null && !whitelist.isEmpty()) {
        stb.append("whiteuserlist=" + String.join(",", whitelist) + "\n");
      }
    }
    return stb.toString();
  }
}