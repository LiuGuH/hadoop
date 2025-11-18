package org.apache.hadoop.security.bzl.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECTION_MANAGER_MAX_PERROUTER;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECTION_MANAGER_MAX_TOTAL;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECT_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_SOCKET_TIMEOUT;

public class BzlHttpUtils {
  static final Logger LOG = LoggerFactory.getLogger(BzlHttpUtils.class);
  private static final CloseableHttpClient HTTP_CLIENT;


  private static final String DATASTAR_AC = "datastarAc";
  private static final String DATASTAR_TS = "datastarTs";
  private static final String DATASTAR_SK = "datastarSk";
  private static final String DATASTAR_SIGN = "datastarSign";
  private static final String TRACE_ID = "traceId";

  static {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(BZL_HTTP_CONNECTION_MANAGER_MAX_TOTAL);
    connectionManager.setDefaultMaxPerRoute(BZL_HTTP_CONNECTION_MANAGER_MAX_PERROUTER);

    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(BZL_HTTP_CONNECT_TIMEOUT)
        .setSocketTimeout(BZL_HTTP_SOCKET_TIMEOUT)
        .setConnectionRequestTimeout(BZL_HTTP_CONNECTION_REQUEST_TIMEOUT).build();

    HTTP_CLIENT = HttpClients.custom().setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig).build();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        closeHttpClient();
      } catch (IOException e) {
        LOG.error("Close HttpClient error:", e);
      }
    }));
  }

  private static void closeHttpClient() throws IOException {
    if (HTTP_CLIENT != null) {
      HTTP_CLIENT.close();
    }
  }

  public static URI buildGroupUserQueryUri(String url, String datastarAc, String datastarSk,
      String traceId) throws URISyntaxException {
    // 1. 构建请求参数
    Map<String, String> allParams = new HashMap<>();
    // 添加基础参数（与原始实现保持一致）
    allParams.put(DATASTAR_AC, datastarAc);
    allParams.put(DATASTAR_TS, String.valueOf(System.currentTimeMillis()));
    allParams.put(DATASTAR_SK, datastarSk);  // 使用原始值，签名计算时需要
    allParams.put(TRACE_ID, traceId);

    // 2. 计算签名（使用原始参数值）
    String sign = sign(allParams);
    allParams.put(DATASTAR_SIGN, sign);

    // 3. 构建URI
    // 注意：签名计算使用的是原始参数值，但构建URI时需要对参数值进行URL编码
    URIBuilder uriBuilder = new URIBuilder(url);
    for (Map.Entry<String, String> entry : allParams.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      uriBuilder.setParameter(key, value);
    }

    return uriBuilder.build();
  }

  /**
   * 计算签名
   * 签名算法：
   * 1. 排除KEY_SK和KEY_SIGN字段
   * 2. 将剩余参数按key排序
   * 3. 拼接成"key=value&key=value"格式
   * 4. 最后加上"datastarSk=原始SK值"
   * 5. 对整个字符串进行MD5加密
   *
   * @param params 请求参数
   * @return 签名
   */
  private static String sign(Map<String, String> params) {
    List<String> pairs = new ArrayList<>();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String k = entry.getKey();
      String v = entry.getValue();
      if (!DATASTAR_SK.equals(k) && !DATASTAR_SIGN.equals(k)) {
        pairs.add(String.format("%s=%s", k, v));
      }
    }
    Collections.sort(pairs);
    pairs.add(String.format("%s=%s", DATASTAR_SK, params.get(DATASTAR_SK)));

    return DigestUtils.md5Hex(StringUtils.join(pairs, "&"));
  }

  public static String doGet(URI uri, String traceId, String logPrefix)
      throws HttpException, IOException {
    HttpGet httpGet = new HttpGet(uri);
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 200) {
        LOG.warn("{}: Request {} return code {}, traceId is {}", logPrefix, uri, statusCode,
            traceId);
        throw new HttpException("Request " + uri + " return code " + statusCode + " traceId is " + traceId);
      }

      HttpEntity entity = response.getEntity();
      if (entity == null) {
        LOG.warn("{}: Request {} return empty entity, traceId is {}", logPrefix, uri, traceId);
        throw new HttpException("Request " + uri + " return empty entity, traceId is " + traceId);
      }

      String result = EntityUtils.toString(entity, StandardCharsets.UTF_8);
      EntityUtils.consume(entity);

      return result;
    }
  }
}