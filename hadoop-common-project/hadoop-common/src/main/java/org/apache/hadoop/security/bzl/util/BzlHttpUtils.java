package org.apache.hadoop.security.bzl.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECT_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_SOCKET_TIMEOUT;

public class BzlHttpUtils {

  static final Logger LOG = LoggerFactory.getLogger(BzlHttpUtils.class);

  public static String getJsonFromHttp(String url, String method, String datastarAc,
                                       String datastarSk,
                                       RpcBzlTokenPasswordFetcherMetrics rpcBzlDatastarMetrics) {
    String datastarTs = String.valueOf(System.currentTimeMillis());
    String beformd5 =
        "datastarAc=" + datastarAc + "&datastarTs=" + datastarTs + "&datastarSk=" + datastarSk;
    String datastarSign = DigestUtils.md5Hex(beformd5);
    String content = null;
    CloseableHttpClient httpClient = null;
    CloseableHttpResponse httpResponse = null;
    URI uri = null;
    try {
      RequestConfig config = RequestConfig.custom().setSocketTimeout(BZL_HTTP_SOCKET_TIMEOUT)
          .setConnectTimeout(BZL_HTTP_CONNECT_TIMEOUT)
          .setConnectionRequestTimeout(BZL_HTTP_CONNECTION_REQUEST_TIMEOUT).build();
      uri = new URIBuilder(url + method)
          .setParameter("datastarAc", datastarAc)
          .setParameter("datastarTs", datastarTs)
          .setParameter("datastarSk", datastarSk)
          .setParameter("datastarSign", datastarSign)
          .build();

      httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
      HttpGet httpGet = new HttpGet(uri);
      httpResponse = httpClient.execute(httpGet);

      if (httpResponse.getStatusLine().getStatusCode() == 200) {
        content = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        if (rpcBzlDatastarMetrics != null) {
          rpcBzlDatastarMetrics.incrBzlTokenPasswordFetchSuccesses();
        }
      } else {
        if (rpcBzlDatastarMetrics != null) {
          rpcBzlDatastarMetrics.incrBzlTokenPasswordFetchFailures();
        }
      }
      return content;
    } catch (IOException e) {
      if (rpcBzlDatastarMetrics != null) {
        rpcBzlDatastarMetrics.incrBzlTokenPasswordFetchFailures();
      }
      LOG.warn("IOException error! The detail message is {}.", e.getMessage());
      return null;
    } catch (URISyntaxException e) {
      if (rpcBzlDatastarMetrics != null) {
        rpcBzlDatastarMetrics.incrBzlTokenPasswordFetchFailures();
      }
      LOG.warn("URISyntaxException error! The detail message is {}.", e.getMessage());
      return null;
    } finally {
      try {
        if (httpClient != null) {
          httpClient.close();
        }
        if (httpResponse != null) {
          httpResponse.close();
        }
      } catch (IOException e) {
        LOG.warn("close error! The detail message is {}.", e.getMessage());
      }
    }
  }
}
