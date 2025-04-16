package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class TestHttpClient {
  public static final Logger LOG = LoggerFactory.getLogger(TestHttpClient.class);

  @Test
  public void testTimeout() {
    String url = "http://www.baidu.com";
    String resStr = doGetHttpWithTimeout(url);
    LOG.info("The result is {}", resStr);
  }

  private String doGetHttp(String url) {
    String resStr = null;
    CloseableHttpClient httpClient = null;
    CloseableHttpResponse httpResponse = null;
    try {
      URI uri = new URIBuilder(url).build();
      httpClient = HttpClients.createDefault();
      HttpGet httpGet = new HttpGet(uri);
      httpResponse = httpClient.execute(httpGet);

      if (httpResponse.getStatusLine().getStatusCode() == 200) {
        resStr = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      } else {
        LOG.warn("Fetch error. The return code is {} .",
            httpResponse.getStatusLine().getStatusCode());
      }
    } catch (IOException e) {
      LOG.warn("IOException error! The detail message is {}.", e.getMessage());
      return null;
    } catch (URISyntaxException e) {
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
        LOG.warn("Close error! The detail message is {}.", e.getMessage());
      }
    }
    return resStr;
  }

  private String doGetHttpWithTimeout(String url) {
    String resStr = null;
    CloseableHttpClient httpClient = null;
    CloseableHttpResponse httpResponse = null;
    try {
      RequestConfig config =
          RequestConfig.custom().setConnectTimeout(10000).setConnectionRequestTimeout(15000)
              .setSocketTimeout(15000).build();
      URI uri = new URIBuilder(url).build();
      httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();

      HttpGet httpGet = new HttpGet(uri);
      httpResponse = httpClient.execute(httpGet);

      if (httpResponse.getStatusLine().getStatusCode() == 200) {
        resStr = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
      } else {
        LOG.warn("Fetch error. The return code is {} .",
            httpResponse.getStatusLine().getStatusCode());
      }
    } catch (IOException e) {
      LOG.warn("IOException error! The detail message is {}.", e.getMessage());
      return null;
    } catch (URISyntaxException e) {
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
        LOG.warn("Close error! The detail message is {}.", e.getMessage());
      }
    }
    return resStr;
  }
}
