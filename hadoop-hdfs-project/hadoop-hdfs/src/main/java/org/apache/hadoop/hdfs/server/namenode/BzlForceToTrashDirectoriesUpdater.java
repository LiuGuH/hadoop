package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.server.namenode.metrics.BzlForceToTrashDirectoriesMetrics;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECTION_REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_CONNECT_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.BZL_HTTP_SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE;

public class BzlForceToTrashDirectoriesUpdater {
  static final Logger LOG = LoggerFactory.getLogger(BzlForceToTrashDirectoriesUpdater.class);

  private static class ForceToTrashDirectoriesUpdateThread extends Thread {
    private long updatePeriod;
    private String forceToTrashDirectoriesBzlRemoteUrl;
    private BzlForceToTrashDirectoriesMetrics bzlForceToTrashDirectoriesMetrics;

    private ForceToTrashDirectoriesUpdateThread(Configuration conf) {
      bzlForceToTrashDirectoriesMetrics = BzlForceToTrashDirectoriesMetrics.create();
      this.updatePeriod = conf.getLong(
          CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_PERIOD,
          60 * 1000);
      this.forceToTrashDirectoriesBzlRemoteUrl = conf.get(
          CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_URL,
          "");
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        BzlForceToTrashDirectoriesUpdater.getInstance()
            .updateRemoteForceToTrashDirectories(getRemoteForceToTrashDirectories());
        BzlForceToTrashDirectoriesUpdater.getInstance()
            .mergeRemoteBzlForceToTrashDirectories();

        try {
          Thread.sleep(updatePeriod);
        } catch (InterruptedException e) {
          LOG.warn("InterruptedException is catched. The details is {}", e.getMessage());
          Thread.currentThread().interrupt();
        }
      }
    }

    private SortedSet<String> getRemoteForceToTrashDirectories() {
      SortedSet<String> remoteForceToTrashDirectories = new TreeSet<>();
      if (BzlDynamicConfiguration.getInstance().getBoolean(
          CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_ENABLE,
          false)) {
        String jsonData = doGetHttp(forceToTrashDirectoriesBzlRemoteUrl);
        remoteForceToTrashDirectories = parseJson(jsonData);
      }
      return remoteForceToTrashDirectories;
    }

    private String doGetHttp(String forceToTrashDirectoriesBzlRemoteUrl) {
      String resStr = null;
      CloseableHttpClient httpClient = null;
      CloseableHttpResponse httpResponse = null;
      try {
        RequestConfig config = RequestConfig.custom().setSocketTimeout(BZL_HTTP_SOCKET_TIMEOUT)
            .setConnectTimeout(BZL_HTTP_CONNECT_TIMEOUT)
            .setConnectionRequestTimeout(BZL_HTTP_CONNECTION_REQUEST_TIMEOUT).build();
        URI uri = new URIBuilder(forceToTrashDirectoriesBzlRemoteUrl).build();
        httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
        HttpGet httpGet = new HttpGet(uri);
        httpResponse = httpClient.execute(httpGet);

        if (httpResponse.getStatusLine().getStatusCode() == 200) {
          resStr = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
          bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchSuccesses();
        } else {
          LOG.warn("Fetch error. The return code is {} .",
              httpResponse.getStatusLine().getStatusCode());
          bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchFailures();
        }
      } catch (IOException e) {
        LOG.warn("IOException error! The detail message is {}.", e.getMessage());
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchFailures();
        return null;
      } catch (URISyntaxException e) {
        LOG.warn("URISyntaxException error! The detail message is {}.", e.getMessage());
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchFailures();
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

    private SortedSet<String> parseJson(String json) {
      SortedSet<String> remoteForceToTrashDirectories = new TreeSet<>();
      if (StringUtils.isEmpty(json)) {
        return remoteForceToTrashDirectories;
      }

      Gson gson = new Gson();
      Map<String, Map<String, List<String>>> rs = gson.fromJson(json, Map.class);
      Map<String, List<String>> allData = rs.get("data");
      int remoteForceToTrashDirectoriesNums = 0;

      if (allData == null) {
        LOG.warn("RemoteForceToTrashDirectories is null.");
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesCheckFailures();
        return remoteForceToTrashDirectories;
      }

      List<String> remoteForceToTrashDirectoriesList = new ArrayList<>();
      for (Map.Entry<String, List<String>> item : allData.entrySet()) {
        List<String> itemList = item.getValue();
        remoteForceToTrashDirectoriesNums += itemList.size();
        remoteForceToTrashDirectoriesList.addAll(itemList);
      }

      if (checkIfTrashPath(remoteForceToTrashDirectoriesList)) {
        LOG.warn("RemoteForceToTrashDirectories has trash path. Will not update.");
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesCheckFailures();
        return remoteForceToTrashDirectories;
      }

      if (remoteForceToTrashDirectoriesList.size() > BzlDynamicConfiguration.getInstance()
          .getLong(FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000)) {
        LOG.warn("RemoteForceToTrashDirectoriesList size exceeds the upper limit of {}.",
            BzlDynamicConfiguration.getInstance()
                .getLong(FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000));
        bzlForceToTrashDirectoriesMetrics.setBzlForceToTrashDirectoriesSizeExceeded(1);
        return remoteForceToTrashDirectories;
      }

      bzlForceToTrashDirectoriesMetrics.setBzlForceToTrashDirectoriesSizeExceeded(0);
      // If path ends with /, like '/a/b/c/'. Make it to '/a/b/c'
      for (int i = 0; i < remoteForceToTrashDirectoriesList.size(); i++) {
        String item = remoteForceToTrashDirectoriesList.get(i);
        if (item.endsWith("/") && !item.equals("/")) {
          remoteForceToTrashDirectoriesList.set(i, item.substring(0, item.length() - 1));
        }
      }
      remoteForceToTrashDirectories.addAll(remoteForceToTrashDirectoriesList);

      bzlForceToTrashDirectoriesMetrics.setBzlForceToTrashDirectoriesNums(
          remoteForceToTrashDirectoriesNums);
      bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesCheckSuccesses();
      return remoteForceToTrashDirectories;
    }

    public BzlForceToTrashDirectoriesMetrics getBzlForceToTrashDirectoriesMetrics() {
      return bzlForceToTrashDirectoriesMetrics;
    }
  }

  private static boolean checkIfTrashPath(List<String> remoteForceToTrashDirectoriesList) {
    for (String item : remoteForceToTrashDirectoriesList) {
      if (item.contains("/.Trash")) {
        LOG.warn("{} contain .Trash", item);
        return true;
      }
    }
    return false;
  }

  private static final BzlForceToTrashDirectoriesUpdater INSTANCE =
      new BzlForceToTrashDirectoriesUpdater();
  private ForceToTrashDirectoriesUpdateThread updateThread;
  private SortedSet<String> forceToTrashDirectories;
  private SortedSet<String> prevRemoteForceToTrashDirectories;
  private SortedSet<String> currRemoteForceToTrashDirectories;

  private BzlForceToTrashDirectoriesUpdater() {
  }

  public static BzlForceToTrashDirectoriesUpdater getInstance() {
    return INSTANCE;
  }

  public void init(Configuration conf, SortedSet<String> forceToTrashDirectories) {
    this.forceToTrashDirectories = forceToTrashDirectories;
    this.prevRemoteForceToTrashDirectories = new TreeSet<>();
    this.currRemoteForceToTrashDirectories = new TreeSet<>();
    updateThread = new ForceToTrashDirectoriesUpdateThread(conf);
    updateThread.start();
  }

  private void mergeRemoteBzlForceToTrashDirectories() {
    if (BzlDynamicConfiguration.getInstance().getBoolean(
        CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_ENABLE, false)
        && currRemoteForceToTrashDirectories.size() != 0
        && !currRemoteForceToTrashDirectories.equals(prevRemoteForceToTrashDirectories)) {
      int previousSize = forceToTrashDirectories.size();
      forceToTrashDirectories.addAll(currRemoteForceToTrashDirectories);
      forceToTrashDirectories.removeIf(
          item -> !currRemoteForceToTrashDirectories.contains(item));
      int newSize = forceToTrashDirectories.size();
      LOG.info(
          "UpdateBzlForceToTrashDirectories, BzlForceToTrashDirectories has changed! PreviousSize is {}, newSise is {}.",
          previousSize, newSize);
      prevRemoteForceToTrashDirectories.clear();
      prevRemoteForceToTrashDirectories.addAll(currRemoteForceToTrashDirectories);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("BzlForceToTrashDirectories is {}", forceToTrashDirectories);
    }
  }

  private void updateRemoteForceToTrashDirectories(
      SortedSet<String> remoteForceToTrashDirectories) {
    this.currRemoteForceToTrashDirectories.addAll(remoteForceToTrashDirectories);
    this.currRemoteForceToTrashDirectories.removeIf(
        item -> !remoteForceToTrashDirectories.contains(item));
  }

  public MutableRate getBzlForceToTrashDirectoriesProcessingTime() {
    return updateThread.getBzlForceToTrashDirectoriesMetrics().getBzlForceToTrashDirectoriesProcessingTime();
  }
}
