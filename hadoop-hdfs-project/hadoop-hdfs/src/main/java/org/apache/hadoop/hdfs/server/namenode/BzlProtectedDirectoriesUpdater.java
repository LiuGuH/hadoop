package org.apache.hadoop.hdfs.server.namenode;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class BzlProtectedDirectoriesUpdater {
  static final Logger LOG = LoggerFactory.getLogger(BzlProtectedDirectoriesUpdater.class);

  private static class ProtectedDirectoriesUpdateThread extends Thread {
    private long updatePeriod;
    private String protectedDirectoriesBzlRemoteUrl;

    private ProtectedDirectoriesUpdateThread(Configuration conf) {
      this.updatePeriod = conf.getLong(
          CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_PERIOD,
          60 * 1000);
      this.protectedDirectoriesBzlRemoteUrl = conf
          .get(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_REMOTE_URL, "");
      this.setDaemon(true);
    }

    @Override
    public void run() {
      while (true) {
        BzlProtectedDirectoriesUpdater.getInstance()
            .updateRemoteProtectedDirectories(getRemoteProtectedDirectories());
        BzlProtectedDirectoriesUpdater.getInstance().mergeRemoteBzlProtectedDirectories();

        try {
          Thread.sleep(updatePeriod);
        } catch (InterruptedException e) {
          LOG.warn("InterruptedException is catched. The details is {}", e.getMessage());
          Thread.currentThread().interrupt();
        }
      }
    }

    private SortedSet<String> getRemoteProtectedDirectories() {
      SortedSet<String> remoteProtectedDirectories = new TreeSet<>();
      if (BzlDynamicConfiguration.getInstance()
          .getBoolean(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_ENABLE,
              false)) {
        String jsonData = doGetHttp(protectedDirectoriesBzlRemoteUrl);
        remoteProtectedDirectories = parseJson(jsonData);
      }
      return remoteProtectedDirectories;
    }

    private String doGetHttp(String protectedDirectoriesBzlRemoteUrl) {
      String resStr = null;
      CloseableHttpClient httpClient = null;
      CloseableHttpResponse httpResponse = null;
      try {
        URI uri = new URIBuilder(protectedDirectoriesBzlRemoteUrl).build();
        httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(uri);
        httpResponse = httpClient.execute(httpGet);

        if (httpResponse.getStatusLine().getStatusCode() == 200) {
          resStr = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
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

    private SortedSet<String> parseJson(String json) {
      SortedSet<String> remoteProtectedDirectories = new TreeSet<>();
      if (StringUtils.isEmpty(json)) {
        return remoteProtectedDirectories;
      }

      Gson gson = new Gson();
      Map<String, Map<String, List<String>>> rs = gson.fromJson(json, Map.class);
      Map<String, List<String>> allData = rs.get("data");
      if (allData != null) {
        for (Map.Entry<String, List<String>> item : allData.entrySet()) {
          remoteProtectedDirectories.addAll(item.getValue());
        }
      }
      return remoteProtectedDirectories;
    }
  }

  private static final BzlProtectedDirectoriesUpdater
      INSTANCE = new BzlProtectedDirectoriesUpdater();
  private ProtectedDirectoriesUpdateThread updateThread;
  private SortedSet<String> prevlocalProtectedDirectories;
  private SortedSet<String> currlocalProtectedDirectories;
  private SortedSet<String> protectedDirectories;
  private SortedSet<String> prevRemoteProtectedDirectories;
  private SortedSet<String> currRemoteProtectedDirectories;

  private BzlProtectedDirectoriesUpdater() {
  }

  public static BzlProtectedDirectoriesUpdater getInstance() {
    return INSTANCE;
  }

  public void init(Configuration conf, SortedSet<String> protectedDirectories) {
    this.protectedDirectories = protectedDirectories;
    this.prevlocalProtectedDirectories = new TreeSet<>();
    this.currlocalProtectedDirectories = new TreeSet<>();
    this.prevRemoteProtectedDirectories = new TreeSet<>();
    this.currRemoteProtectedDirectories = new TreeSet<>();
    updateThread = new ProtectedDirectoriesUpdateThread(conf);
    updateThread.start();
  }

  public void updateLocalProtectedDirectories(SortedSet<String> localProtectedDirectories) {
    this.currlocalProtectedDirectories.addAll(localProtectedDirectories);
    this.currlocalProtectedDirectories.removeIf(item -> !localProtectedDirectories.contains(item));

    if (this.currlocalProtectedDirectories.size() != 0 &&
        !this.currlocalProtectedDirectories.equals(this.prevlocalProtectedDirectories)) {
      int previousSize = protectedDirectories.size();
      protectedDirectories.addAll(this.currlocalProtectedDirectories);
      protectedDirectories.removeIf(item -> !this.currlocalProtectedDirectories.contains(item));
      int newSize = protectedDirectories.size();
      LOG.info("UpdateLocalProtectedDirectories, BzlProtectedDirectories has changed! PreviousSize is {}, newSise is {}.",
          previousSize, newSize);
      prevlocalProtectedDirectories.clear();
      prevlocalProtectedDirectories.addAll(currlocalProtectedDirectories);

      mergeRemoteBzlProtectedDirectories();
    }
  }

  private void mergeRemoteBzlProtectedDirectories() {
    if (BzlDynamicConfiguration.getInstance()
        .getBoolean(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_ENABLE,
            false) && currRemoteProtectedDirectories.size() != 0 &&
        !currRemoteProtectedDirectories.equals(prevRemoteProtectedDirectories)) {
      int previousSize = protectedDirectories.size();
      protectedDirectories.addAll(currRemoteProtectedDirectories);
      protectedDirectories.removeIf(item -> !currlocalProtectedDirectories.contains(item) &&
          !currRemoteProtectedDirectories.contains(item));
      int newSize = protectedDirectories.size();
      LOG.info("UpdateBzlProtectedDirectories, BzlProtectedDirectories has changed! PreviousSize is {}, newSise is {}.",
          previousSize, newSize);
      prevRemoteProtectedDirectories.clear();
      prevRemoteProtectedDirectories.addAll(currRemoteProtectedDirectories);
    }

    if(LOG.isDebugEnabled()){
      LOG.debug("ProtectedDirectories is {}",protectedDirectories);
    }
  }

  private void updateRemoteProtectedDirectories(SortedSet<String> remoteProtectedDirectories) {
    this.currRemoteProtectedDirectories.addAll(remoteProtectedDirectories);
    this.currRemoteProtectedDirectories.removeIf(
        item -> !remoteProtectedDirectories.contains(item));
  }
}
