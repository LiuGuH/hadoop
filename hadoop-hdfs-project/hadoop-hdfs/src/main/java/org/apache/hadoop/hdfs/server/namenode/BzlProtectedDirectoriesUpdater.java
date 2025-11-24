package org.apache.hadoop.hdfs.server.namenode;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.server.namenode.metrics.BzlProtectedDirectoriesMetrics;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlHttpUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_REMOTE_LIST_MAX_SIZE;

public class BzlProtectedDirectoriesUpdater {
  static final Logger LOG = LoggerFactory.getLogger(BzlProtectedDirectoriesUpdater.class);

  private static class ProtectedDirectoriesUpdateThread extends Thread {
    private long updatePeriod;
    private BzlProtectedDirectoriesMetrics bzlProtectedDirectoriesMetrics;
    private List<String> protectedDirectoriesListInCoresite;

    private ProtectedDirectoriesUpdateThread(Configuration conf) {
      bzlProtectedDirectoriesMetrics = BzlProtectedDirectoriesMetrics.create();
      this.updatePeriod = conf.getLong(
          CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_PERIOD,
          60 * 1000);
      this.setDaemon(true);
      this.protectedDirectoriesListInCoresite =
          (List<String>) conf.getStringCollection(FS_PROTECTED_DIRECTORIES);

      new BzlProtectedDirectoriesGlobalEnableThread().start();
    }

    @Override
    public void run() {
      while (true) {
        try {
          BzlProtectedDirectoriesUpdater.getInstance()
              .updateRemoteProtectedDirectories(getRemoteProtectedDirectories());
          BzlProtectedDirectoriesUpdater.getInstance().mergeRemoteBzlProtectedDirectories();
        } catch (Exception e) {
          LOG.error("BzlProtectedDirectoriesUpdater throw Exception:", e);
          bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesThrowException();
        }

        try {
          Thread.sleep(updatePeriod);
        } catch (InterruptedException e) {
          LOG.warn("InterruptedException is catched:", e);
          Thread.currentThread().interrupt();
        }
      }
    }

    private SortedSet<String> getRemoteProtectedDirectories() throws Exception {
      SortedSet<String> remoteProtectedDirectories = new TreeSet<>();
      if (!BzlDynamicConfiguration.getInstance()
          .getBoolean(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_ENABLE,
              false)) {
        return remoteProtectedDirectories;
      }

      try {
        String protectedDirectoriesBzlRemoteUrl = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_REMOTE_URL, "");

        String traceId = UUID.randomUUID().toString();
        URI uri = new URIBuilder(protectedDirectoriesBzlRemoteUrl).setParameter("traceId", traceId)
            .build();
        String jsonData = BzlHttpUtils.doGet(uri, traceId, "[BDH]protectedDirectories");

        remoteProtectedDirectories = parseJson(jsonData);
        bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesFetchSuccesses();

        return remoteProtectedDirectories;
      } catch (Exception e) {
        bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesFetchFailures();
        LOG.warn("getRemoteProtectedDirectories throw Exception:", e);
        throw e;
      }
    }

    private SortedSet<String> parseJson(String json) {
      SortedSet<String> remoteProtectedDirectories = new TreeSet<>();
      if (StringUtils.isEmpty(json)) {
        return remoteProtectedDirectories;
      }

      Gson gson = new Gson();
      Map<String, Map<String, List<String>>> rs = gson.fromJson(json, Map.class);
      Map<String, List<String>> allData = rs.get("data");
      int remoteProtectedDirectoriesNums = 0;

      if (allData == null) {
        LOG.warn("RemoteProtectedDirectories is null.");
        bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesCheckFailures();
        return remoteProtectedDirectories;
      }

      List<String> remoteProtectedDirectoriesList = new ArrayList<>();
      for (Map.Entry<String, List<String>> item : allData.entrySet()) {
        List<String> itemList = item.getValue();
        remoteProtectedDirectoriesNums += itemList.size();
        remoteProtectedDirectoriesList.addAll(itemList);
      }

      if (!remoteProtectedDirectoriesList.containsAll(protectedDirectoriesListInCoresite)) {
        LOG.warn("RemoteProtectedDirectoriesList does not contain all local protected directories.");
        bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesCheckFailures();
        return remoteProtectedDirectories;
      }

      if (remoteProtectedDirectoriesList.size() > BzlDynamicConfiguration.getInstance()
          .getLong(FS_PROTECTED_DIRECTORIES_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000)) {
        LOG.warn("RemoteProtectdDirectoriesList size exceeds the upper limit of {}.",
            BzlDynamicConfiguration.getInstance()
                .getLong(FS_PROTECTED_DIRECTORIES_BZL_UPDATER_REMOTE_LIST_MAX_SIZE, 100000));
        bzlProtectedDirectoriesMetrics.setBzlProtectedDirectoriesSizeExceeded(1);
        return remoteProtectedDirectories;
      }
      bzlProtectedDirectoriesMetrics.setBzlProtectedDirectoriesSizeExceeded(0);
      remoteProtectedDirectories.addAll(remoteProtectedDirectoriesList);

      bzlProtectedDirectoriesMetrics.setBzlProtectedDirectoriesNums(remoteProtectedDirectoriesNums);
      bzlProtectedDirectoriesMetrics.incrBzlProtectedDirectoriesCheckSuccesses();
      return remoteProtectedDirectories;
    }

    public BzlProtectedDirectoriesMetrics getBzlProtectedDirectoriesMetrics() {
      return bzlProtectedDirectoriesMetrics;
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
      LOG.info("UpdateBzlProtectedDirectories, BzlProtectedDirectories has changed! PreviousSize is {}, newSize is {}.",
          previousSize, newSize);
      prevRemoteProtectedDirectories.clear();
      prevRemoteProtectedDirectories.addAll(currRemoteProtectedDirectories);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("ProtectedDirectories is {}",protectedDirectories);
    }
  }

  private void updateRemoteProtectedDirectories(SortedSet<String> remoteProtectedDirectories) {
    this.currRemoteProtectedDirectories.addAll(remoteProtectedDirectories);
    this.currRemoteProtectedDirectories.removeIf(
        item -> !remoteProtectedDirectories.contains(item));
  }

  public MutableRate getBzlProtectedDirectoriesProcessingTime() {
    return updateThread.getBzlProtectedDirectoriesMetrics().getBzlProtectedDirectoriesProcessingTime();
  }
}
