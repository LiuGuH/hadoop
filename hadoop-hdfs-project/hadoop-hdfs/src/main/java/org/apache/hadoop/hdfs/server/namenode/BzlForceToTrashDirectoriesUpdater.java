package org.apache.hadoop.hdfs.server.namenode;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.server.namenode.metrics.BzlForceToTrashDirectoriesMetrics;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlHttpUtils;
import org.apache.http.client.utils.URIBuilder;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_LIST_MAX_SIZE;

public class BzlForceToTrashDirectoriesUpdater {
  static final Logger LOG = LoggerFactory.getLogger(BzlForceToTrashDirectoriesUpdater.class);

  private static class ForceToTrashDirectoriesUpdateThread extends Thread {
    private BzlForceToTrashDirectoriesMetrics bzlForceToTrashDirectoriesMetrics;

    private ForceToTrashDirectoriesUpdateThread(Configuration conf) {
      bzlForceToTrashDirectoriesMetrics = BzlForceToTrashDirectoriesMetrics.create();
      this.setDaemon(true);

      new BzlForceToTrashGlobalEnableThread().start();
    }

    @Override
    public void run() {
      while (true) {
        try {
          BzlForceToTrashDirectoriesUpdater.getInstance()
              .updateRemoteForceToTrashDirectories(getRemoteForceToTrashDirectories());
          BzlForceToTrashDirectoriesUpdater.getInstance().mergeRemoteBzlForceToTrashDirectories();
        } catch (Exception e) {
          LOG.error("BzlForceToTrashDirectoriesUpdater throw Exception:", e);
          bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesCheckFailures();
        }

        try {
          Thread.sleep(BzlDynamicConfiguration.getInstance()
              .getLong(CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_PERIOD,
                  60 * 1000));
        } catch (InterruptedException e) {
          LOG.warn("InterruptedException is catched:", e);
          Thread.currentThread().interrupt();
        }
      }
    }

    private SortedSet<String> getRemoteForceToTrashDirectories() throws Exception {
      SortedSet<String> remoteForceToTrashDirectories = new TreeSet<>();
      if (!BzlDynamicConfiguration.getInstance()
          .getBoolean(CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_ENABLE, false)) {
        return remoteForceToTrashDirectories;
      }

      try {
        String forceToTrashDirectoriesBzlRemoteUrl = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_REMOTE_URL, "");

        String traceId = UUID.randomUUID().toString();
        URI uri =
            new URIBuilder(forceToTrashDirectoriesBzlRemoteUrl).setParameter("traceId", traceId)
                .build();
        String jsonData = BzlHttpUtils.doGet(uri, traceId, "[BDH]forceToTrashDirectories");
        remoteForceToTrashDirectories = parseJson(jsonData);
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchSuccesses();

        return remoteForceToTrashDirectories;
      } catch (Exception e) {
        LOG.warn("getRemoteForceToTrashDirectories throw Exception:", e);
        bzlForceToTrashDirectoriesMetrics.incrBzlForceToTrashDirectoriesFetchFailures();
        throw e;
      }
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
