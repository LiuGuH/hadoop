package org.apache.hadoop.hdfs.server.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_BZL_UPDATER_PERIOD;

public class BzlProtectedDirectoriesGlobalEnableThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(
      BzlProtectedDirectoriesGlobalEnableThread.class);
  private static volatile boolean enable = true;

  public BzlProtectedDirectoriesGlobalEnableThread() {
    this.setName("BzlProtectedDirectoriesGlobalEnableThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    LOG.info("BzlProtectedDirectoriesGlobalEnableThread is starting.");

    while (true) {
      try {
        String url = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_ENABLE_URI, "");

        String result = BzlJsonUtils.getBzlGlobalEnableFromHttp(url, "[BDH]protectedDirectoriesEnable");

        if (result.equalsIgnoreCase("false") && enable) {
          LOG.warn("Disable protectedDirectories globally from {}", url);
          enable = false;
        } else if (result.equalsIgnoreCase("true") && !enable) {
          LOG.info("Enable protectedDirectories globally from {}", url);
          enable = true;
        }

      } catch (Exception e) {
        LOG.warn("BzlProtectedDirectoriesGlobalEnableThread throw exception:", e);
      }

      try {
        Thread.sleep(BzlDynamicConfiguration.getInstance()
            .getLong(FS_PROTECTED_DIRECTORIES_BZL_UPDATER_PERIOD, 60 * 1000L));
      } catch (InterruptedException e) {
        LOG.warn("BzlProtectedDirectoriesGlobalEnableThread catch interruptedException:", e);
        Thread.currentThread().interrupt();
      }
    }

  }

  public static boolean isGlobalEnable() {
    return enable;
  }

}

