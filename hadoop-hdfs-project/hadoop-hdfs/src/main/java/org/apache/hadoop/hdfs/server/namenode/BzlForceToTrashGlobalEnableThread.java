package org.apache.hadoop.hdfs.server.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_UPDATER_PERIOD;

public class BzlForceToTrashGlobalEnableThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(
      BzlForceToTrashGlobalEnableThread.class);
  private static volatile boolean enable = true;

  public BzlForceToTrashGlobalEnableThread() {
    this.setName("BzlForceToTrashGlobalEnableThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    LOG.info("BzlForceToTrashGlobalEnableThread is starting.");

    while (true) {
      try {
        String url = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.FS_FORCE_TO_TRASH_BZL_ENABLE_URI, "");
        String result = BzlJsonUtils.getBzlGlobalEnableFromHttp(url, "[BDH]forceToTrashEnable");

        if (result.equalsIgnoreCase("false") && enable) {
          LOG.warn("Disable forceToTrash globally from {}", url);
          enable = false;
        } else if (result.equalsIgnoreCase("true") && !enable) {
          LOG.info("Enable forceToTrash globally from {}", url);
          enable = true;
        }

      } catch (Exception e) {
        LOG.warn("BzlForceToTrashGlobalEnableThread throw exception:", e);
      }

      try {
        Thread.sleep(BzlDynamicConfiguration.getInstance()
            .getLong(FS_FORCE_TO_TRASH_BZL_UPDATER_PERIOD, 60 * 1000L));
      } catch (InterruptedException e) {
        LOG.warn("BzlForceToTrashGlobalEnableThread catch interruptedException:", e);
        Thread.currentThread().interrupt();
      }
    }

  }

  public static boolean isGlobalEnable() {
    return enable;
  }

}
