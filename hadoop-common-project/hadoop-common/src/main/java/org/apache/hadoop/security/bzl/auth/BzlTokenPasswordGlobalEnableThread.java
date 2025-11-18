package org.apache.hadoop.security.bzl.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

public class BzlTokenPasswordGlobalEnableThread extends Thread {
  private static final Logger LOG =
      LoggerFactory.getLogger(BzlTokenPasswordGlobalEnableThread.class);
  private static volatile boolean enable = true;
  private long fetcherPeriod;

  public BzlTokenPasswordGlobalEnableThread(Configuration conf) {
    this.fetcherPeriod =
        conf.getLong(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_UPDATE_PERIOD_MS, 30000l);
  }

  @Override
  public void run() {

    LOG.info("BzlTokenPasswordGlobalEnableThread is starting.");

    while (true) {
      try {
        String url = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.HADOOP_BZL_TOKEN_AUTH_ENABLE_URI, "");

        String result = BzlJsonUtils.getBzlTokenEnableFromHttp(url, "[BDH]bzlTokenEnable");

        if (result.equalsIgnoreCase("false") && enable) {
          LOG.warn("Disable bzlToken auth globally from {}", url);
          enable = false;
        } else if (result.equalsIgnoreCase("true") && !enable) {
          LOG.info("Enable bzlToken auth globally from {}", url);
          enable = true;
        }

      } catch (Exception e) {
        LOG.warn("BzlTokenPasswordGlobalEnableThread throw exception:", e);
      }

      try {
        Thread.sleep(fetcherPeriod);
      } catch (InterruptedException e) {
        LOG.warn("BzlTokenPasswordGlobalEnableThread catch interruptedException:", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  public static boolean isEnable() {
    return enable;
  }

}
