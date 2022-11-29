package org.apache.hadoop.security.bzl.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordLoaderMetrics;
import org.apache.hadoop.security.bzl.util.BzlAuthFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class BzlTokenPasswordLoaderThread extends Thread {
  static final Logger LOG = LoggerFactory.getLogger(
      BzlTokenPasswordLoaderThread.class);
  private final static String FILE_SEPARATOR = File.separator;
  private RpcBzlTokenPasswordLoaderMetrics rpcBzlTokenPasswordLoaderMetrics;
  private String bzlAuthLocalDir;
  private String bzlAuthFilePath;
  private long loaderPeriod;

  public BzlTokenPasswordLoaderThread(Configuration conf) {
    this.rpcBzlTokenPasswordLoaderMetrics = RpcBzlTokenPasswordLoaderMetrics.create();
    this.bzlAuthLocalDir = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR);
    this.loaderPeriod =
        conf.getLong(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_UPDATE_PERIOD, 60000l);
    this.bzlAuthFilePath =
        bzlAuthLocalDir + FILE_SEPARATOR + BzlTokenPasswordManager.BZL_AUTH_FILENAME;

    this.setName("BzlTokenPasswordLoaderThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    LOG.info("BzlTokenPasswordLoaderThread is starting.");

    while (true) {
      try {
        LOG.info("BzlTokenPasswordLoaderThread is running.");
        ConcurrentHashMap<String, String> con =
            BzlAuthFileUtils.readBzlTokenPassword(bzlAuthFilePath);

        if (BzlTokenPasswordManager.getInstance().updateConfiguration(con)) {
          rpcBzlTokenPasswordLoaderMetrics.incrBzlTokenPasswordChangeNumbers();
          LOG.info("BzlTokenPasswordManager has changed！New ConcurrentHashMap size is {}.",
              con.size());
        }
      } catch (Exception e) {
        LOG.warn("BzlTokenPasswordLoaderThread catch exception. The detail is {}.", e.getMessage());
      }

      try {
        Thread.sleep(loaderPeriod);
      } catch (InterruptedException e) {
        LOG.warn("BzlTokenPasswordLoaderThread interruptedException. The detail is: {}.",
            e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
  }
}