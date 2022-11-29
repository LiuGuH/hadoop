package org.apache.hadoop.security.bzl.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.security.bzl.util.BzlAuthFileUtils;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BzlTokenPasswordFetcherThread extends Thread {
  static final Logger LOG = LoggerFactory.getLogger(
      BzlTokenPasswordFetcherThread.class);
  private final static String FILE_SEPARATOR = File.separator;

  private RpcBzlTokenPasswordFetcherMetrics rpcBzlDatastarMetrics;
  private String bzlAuthUrlEndpoint;
  private String bzlAuthUrlPasswordApi;
  private String bzlAuthUrlWhiteListApi;
  private String bzlAuthUrlAc;
  private String bzlAuthUrlSk;
  private String bzlAuthLocalDir;
  private String bzlAuthFilePath;
  private String bzlTmpAuthFilePath;
  private long fetcherPeriod;

  public BzlTokenPasswordFetcherThread(Configuration conf) {
    this.rpcBzlDatastarMetrics = RpcBzlTokenPasswordFetcherMetrics.create();
    this.bzlAuthLocalDir = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR);
    this.bzlAuthUrlEndpoint = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_ENDPOINT);
    this.bzlAuthUrlPasswordApi = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_PASSWORDAPI);
    this.bzlAuthUrlWhiteListApi =
        conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_WHITELISTAPI);
    this.bzlAuthUrlAc = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_AC);
    this.bzlAuthUrlSk = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_SK);
    this.fetcherPeriod =
        conf.getLong(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_UPDATE_PERIOD, 60000l);
    this.bzlAuthFilePath =
        bzlAuthLocalDir + FILE_SEPARATOR + BzlTokenPasswordManager.BZL_AUTH_FILENAME;
    this.bzlTmpAuthFilePath =
        bzlAuthLocalDir + FILE_SEPARATOR + BzlTokenPasswordManager.BZL_AUTH_TMP_FILENAME;

    this.setName("BzlTokenPasswordFetcherThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    LOG.info("BzlTokenPasswordFetcherThread is starting.");
    while (true) {
      try {
        LOG.info("BzlTokenPasswordFetcherThread is running.");
        String propertyString =
            BzlJsonUtils.getTokenPasswordContext(bzlAuthUrlEndpoint, bzlAuthUrlPasswordApi,
                bzlAuthUrlWhiteListApi, bzlAuthUrlAc, bzlAuthUrlSk, rpcBzlDatastarMetrics);
        BzlAuthFileUtils.writeTokenPasswordToTmpFile(propertyString, bzlTmpAuthFilePath,
            rpcBzlDatastarMetrics);
        BzlAuthFileUtils.copyTokenPasswordFile(bzlTmpAuthFilePath, bzlAuthFilePath,
            rpcBzlDatastarMetrics);
      } catch (Exception e) {
        LOG.warn("BzlTokenPasswordFetcherThread catch exception. The detail is {}.",
            e.getMessage());
      }

      try {
        Thread.sleep(fetcherPeriod);
      } catch (InterruptedException e) {
        LOG.warn("BzlTokenPasswordFetcherThread interruptedException. The detail is {}.",
            e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
  }
}
