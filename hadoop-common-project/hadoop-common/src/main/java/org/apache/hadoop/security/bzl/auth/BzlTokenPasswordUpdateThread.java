package org.apache.hadoop.security.bzl.auth;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordFetcherMetrics;
import org.apache.hadoop.ipc.metrics.RpcBzlTokenPasswordLoaderMetrics;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean;
import org.apache.hadoop.security.bzl.bean.BzlPasswordBean.PasswordData;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlAuthFileUtils;
import org.apache.hadoop.security.bzl.util.BzlEncryptionUtils;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

public class BzlTokenPasswordUpdateThread extends Thread {
  static final Logger LOG = LoggerFactory.getLogger(BzlTokenPasswordUpdateThread.class);
  public final static String SPECIFIED_GROUP_PASSWORD = "specified_groupPassword";
  public final static String GROUP_PASSWORD_FILE_PREFIX = "groupPassword_";
  private RpcBzlTokenPasswordLoaderMetrics rpcBzlTokenPasswordLoaderMetrics;
  private RpcBzlTokenPasswordFetcherMetrics rpcBzlTokenPasswordFetcherMetrics;

  private String bzlAuthLocalDir;
  private long updatePeriod;

  private String bzlAuthUrl;
  private String bzlAuthUrlAc;
  private String bzlAuthUrlSk;
  private String bzlAuthFilePathPrefix;
  List<PasswordData> previousPasswordList;

  public BzlTokenPasswordUpdateThread(Configuration conf) {
    this.rpcBzlTokenPasswordLoaderMetrics = RpcBzlTokenPasswordLoaderMetrics.create();
    this.rpcBzlTokenPasswordFetcherMetrics = RpcBzlTokenPasswordFetcherMetrics.create();

    this.bzlAuthLocalDir = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR);
    this.bzlAuthFilePathPrefix =
        bzlAuthLocalDir + File.separator + GROUP_PASSWORD_FILE_PREFIX;
    this.updatePeriod =
        conf.getLong(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_UPDATE_PERIOD_MS, 60000l);

    this.bzlAuthUrl = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_ENDPOINT) + conf.get(
        CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_PASSWORDAPI);
    this.bzlAuthUrlAc = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_AC);
    this.bzlAuthUrlSk = conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_URL_SK);

    this.previousPasswordList = new ArrayList<>();

    this.setName("BzlTokenPasswordUpdateThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {

    while (true) {
      boolean isPasswordListUpdated = false;

      try {
        int retainVersions = BzlDynamicConfiguration.getInstance()
            .getInt(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_PASSWORD_RETAIN_VERSION,
                CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_PASSWORD_RETAIN_VERSION_DEFAULT);
        List<Path> latestVersionPaths =
            BzlAuthFileUtils.findLatestGroupPasswordFileAndRetainMaxVersions(bzlAuthLocalDir,
                SPECIFIED_GROUP_PASSWORD, GROUP_PASSWORD_FILE_PREFIX, retainVersions);

        if (latestVersionPaths.size() > 0) {
          LOG.debug("BzlTokenPasswordUpdateThread find latest groupPassword file: {}",
              latestVersionPaths.get(0).toString());
          // load
          List<PasswordData> passwordList =
              BzlAuthFileUtils.readPasswordFile(latestVersionPaths.get(0).toString());
          HashMap<String, ArrayList<String>> encodePasswordMap =
              convertToHashMap(passwordList, rpcBzlTokenPasswordLoaderMetrics);

          ConcurrentHashMap<String, ArrayList<String>> decodePasswordMap =
              getDecodePasswordMap(encodePasswordMap);
          if (BzlTokenPasswordManager.getInstance().updateDecodePasswordMap(decodePasswordMap)) {
            rpcBzlTokenPasswordLoaderMetrics.incrBzlTokenPasswordChangeNumbers();
            LOG.info("BzlTokenPasswordManager has changed！New ConcurrentHashMap size is {}.",
                decodePasswordMap.size());
            previousPasswordList = passwordList;
            rpcBzlTokenPasswordLoaderMetrics.setBzlTokenPasswordLoaderUserCount(decodePasswordMap.size());
          }
        }

        if (latestVersionPaths.isEmpty() || !latestVersionPaths.get(0).toString()
            .endsWith(SPECIFIED_GROUP_PASSWORD)) {
          // fetch from remote url
          List<BzlPasswordBean.PasswordData> remotePasswordList =
              BzlJsonUtils.getGroupPasswordList(bzlAuthUrl, bzlAuthUrlAc, bzlAuthUrlSk,
                  rpcBzlTokenPasswordFetcherMetrics, "[BDH]groupQuery");

          // write local password file if changed
          if (!remotePasswordList.isEmpty() && !previousPasswordList.equals(remotePasswordList)) {
            BzlAuthFileUtils.writePasswordFile(remotePasswordList,
                bzlAuthFilePathPrefix + getCurrentDataString(), rpcBzlTokenPasswordFetcherMetrics);
            isPasswordListUpdated = true;
          }
        }
      } catch (Exception e) {
        LOG.warn("BzlTokenPasswordUpdateThread catch exception:", e);
        isPasswordListUpdated = false;
        rpcBzlTokenPasswordFetcherMetrics.incrBzlTokenPasswordUpdateFailures();
      }

      try {
        if (!isPasswordListUpdated) {
          Thread.sleep(updatePeriod);
        }
      } catch (InterruptedException e) {
        LOG.warn("BzlTokenPasswordUpdateThread catch interruptedException:", e);
        Thread.currentThread().interrupt();
      }

    }
  }

  private static HashMap<String, ArrayList<String>> convertToHashMap(
      List<PasswordData> passwordList,
      RpcBzlTokenPasswordLoaderMetrics rpcBzlTokenPasswordLoaderMetrics) {
    HashMap<String, ArrayList<String>> resultMap = new HashMap<>();

    for (BzlPasswordBean.PasswordData data : passwordList) {
      ArrayList<String> list = new ArrayList<>();
      if(data.groupAccountIsEmpty()) {
        LOG.warn("BzlTokenPasswordLoader found GroupAccount is empty.");
        rpcBzlTokenPasswordLoaderMetrics.incrzlTokenPasswordLoaderAccountEmpty();
        continue;
      }

      if(data.allPasswordIsEmpty()) {
        LOG.debug("BzlTokenPasswordLoader found GroupAccount {} has empty password.", data.getGroupAccount());
        rpcBzlTokenPasswordLoaderMetrics.incrBzlTokenPasswordLoaderPasswordEmpty();
        continue;
      }

      if (!data.groupPasswordIsEmpty()) {
        list.add(data.getGroupPassword());
      }

      if (!data.expiringGroupPasswordIsEmpty()) {
        list.add(data.getExpiringGroupPassword());
      }

      resultMap.put(data.getGroupAccount(), list);
    }

    return resultMap;
  }

  private ConcurrentHashMap<String, ArrayList<String>> getDecodePasswordMap(HashMap<String, ArrayList<String>> encodePasswordMap) {
    ConcurrentHashMap<String, ArrayList<String>> decodePasswordMap = new ConcurrentHashMap<>();

    for (Map.Entry<String, ArrayList<String>> entry : encodePasswordMap.entrySet()) {
      String groupAccount = entry.getKey();
      ArrayList<String> encodePasswordList = entry.getValue();
      ArrayList<String> decodePasswordList = new ArrayList<>();

      for (String password : encodePasswordList) {
        String decodePassword = BzlEncryptionUtils.decode(password);
        if (StringUtils.isBlank(decodePassword)) {
          LOG.warn("BzlEncryptionUtils.decode account user {} password throw exception.", groupAccount);
          rpcBzlTokenPasswordLoaderMetrics.incrBzlTokenPasswordLoaderDecodeException();
          continue;
        }
        decodePasswordList.add(decodePassword);
      }

      if (!decodePasswordList.isEmpty()) {
        decodePasswordMap.put(groupAccount, decodePasswordList);
      }
    }

    if(!BzlAuthFileUtils.checkSystemAccount(decodePasswordMap)) {
      decodePasswordMap.clear();
      rpcBzlTokenPasswordLoaderMetrics.incrBzlTokenPasswordLoaderMissingSystemAccount();
    }

    return decodePasswordMap;
  }


  public static String getCurrentDataString() {
    LocalDateTime now = LocalDateTime.now();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    return now.format(formatter);
  }
}
