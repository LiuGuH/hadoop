package org.apache.hadoop.security.bzl.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.util.BzlEncryptionUtils;

import java.util.concurrent.ConcurrentHashMap;

public class BzlTokenPasswordManager {
  public final static String BZL_AUTH_FILENAME = "bzlauth.data";
  public final static String BZL_AUTH_TMP_FILENAME = "tmp_" + BZL_AUTH_FILENAME;
  private final static BzlTokenPasswordManager INSTANCE = new BzlTokenPasswordManager();
  private final ConcurrentHashMap<String, String> concurrentHashMap = new ConcurrentHashMap<>();
  private BzlTokenPasswordFetcherThread bzlTokenPasswordFetcherThread;
  private BzlTokenPasswordLoaderThread bzlTokenPasswordLoaderThread;


  private BzlTokenPasswordManager() {

  }

  public static BzlTokenPasswordManager getInstance() {
    return INSTANCE;
  }

  public void init(Configuration conf) {
    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_BZL_AUTH_UPDATE_ENABLE, false)) {
      if (!checkConfigExist(conf.get(CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR))) {
        new IllegalArgumentException(
            "The config " + CommonConfigurationKeys.HADOOP_BZL_AUTH_LOCALDIR + " is null.");
      }

      bzlTokenPasswordFetcherThread = new BzlTokenPasswordFetcherThread(conf);
      bzlTokenPasswordFetcherThread.start();

      try {
        //延迟3s，防止BzlTokenPasswordFetcherThread还没有完成文件更新
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      bzlTokenPasswordLoaderThread = new BzlTokenPasswordLoaderThread(conf);
      bzlTokenPasswordLoaderThread.start();
    }
  }

  private boolean checkConfigExist(String value) {
    return value != null;
  }

  public boolean checkConfigurationConsistency(ConcurrentHashMap<String, String> conMap) {
    return concurrentHashMap.equals(conMap);
  }

  public boolean updateConfiguration(ConcurrentHashMap<String, String> conMap) {
    if (!shouldUpdate((conMap))) {
      return false;
    }

    concurrentHashMap.putAll(conMap);
    concurrentHashMap.entrySet().removeIf(item -> conMap.get(item.getKey()) == null);
    return true;
  }

  private boolean shouldUpdate(ConcurrentHashMap<String, String> con) {
    return con != null &&
        !BzlTokenPasswordManager.getInstance().checkConfigurationConsistency(con) &&
        con.get("hdfs") != null && con.get("yarn") != null && con.get("hive") != null;
  }

  public long getConfigurationCount() {
    return concurrentHashMap.mappingCount();
  }

  private boolean isUserInWhiteList(String userName) {
    String userlist = concurrentHashMap.get("whiteuserlist");
    if (userlist != null && userlist.contains(userName)) {
      return true;
    }
    return false;
  }

  public boolean isUserInWhiteList(String clientRealUser, String clientUser) {
    if (isUserInWhiteList(clientUser) ||
        (clientRealUser != null && isUserInWhiteList(clientRealUser))) {
      return true;
    }
    return false;
  }

  public String[] getUserPasswordList(String userName) {
    String decodepassword[] = null;
    String password = concurrentHashMap.get(userName);

    if (password != null) {
      decodepassword = password.split(",");
      for (int i = 0; i < decodepassword.length; i++) {
        decodepassword[i] = BzlEncryptionUtils.decode(decodepassword[i]);
      }
    }
    return decodepassword;
  }
}