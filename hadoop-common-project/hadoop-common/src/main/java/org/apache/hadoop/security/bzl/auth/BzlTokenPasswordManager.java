package org.apache.hadoop.security.bzl.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class BzlTokenPasswordManager {
  private final static BzlTokenPasswordManager INSTANCE = new BzlTokenPasswordManager();
  private final ConcurrentHashMap<String, ArrayList<String>> concurrentHashMap = new ConcurrentHashMap<>();
  private BzlTokenPasswordUpdateThread bzlTokenPasswordUpdateThread;
  private BzlTokenPasswordGlobalEnableThread bzlTokenPasswordGlobalEnableThread;


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

      bzlTokenPasswordUpdateThread= new BzlTokenPasswordUpdateThread(conf);
      bzlTokenPasswordUpdateThread.start();

      bzlTokenPasswordGlobalEnableThread = new BzlTokenPasswordGlobalEnableThread(conf);
      bzlTokenPasswordGlobalEnableThread.start();
    }
  }

  private boolean checkConfigExist(String value) {
    return value != null;
  }

  public boolean updateDecodePasswordMap(ConcurrentHashMap<String, ArrayList<String>> hashMap) {
    if (!shouldUpdate(hashMap)) {
      return false;
    }

    concurrentHashMap.putAll(hashMap);
    concurrentHashMap.entrySet().removeIf(item -> hashMap.get(item.getKey()) == null);
    return true;
  }

  private boolean shouldUpdate(ConcurrentHashMap<String, ArrayList<String>> conMap) {
    return conMap != null && !conMap.isEmpty() && !concurrentHashMap.equals(conMap);
  }

  public long getConfigurationCount() {
    return concurrentHashMap.mappingCount();
  }

  public ArrayList<String> getUserDecodePasswordList(String userName) {
    return concurrentHashMap.get(userName);
  }

  public static boolean isBzlTokenPasswordGlobalEnable() {
    return BzlTokenPasswordGlobalEnableThread.isEnable();
  }
}