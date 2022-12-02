package org.apache.hadoop.security.bzl.dynamicconfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BZLDynamicConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(BZLDynamicConfiguration.class);
  private static final BZLDynamicConfiguration INSTANCE = new BZLDynamicConfiguration();
  private final ConcurrentHashMap<String, String> bzlDynamicConfigMap = new ConcurrentHashMap();

  private BZLDynamicConfiguration() {
  }

  public void updateConfiguration(Configuration conf) {
    //更新bzlDynamicConfigMap
    for (Map.Entry<String, String> item : conf) {
      bzlDynamicConfigMap.put(item.getKey(), item.getValue());
    }
    //去掉本次bzl-dynamic.xml中不存在的key
    bzlDynamicConfigMap.entrySet().removeIf(item -> conf.get(item.getKey()) == null);

    if (LOG.isDebugEnabled()) {
      Iterator<Map.Entry<String, String>> printIterator = bzlDynamicConfigMap.entrySet().iterator();
      while (printIterator.hasNext()) {
        Map.Entry<String, String> next = printIterator.next();
        LOG.debug("After removed all unuseless config, the dynamic conf is: {}={}", next.getKey(),
            next.getValue());
      }
    }
  }

  public static BZLDynamicConfiguration getInstance() {
    return INSTANCE;
  }


  public String get(String key, String defaultValue) {
    return bzlDynamicConfigMap.getOrDefault(key, defaultValue);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String valueString = bzlDynamicConfigMap.getOrDefault(key, String.valueOf(defaultValue));
    return StringUtils.equalsIgnoreCase("true", valueString);
  }


  public void init(Configuration conf) {
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_BZL_DYNAMIC_CONFIG_ENABLE, false)) {
      new BzlDynamicConfigLoaderThread(
          conf.getLong(CommonConfigurationKeys.HADOOP_BZL_DYNAMIC_CONFIG_PERIOD, 30000)).start();
    }
  }
}