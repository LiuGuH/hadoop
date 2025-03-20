package org.apache.hadoop.security.bzl.dynamicconfig;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BzlDynamicConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(BzlDynamicConfiguration.class);
  private static final BzlDynamicConfiguration INSTANCE = new BzlDynamicConfiguration();
  private final ConcurrentHashMap<String, String> bzlDynamicConfigMap = new ConcurrentHashMap();

  private BzlDynamicConfiguration() {
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

  public static BzlDynamicConfiguration getInstance() {
    return INSTANCE;
  }


  public String get(String key, String defaultValue) {
    return bzlDynamicConfigMap.getOrDefault(key, defaultValue);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    String valueString = bzlDynamicConfigMap.getOrDefault(key, String.valueOf(defaultValue));
    return StringUtils.equalsIgnoreCase("true", valueString);
  }

  public int getInt(String key, long defaultValue) {
    String valueString = bzlDynamicConfigMap.getOrDefault(key, String.valueOf(defaultValue));
    return Integer.parseInt(valueString);
  }

  public long getLong(String key, long defaultValue) {
    String valueString = bzlDynamicConfigMap.getOrDefault(key, String.valueOf(defaultValue));
    return Long.parseLong(valueString);
  }

  public long getTimeDuration(String name, String defaultValue, TimeUnit unit) {
    return getTimeDuration(name, defaultValue, unit, unit);
  }

  public long getTimeDuration(String name, String defaultValue,
                              TimeUnit defaultUnit, TimeUnit returnUnit) {
    String vStr = get(name, defaultValue);
    if (null == vStr) {
      return getTimeDurationHelper(name, defaultValue, defaultUnit, returnUnit);
    } else {
      return getTimeDurationHelper(name, vStr, defaultUnit, returnUnit);
    }
  }

  private long getTimeDurationHelper(String name, String vStr,
                                     TimeUnit defaultUnit, TimeUnit returnUnit) {
    vStr = vStr.trim();
    vStr = StringUtils.toLowerCase(vStr);
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      vUnit = ParsedTimeDuration.unitFor(defaultUnit);
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }

    long raw = Long.parseLong(vStr);
    long converted = returnUnit.convert(raw, vUnit.unit());
    if (vUnit.unit().convert(converted, returnUnit) < raw) {
      LOG.warn("Possible loss of precision converting " + vStr
          + vUnit.suffix() + " to " + returnUnit + " for " + name);
    }
    return converted;
  }

  public void init(Configuration conf) {
    if (conf.getBoolean(CommonConfigurationKeys.HADOOP_BZL_DYNAMIC_CONFIG_ENABLE, false)) {
      new BzlDynamicConfigLoaderThread(
          conf.getLong(CommonConfigurationKeys.HADOOP_BZL_DYNAMIC_CONFIG_PERIOD, 30000)).start();
    }
  }

  enum ParsedTimeDuration {
    NS {
      TimeUnit unit() { return TimeUnit.NANOSECONDS; }
      String suffix() { return "ns"; }
    },
    US {
      TimeUnit unit() { return TimeUnit.MICROSECONDS; }
      String suffix() { return "us"; }
    },
    MS {
      TimeUnit unit() { return TimeUnit.MILLISECONDS; }
      String suffix() { return "ms"; }
    },
    S {
      TimeUnit unit() { return TimeUnit.SECONDS; }
      String suffix() { return "s"; }
    },
    M {
      TimeUnit unit() { return TimeUnit.MINUTES; }
      String suffix() { return "m"; }
    },
    H {
      TimeUnit unit() { return TimeUnit.HOURS; }
      String suffix() { return "h"; }
    },
    D {
      TimeUnit unit() { return TimeUnit.DAYS; }
      String suffix() { return "d"; }
    };
    abstract TimeUnit unit();
    abstract String suffix();
    static ParsedTimeDuration unitFor(String s) {
      for (ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }
    public static ParsedTimeDuration unitFor(TimeUnit unit) {
      for (ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }

  // This is only used for test case.
  @VisibleForTesting
  public String set(String key, String defaultValue) {
    return bzlDynamicConfigMap.put(key, defaultValue);
  }
}