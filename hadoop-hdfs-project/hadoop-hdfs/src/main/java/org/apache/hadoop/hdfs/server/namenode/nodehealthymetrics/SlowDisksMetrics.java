package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics.bean.DiskLatencyBean;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Map;


/**
 * Slow Disks related metrics
 */
@InterfaceAudience.Private
public class SlowDisksMetrics implements MetricsSource {

  public static final Logger LOG = LoggerFactory.getLogger(SlowDisksMetrics.class);
  public static final String SLOW_DISKS_METRICS_SOURCE_NAME =
      "SlowDisks";
  private FSNamesystem fsNamesystem;
  private static final ObjectMapper objMapper = new ObjectMapper();
  private final boolean isSlowDiskMetricsSourceEnabled;

  public SlowDisksMetrics(Configuration conf, FSNamesystem fsNamesystem) {
    this.fsNamesystem = fsNamesystem;
    this.fsNamesystem = fsNamesystem;
    this.isSlowDiskMetricsSourceEnabled =
        conf.getBoolean(DFSConfigKeys.SLOWDISK_METRICS_ENABLED_KEY,
            DFSConfigKeys.SLOWDISK_METRICS_ENABLED_DEFAULT);
    if (DefaultMetricsSystem.instance().getSource(SLOW_DISKS_METRICS_SOURCE_NAME) == null) {
      DefaultMetricsSystem.instance().register(SLOW_DISKS_METRICS_SOURCE_NAME,
          "Slow Disks Monitor", this);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    if (!isSlowDiskMetricsSourceEnabled) {
      return;
    }

    String slowDisksJson = getSlowDisksJsonReports();
    try {
      if (StringUtils.isEmpty(slowDisksJson)) {
        return;
      }
      MetricsRecordBuilder rb = collector.addRecord(getClass().getName())
          .setContext("dfs");
      ArrayList<DiskLatencyBean> diskLatencies =
          objMapper.readValue(slowDisksJson, new TypeReference<ArrayList<DiskLatencyBean>>() {
          });
      for (DiskLatencyBean bean : diskLatencies) {
        // "SlowDiskID": "xx.xx.xx.xx:8010:/dataN/hadoop/hdfs/data/"
        String slowDiskID = bean.getSlowDiskID();
        Map<SlowDiskReports.DiskOp, Double> latencyMap = bean.getLatencyMap();
        String[] strs = slowDiskID.split(":");
        String ip;
        String path;
        if (strs.length == 3) {
          ip = strs[0];
          path = strs[2];
        } else {
          ip = "1.1.1.1";
          path = "/need/to/check";
        }
        for (Map.Entry<SlowDiskReports.DiskOp, Double> entry : latencyMap.entrySet()) {
          MetricsInfo metricsInfo = buildSlowDisksMetricsInfo(ip, path, entry.getKey());
          rb.addGauge(metricsInfo, entry.getValue());
        }
      }

    } catch (JsonProcessingException e) {
      LOG.debug("Failed to resolve diskLatency" + e);
    }
  }

  private MetricsInfo buildSlowDisksMetricsInfo(String ip, String path,
                                                SlowDiskReports.DiskOp diskOp) {
    return Interns.info("slowdisk=" + ip +
        ".path=" + path + "." + diskOp.toString(), "Slow Disks Reports");
  }

  public String getSlowDisksJsonReports() {
    return fsNamesystem.getBlockManager().getDatanodeManager().getSlowDisksReport();
  }
}
