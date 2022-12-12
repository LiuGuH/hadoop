package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics.bean.LiveDataNodeBean;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Live DataNode related metrics
 */
@InterfaceAudience.Private
public class LiveNodesMetrics implements MetricsSource {
  public static final Logger LOG = LoggerFactory.getLogger(LiveNodesMetrics.class);
  public static final String LIVENODES_METRICS_SOURCE_NAME =
      "LiveNodesMetrics";

  private final boolean isMetricsSourceEnabled;
  private FSNamesystem fsNamesystem;

  public LiveNodesMetrics(Configuration conf, FSNamesystem fsNamesystem) {
    logConf(conf);
    this.fsNamesystem = fsNamesystem;
    this.isMetricsSourceEnabled = conf.getBoolean(DFSConfigKeys.LIVENODES_ENABLED_KEY,
        DFSConfigKeys.LIVENODES_ENABLED_DEFAULT);

    if (DefaultMetricsSystem.instance().getSource(LIVENODES_METRICS_SOURCE_NAME) == null) {
      DefaultMetricsSystem.instance().register(LIVENODES_METRICS_SOURCE_NAME,
          "Live DataNodes LastContact", this);
    }
  }

  private static void logConf(Configuration conf) {
    LOG.info("LiveNodes conf: " + DFSConfigKeys.LIVENODES_ENABLED_KEY +
        " = " +  conf.get(DFSConfigKeys.LIVENODES_ENABLED_KEY));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    if (!isMetricsSourceEnabled) {
      return;
    }
    MetricsRecordBuilder rb = collector.addRecord(getClass().getName())
        .setContext("dfs");
    for (final LiveDataNodeBean dataNodeBean : getAllLiveNodes()) {
      rb.addGauge(buildLastContactMetricsInfo(dataNodeBean), dataNodeBean.getLastContact())
          .addGauge(buildAdminStateMetricsInfo(dataNodeBean), dataNodeBean.getAdminState())
          .addGauge(buildScheduledBlocksMetricsInfo(dataNodeBean),
              dataNodeBean.getBlockScheduled())
          .addGauge(buildVolumeFailureMetricsInfo(dataNodeBean), dataNodeBean.getVolfails());
    }
  }

  private List<LiveDataNodeBean> getAllLiveNodes() {
    final List<LiveDataNodeBean> result = new ArrayList<>();
    final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    this.fsNamesystem.getBlockManager().getDatanodeManager()
      .fetchDatanodes(live, null, false);
    for (DatanodeDescriptor node : live) {
      LiveDataNodeBean bean = new LiveDataNodeBean();
      bean.setHostnamePlusPort(node.getHostName() + "-" + node.getXferPort());
      bean.setLastContact(getLastContact(node));
      bean.setAdminState(node.getAdminState().ordinal());
      bean.setBlockScheduled(node.getBlocksScheduled());
      bean.setVolfails(node.getVolumeFailures());
      result.add(bean);
    }
    return result;
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (monotonicNow() - alivenode.getLastUpdateMonotonic())/1000;
  }

  private MetricsInfo buildLastContactMetricsInfo(LiveDataNodeBean dataNodeBean) {
    return Interns.info("livenode=" + dataNodeBean.getHostnamePlusPort()
        + ".lastcontact", "lastcontact");
  }

  private MetricsInfo buildAdminStateMetricsInfo(LiveDataNodeBean dataNodeBean) {
    return Interns.info("livenode=" + dataNodeBean.getHostnamePlusPort()
        + ".adminstate", "adminstate");
  }

  private MetricsInfo buildScheduledBlocksMetricsInfo(LiveDataNodeBean dataNodeBean) {
    return Interns.info("livenode=" + dataNodeBean.getHostnamePlusPort()
        + ".blockscheduled", "blockscheduled");
  }

  private MetricsInfo buildVolumeFailureMetricsInfo(LiveDataNodeBean dataNodeBean) {
    return Interns.info("livenode=" + dataNodeBean.getHostnamePlusPort()
        + ".volumefailure", "volumefailure");
  }
}
