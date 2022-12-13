package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowPeerTracker;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowPeerTracker.ReportForJson;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Slow Peer related metrics
 */
@InterfaceAudience.Private
public class SlowPeersMetrics implements MetricsSource {

  public static final Logger LOG = LoggerFactory.getLogger(SlowPeersMetrics.class);
  public static final String SLOW_PEERS_METRICS_SOURCE_NAME =
      "SlowPeers";
  public static final int MAX_NODES_TO_REPORT = 5;

  private FSNamesystem fsNamesystem;
  private final boolean dataNodePeerStatsEnabled;
  private final boolean isSlowPeerMetricsSourceEnabled;

  public SlowPeersMetrics(Configuration conf, FSNamesystem fsNamesystem) {
    this.fsNamesystem = fsNamesystem;
    this.dataNodePeerStatsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);
    this.isSlowPeerMetricsSourceEnabled =
        conf.getBoolean(DFSConfigKeys.SLOWPEER_METRICS_ENABLED_KEY,
            DFSConfigKeys.SLOWPEER_METRICS_ENABLED_DEFAULT);
    if (DefaultMetricsSystem.instance().getSource(SLOW_PEERS_METRICS_SOURCE_NAME) == null) {
      DefaultMetricsSystem.instance().register(SLOW_PEERS_METRICS_SOURCE_NAME,
          "Slow Peers Monitor", this);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    if (!dataNodePeerStatsEnabled || !isSlowPeerMetricsSourceEnabled) {
      return;
    }
    MetricsRecordBuilder rb = collector.addRecord(getClass().getName())
        .setContext("dfs");
    Collection<ReportForJson> jsonReports = getJsonReports();
    for (ReportForJson report : jsonReports) {
      String slowNode = report.getSlowNode();
      int reportingCounts = report.getReportingNodes().size();
      MetricsInfo metricsInfo = buildSlowPeersMetricsInfo(slowNode);
      rb.addGauge(metricsInfo, reportingCounts);
    }

  }

  private MetricsInfo buildSlowPeersMetricsInfo(String slowNode) {
    return Interns.info("slowpeer=" + slowNode +
      ".reportcounts", "Slow Peers Reports");
  }

  public Collection<ReportForJson>  getJsonReports() {
    SlowPeerTracker slowPeersTracker =
        fsNamesystem.getBlockManager().getDatanodeManager().getSlowPeerTracker();
    return slowPeersTracker.getJsonReports(MAX_NODES_TO_REPORT);
  }
}
