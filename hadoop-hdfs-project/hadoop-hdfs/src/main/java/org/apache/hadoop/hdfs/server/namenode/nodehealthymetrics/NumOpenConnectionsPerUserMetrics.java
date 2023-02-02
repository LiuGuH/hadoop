package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

@InterfaceAudience.Private
public class NumOpenConnectionsPerUserMetrics implements MetricsSource {

  public static final Logger LOG = LoggerFactory.getLogger(NumOpenConnectionsPerUserMetrics.class);

  private Server rpcServer;

  public NumOpenConnectionsPerUserMetrics(Server rpcServer) {
    this.rpcServer = rpcServer;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource(NumOpenConnectionsPerUserMetrics.class.getName()) == null) {
      ms.register(NumOpenConnectionsPerUserMetrics.class.getName(),
          "HDFS NumOpenConnectionsPerUser Metrics", this);
    }

  }
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(NumOpenConnectionsPerUserMetrics.class.getName())
        .setContext("dfs");
    Map<String, Integer> userOpenConnectionsMap = rpcServer.obtainUserToConnectionsMap();
    Iterator<Map.Entry<String, Integer>> iterator = userOpenConnectionsMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Integer> entry = iterator.next();
      String user = entry.getKey();
      int openConnectionsNum = entry.getValue();
      rb.addGauge(buildNumOpenConnectionsPerUserMetricsInfo(user), openConnectionsNum);
    }
  }

  private MetricsInfo buildNumOpenConnectionsPerUserMetricsInfo(String user) {
    return Interns.info("user=" + user + ".openConnections", "OpenConnections");
  }
}
