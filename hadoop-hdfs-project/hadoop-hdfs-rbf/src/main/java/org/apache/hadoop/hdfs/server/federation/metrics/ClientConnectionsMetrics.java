package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.hdfs.server.federation.metrics.bean.ClientConnectionBean;
import org.apache.hadoop.hdfs.server.federation.router.ConnectionPool;
import org.apache.hadoop.hdfs.server.federation.router.ConnectionPoolId;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class ClientConnectionsMetrics implements MetricsSource {

  private RouterRpcClient routerRpcClient;
  Map<ConnectionPoolId, ConnectionPool> forMetrics = new ConcurrentHashMap<>();

  public ClientConnectionsMetrics(RouterRpcClient routerRpcClient) {
    this.routerRpcClient = routerRpcClient;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource(ClientConnectionsMetrics.class.getName()) == null) {
      ms.register(ClientConnectionsMetrics.class.getName(),
          "HDFS ClientConnections Metrics", this);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(ClientConnectionsMetrics.class.getName())
        .setContext("dfs");
    forMetrics.putAll(routerRpcClient.getConnectionManager().getPools());
    for (Map.Entry<ConnectionPoolId, ConnectionPool> entry : forMetrics.entrySet()) {
      ConnectionPoolId connectionPoolId = entry.getKey();
      ConnectionPool pool = entry.getValue();
      ClientConnectionBean bean = new ClientConnectionBean();
      bean.setUser(connectionPoolId.getUserName());
      bean.setNnId(connectionPoolId.getNnId());
      bean.setActive(pool.getNumActiveConnections());
      bean.setIdle(pool.getNumIdleConnections());
      bean.setRecent_active(pool.getNumActiveConnectionsRecently());
      bean.setTotal(pool.getNumConnections());
      rb.addGauge(buildRpcClientConnectionsActive(bean), bean.getActive());
      rb.addGauge(buildRpcClientConnectionsActiveRecently(bean), bean.getRecent_active());
      rb.addGauge(buildRpcClientConnectionsIdle(bean), bean.getIdle());
      rb.addGauge(buildRpcClientConnectionsTotal(bean), bean.getTotal());
    }
    forMetrics.clear();
  }

  private MetricsInfo buildRpcClientConnectionsActive(ClientConnectionBean bean) {
    return Interns.info("nameservice=" + bean.getNnId()
        + ".user=" + bean.getUser() + ".activeConnections" , "UserActiveConnections");
  }

  private MetricsInfo buildRpcClientConnectionsIdle(ClientConnectionBean bean) {
    return Interns.info("nameservice=" + bean.getNnId()
        + ".user=" + bean.getUser() + ".idleConnections" , "UserIdleConnections");
  }

  private MetricsInfo buildRpcClientConnectionsActiveRecently(ClientConnectionBean bean) {
    return Interns.info("nameservice=" + bean.getNnId()
        + ".user=" + bean.getUser() + ".recentlyactiveConnections" , "UserRecentlyActiveConnections");
  }

  private MetricsInfo buildRpcClientConnectionsTotal(ClientConnectionBean bean) {
    return Interns.info("nameservice=" + bean.getNnId()
        + ".user=" + bean.getUser() + ".totalConnections" , "UserTotalConnections");
  }
}

