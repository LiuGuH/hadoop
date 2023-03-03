package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.hdfs.server.federation.metrics.bean.ClientConnectionBean;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

public class AsyncCallerPoolMetrics implements MetricsSource {

  private RouterRpcClient routerRpcClient;

  public AsyncCallerPoolMetrics() {
  }

  public AsyncCallerPoolMetrics(RouterRpcClient routerRpcClient) {
    this.routerRpcClient = routerRpcClient;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource(AsyncCallerPoolMetrics.class.getName()) == null) {
      ms.register(AsyncCallerPoolMetrics.class.getName(),
          "HDFS AsyncCallerPool Metrics", this);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(AsyncCallerPoolMetrics.class.getName())
        .setContext("dfs");

    int active = this.routerRpcClient.getExecutorService().getActiveCount();
    int total = this.routerRpcClient.getExecutorService().getPoolSize();
    int max = this.routerRpcClient.getExecutorService().getMaximumPoolSize();

    rb.addGauge(buildAsyncCallerPoolActive(), active);
    rb.addGauge(buildAsyncCallerPoolMax(), max);
    rb.addGauge(buildAsyncCallerPoolTotal(), total);
  }

  private MetricsInfo buildAsyncCallerPoolActive() {
    return Interns.info("AsyncCaller_active", "AsyncCallerPool active");
  }

  private MetricsInfo buildAsyncCallerPoolTotal() {
    return Interns.info("AsyncCaller_total", "AsyncCallerPool total");
  }

  private MetricsInfo buildAsyncCallerPoolMax() {
    return Interns.info("AsyncCaller_max", "AsyncCallerPool max");
  }
}
