package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.Iterator;
import java.util.Set;

public class FairnessControllerMetrics implements MetricsSource {

  private RouterRpcServer rpcServer;
  private Configuration conf;

  public FairnessControllerMetrics() {

  }

  public FairnessControllerMetrics(RouterRpcServer rpcServer, Configuration conf) {
    this.rpcServer = rpcServer;
    this.conf = conf;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms.getSource(FairnessControllerMetrics.class.getName()) == null) {
      ms.register(FairnessControllerMetrics.class.getName(),
          "HDFS FairnessController Metrics", this);
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(FederationRPCMetrics.class.getName())
        .setContext("dfs");

    Set<String> allConfiguredNS = FederationUtil.getAllConfiguredNS(conf);
    Iterator<String> iterator = allConfiguredNS.iterator();
    while (iterator.hasNext()) {
      String ns = iterator.next();
      long acceptedPermits = this.rpcServer.getRPCClient().getAcceptedPermitForNs(ns);
      long rejectedPermits = this.rpcServer.getRPCClient().getRejectedPermitForNs(ns);
      int availablePermits = this.rpcServer.getRPCClient().getRouterRpcFairnessPolicyController()
          .getAvailablePermits(ns);
      rb.addGauge(buildAcceptedPermitsMetricsInfo(ns), acceptedPermits);
      rb.addGauge(buildRejectedPermitsMetricsInfo(ns), rejectedPermits);
      rb.addGauge(buildAvailablePermitsMetricsInfo(ns), availablePermits);
    }
    rb.addGauge(buildAcceptedPermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getAcceptedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS));

    rb.addGauge(buildRejectedPermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getRejectedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS));

    rb.addGauge(buildAvailablePermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getRouterRpcFairnessPolicyController()
            .getAvailablePermits(RouterRpcFairnessConstants.CONCURRENT_NS));
  }

  private MetricsInfo buildAcceptedPermitsMetricsInfo(String ns) {
    return Interns.info("nameservice=" + ns
        + ".acceptedPermits", "AcceptedPermits");
  }

  private MetricsInfo buildRejectedPermitsMetricsInfo(String ns) {
    return Interns.info("nameservice=" + ns
        + ".rejectedPermits", "RejectedPermits");
  }

  private MetricsInfo buildAvailablePermitsMetricsInfo(String ns) {
    return Interns.info("nameservice=" + ns
        + ".availablePermits", "AvailablePermits");
  }
}
