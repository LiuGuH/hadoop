package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics.NumOpenConnectionsPerUserMetrics;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

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
      Map<String, LongAdder> acceptedPermitsPerUserForNs = this.rpcServer.getRPCClient()
          .getAcceptedPermitsPerUserForNs(ns);
      for (Map.Entry<String, LongAdder> entry : acceptedPermitsPerUserForNs.entrySet()) {
        String user = entry.getKey();
        long acceptedPermitsPerUser = entry.getValue().longValue();
        rb.addGauge(buildAcceptedPermitsPerUserMetricsInfo(ns, user), acceptedPermitsPerUser);
      }
      Map<String, LongAdder> rejectedPermitsPerUserForNs = this.rpcServer.getRPCClient()
          .getRejectedPermitsPerUserForNs(ns);
      for (Map.Entry<String, LongAdder> entry : rejectedPermitsPerUserForNs.entrySet()) {
        String user = entry.getKey();
        long rejectedPermitsPerUser = entry.getValue().longValue();
        rb.addGauge(buildRejectedPermitsPerUserMetricsInfo(ns, user), rejectedPermitsPerUser);
      }
    }
    rb.addGauge(buildAcceptedPermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getAcceptedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS));
    Map<String, LongAdder> acceptedPermitsPerUserForNs = this.rpcServer.getRPCClient()
        .getAcceptedPermitsPerUserForNs(RouterRpcFairnessConstants.CONCURRENT_NS);
    for (Map.Entry<String, LongAdder> entry : acceptedPermitsPerUserForNs.entrySet()) {
      String user = entry.getKey();
      long acceptedPermitsPerUser = entry.getValue().longValue();
      rb.addGauge(buildAcceptedPermitsPerUserMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS, user),
          acceptedPermitsPerUser);
    }
    rb.addGauge(buildRejectedPermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getRejectedPermitForNs(RouterRpcFairnessConstants.CONCURRENT_NS));
    Map<String, LongAdder> rejectedPermitsPerUserForNs = this.rpcServer.getRPCClient()
        .getRejectedPermitsPerUserForNs(RouterRpcFairnessConstants.CONCURRENT_NS);
    for (Map.Entry<String, LongAdder> entry : rejectedPermitsPerUserForNs.entrySet()) {
      String user = entry.getKey();
      long rejectedPermitsPerUser = entry.getValue().longValue();
      rb.addGauge(buildRejectedPermitsPerUserMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS, user),
          rejectedPermitsPerUser);
    }

    rb.addGauge(buildAvailablePermitsMetricsInfo(RouterRpcFairnessConstants.CONCURRENT_NS),
        this.rpcServer.getRPCClient().getRouterRpcFairnessPolicyController()
            .getAvailablePermits(RouterRpcFairnessConstants.CONCURRENT_NS));
  }

  private MetricsInfo buildAcceptedPermitsPerUserMetricsInfo(String ns, String user) {
    return Interns.info("nameservice=" + ns + ".user=" + user + ".acceptedPermits",
        "AcceptedPermitsPerUser");
  }

  private MetricsInfo buildRejectedPermitsPerUserMetricsInfo(String ns, String user) {
    return Interns.info("nameservice=" + ns + ".user=" + user + ".rejectedPermits",
        "RejectedPermitsPerUser");
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
