package org.apache.hadoop;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.router.SecurityConfUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcRateLimiter;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_URL;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestNNRpcRateLimiter {

  private static MiniRouterDFSCluster cluster;

  public static void createCluster() throws IOException {
    createCluster(false);
  }

  public static void createCluster(boolean security) throws IOException {
    createCluster(true, 2, security);
  }

  public static void createCluster(
      boolean ha, int numNameServices, boolean security) throws IOException {
    try {
      Configuration conf = null;
      if (security) {
        conf = SecurityConfUtil.initSecurity();
      }
      cluster = new MiniRouterDFSCluster(ha, numNameServices, conf);

      // Start NNs and DNs and wait until ready
      cluster.startCluster(conf);

      // Start routers with only an RPC service
      cluster.startRouters();

      // Register and verify all NNs with all routers
      cluster.registerNamenodes();
      cluster.waitNamenodeRegistration();

      // Setup the mount table
      cluster.installMockLocations();

      // Making one Namenodes active per nameservice
      if (cluster.isHighAvailability()) {
        for (String ns : cluster.getNameservices()) {
          cluster.switchToActive(ns, NAMENODES[0]);
          cluster.switchToStandby(ns, NAMENODES[1]);
        }
      }

      cluster.waitActiveNamespaces();
    } catch (Exception e) {
      destroyCluster();
      throw new IOException("Cannot start federated cluster", e);
    }
  }

  public static void destroyCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    try {
      SecurityConfUtil.destroy();
    } catch (Exception e) {
      throw new IOException("Cannot destroy security context", e);
    }
  }

  public static MiniDFSCluster getCluster() {
    return cluster.getCluster();
  }

  public static MiniRouterDFSCluster getRouterCluster() {
    return cluster;
  }

  public static FileSystem getFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", cluster);
    return cluster.getRandomRouter().getFileSystem();
  }

  @Test
  public void testRealClientIP() throws IOException {
    BzlDynamicConfiguration.getInstance().set("ipc.server.rate.limit.enable","true");
    BzlDynamicConfiguration.getInstance().set("ipc.server.rate.limit.rules","*,mkdirs,1.1.1.1/32,*:0");
    BzlDynamicConfiguration.getInstance().set("ipc.server.rate.limit.local.config.enable","true");

    createCluster();
    CallerContext ctx = new CallerContext.Builder("clientIp:1.1.1.1").build();
    CallerContext.setCurrent(ctx);

    FileSystem fs = getFileSystem();
    boolean sucess = false;
    try {
      sucess = fs.mkdirs(new Path("/a"));
    } catch (IOException e) {
      assertTrue(e instanceof RemoteException);
      ((RemoteException)e).getClassName();
    }
    assertFalse(sucess);
  }


  @Test
  public void testSplit() {
    String s = "a,,c";
    String[] items = s.split(",");
    System.out.println(items.length);
    for (String item : items) {
      System.out.println("---"+item+"---");
    }
  }

  @Test
  public void testGetRpcRatelimiterUrl() throws Exception {
    String url =
        "https://datastar.kanzhun-inc.com//api/guardian/openapi/limitRule/queryLimitRuleContentByCode/yj-hadoop/yj-hdfs6";
    BzlDynamicConfiguration.getInstance().set(IPC_SERVER_RATE_LIMIT_RULES_URL, url);
    String result = RpcRateLimiter.getInstance().getRefreshRpcRateLimitThread().getRateLimiterRules();
    // result should be "" if request success but the rules is emtpy
    // result should be rules if request success but the rules is config
    // result should be null if request failed
  }

}
