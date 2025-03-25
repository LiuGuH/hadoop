/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterAdmin;
import org.apache.hadoop.test.Whitebox;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createNamenodeReport;

public class TestAsyncRouterAdmin extends TestRouterAdmin {

  @BeforeClass
  public static void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(false, 1);
    // Build and start a router with State Store + admin + RPC.
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_ASYNC_RPC_ENABLE, true);
    cluster.addRouterOverrides(conf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    stateStore = router.getStateStore();

    // Add two name services for testing disabling.
    ActiveNamenodeResolver membership = router.getNamenodeResolver();
    membership.registerNamenode(
        createNamenodeReport("ns0", "nn1", HAServiceProtocol.HAServiceState.ACTIVE));
    membership.registerNamenode(
        createNamenodeReport("ns1", "nn1", HAServiceProtocol.HAServiceState.ACTIVE));
    stateStore.refreshCaches(true);

    RouterRpcServer spyRpcServer =
        Mockito.spy(routerContext.getRouter().createRpcServer());
    Whitebox
        .setInternalState(routerContext.getRouter(), "rpcServer", spyRpcServer);
    Mockito.doReturn(null).when(spyRpcServer).getFileInfo(Mockito.anyString());
  }
}
