/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ipc.metrics.RpcMetrics.getMetricsTimeUnit;

/**
 * This class is for maintaining the various RpcBzlTokenAuthMetrics statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Aggregate RpcBzlTokenAuth metrics", context = "rpc")
public class RpcBzlTokenAuthMetrics {
  static final Logger LOG = LoggerFactory.getLogger(RpcBzlTokenAuthMetrics.class);
  final Server server;
  final MetricsRegistry registry;
  final String name;
  final boolean rpcQuantileEnable;
  private final TimeUnit metricsTimeUnit;

  RpcBzlTokenAuthMetrics(Server server, Configuration conf) {
    String port = String.valueOf(server.getListenerAddress().getPort());
    name = "RpcBzlTokenAuthForPort" + port;
    this.server = server;
    registry = new MetricsRegistry(name);

    int[] intervals = conf.getInts(
        CommonConfigurationKeys.RPC_METRICS_PERCENTILES_INTERVALS_KEY);
    rpcQuantileEnable = (intervals.length > 0) && conf.getBoolean(
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE,
        CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE_DEFAULT);
    metricsTimeUnit = getMetricsTimeUnit(conf);
    if (rpcQuantileEnable) {
      rpcBzlTokenAuthTimeQuantiles =
          new MutableQuantiles[intervals.length];

      for (int i = 0; i < intervals.length; i++) {
        int interval = intervals[i];
        rpcBzlTokenAuthTimeQuantiles[i] = registry.newQuantiles("rpcBzlTokenAuthTime"
                + interval + "s", "rpc BzlTokenAuth time in " + metricsTimeUnit, "ops",
            "latency", interval);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialized " + registry);
    }
  }

  public String getName() {
    return name;
  }

  public static RpcBzlTokenAuthMetrics create(Server server, Configuration conf) {
    RpcBzlTokenAuthMetrics m = new RpcBzlTokenAuthMetrics(server, conf);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  @Metric("Number of bzlToken authentication successes")
  MutableCounterLong rpcBzlTokenAuthSuccesses;
  @Metric("Number of bzlToken authentication failures")
  MutableCounterLong rpcBzlTokenAuthFailures;
  @Metric("Number of bzlToken format errors")
  MutableCounterLong rpcBzlTokenFormatErrors;
  @Metric("Number of bzltoken nullPoint numbers")
  MutableCounterLong rpcBzlTokenNullPointNumbers;
  @Metric("Number of bzltoken server side missing password")
  MutableCounterLong rpcBzlTokenServerMissingPassword;
  @Metric("Number of bzltoken server side exception numbers")
  MutableCounterLong rpcBzlTokenServerExceptionNumbers;


  @Metric("BzlTokenAuth time")
  MutableRate rpcBzlTokenAuthTime;
  MutableQuantiles[] rpcBzlTokenAuthTimeQuantiles;

  public void incrBzlTokenAuthSuccesses() {
    rpcBzlTokenAuthSuccesses.incr();
  }

  public void incrBzlTokenAuthFailures() {
    rpcBzlTokenAuthFailures.incr();
  }

  public void incrBzlTokenFormatErrors() {
    rpcBzlTokenFormatErrors.incr();
  }

  public void incrBzlTokenNullPointNumbers() {
    rpcBzlTokenNullPointNumbers.incr();
  }

  public void incrBzlTokenServerMissingPassword() {
    rpcBzlTokenServerMissingPassword.incr();
  }

  public void incrBzlTokenServerExceptionNumbers() {
    rpcBzlTokenServerExceptionNumbers.incr();
  }

  public void addRpcBzlTokenAuthTime(long qTime) {
    rpcBzlTokenAuthTime.add(qTime);
    if (rpcQuantileEnable) {
      for (MutableQuantiles q : rpcBzlTokenAuthTimeQuantiles) {
        q.add(qTime);
      }
    }
  }
}
