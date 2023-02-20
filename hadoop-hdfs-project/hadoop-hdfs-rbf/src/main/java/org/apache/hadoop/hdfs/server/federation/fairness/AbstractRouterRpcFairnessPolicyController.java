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

package org.apache.hadoop.hdfs.server.federation.fairness;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_USER_HANDLER_CONFIG;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_USER_HANDLER_CONFIG_DEFAULT;

/**
 * Base fairness policy that implements @RouterRpcFairnessPolicyController.
 * Internally a map of nameservice to Semaphore is used to control permits.
 */
public class AbstractRouterRpcFairnessPolicyController
    implements RouterRpcFairnessPolicyController {

  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractRouterRpcFairnessPolicyController.class);

  /** Hash table to hold semaphore for each configured name service. */
  private Map<String, Semaphore> permits;
  private Map<String, Semaphore> userPermits;


  private long acquireTimeoutMs = DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT_DEFAULT;

  private Map<String,Integer> userMaxPermits;

  public void init(Configuration conf) {
    this.permits = new HashMap<>();
    this.userPermits = new HashMap<>();
    long timeoutMs = conf.getTimeDuration(DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT,
        DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    if (timeoutMs >= 0) {
      acquireTimeoutMs = timeoutMs;
    } else {
      LOG.warn("Invalid value {} configured for {} should be greater than or equal to 0. " +
          "Using default value of : {}ms instead.", timeoutMs,
          DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT, DFS_ROUTER_FAIRNESS_ACQUIRE_TIMEOUT_DEFAULT);
    }
    initUserMaxPermits();
  }

  public static String combineNsIdUser(String nsId, String user) {
    return nsId + ":" + user;
  }

  private void initUserMaxPermits() {
    this.userMaxPermits = new HashMap<>();
    String value = getVaildConfig();
    String userPermits[] = value.split(",");
    for (String userPermit : userPermits) {
      String userPermitValue[] = userPermit.split(":");
      userMaxPermits.put(userPermitValue[0], Integer.valueOf(userPermitValue[1]));
      LOG.info("User {} maxpermit  is {}.", userPermitValue[0], userPermitValue[1]);
    }
  }

  private String getVaildConfig(){
    String value = BzlDynamicConfiguration.getInstance().get(DFS_ROUTER_FAIR_USER_HANDLER_CONFIG,
        DFS_ROUTER_FAIR_USER_HANDLER_CONFIG_DEFAULT);
    if (value == null || !value.contains("other")) {
      LOG.warn(
          "The config key : dfs.federation.router.fairness.user.handler.config is incorrect! The value is {}.",
          value);
      value = DFS_ROUTER_FAIR_USER_HANDLER_CONFIG_DEFAULT;
      return value;
    }

    String userPermits[] = value.split(",");
    for (String userPermit : userPermits) {
      String userPermitValue[] = userPermit.split(":");
      if (userPermitValue.length != 2 || userPermitValue[0]==null || userPermitValue[0].equals("")) {
        LOG.warn(
            "The config key : dfs.federation.router.fairness.user.handler.config is incorrect! The value is {}.",
            value);
        value = DFS_ROUTER_FAIR_USER_HANDLER_CONFIG_DEFAULT;
        return value;
      }

      try {
        Integer.valueOf(userPermitValue[1]);
      } catch (NumberFormatException e) {
        LOG.warn(
            "The config key : dfs.federation.router.fairness.user.handler.config is incorrect! The value is {}.",
            value);
        value = DFS_ROUTER_FAIR_USER_HANDLER_CONFIG_DEFAULT;
        return value;
      }
    }
    return value;
  }

  @Override
  public boolean acquirePermit(String nsId) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Taking lock for nameservice {}", nsId);
      }
      return this.permits.get(nsId).tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot get a permit for nameservice {}", nsId);
      }
    }
    return false;
  }

  @Override
  public boolean acquireUserPermit(String nsId, String user) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Taking lock for nameservice {}, user {}", nsId, user);
      }

      if (this.userPermits.get(combineNsIdUser(nsId, user)) == null) {
        synchronized (this) {
          Integer userMax = userMaxPermits.get(user) == null ? userMaxPermits.get("other") :
              userMaxPermits.get(user);
          if (this.userPermits.get(combineNsIdUser(nsId, user)) == null) {
            this.userPermits.put(combineNsIdUser(nsId, user), new Semaphore(userMax));
          }
        }
      }
      return this.userPermits.get(combineNsIdUser(nsId, user))
          .tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot get a permit for nameservice {}, user {}", nsId, user);
      }
    }
    return false;
  }

  @Override
  public void releasePermit(String nsId) {
    this.permits.get(nsId).release();
  }

  @Override
  public void releaseUserPermit(String nsId, String user) {
    this.userPermits.get(combineNsIdUser(nsId, user)).release();
  }

  @Override
  public void shutdown() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shutting down router fairness policy controller");
    }
    // drain all semaphores
    for (Semaphore sema: this.permits.values()) {
      sema.drainPermits();
    }
  }

  protected void insertNameServiceWithPermits(String nsId, int maxPermits) {
    this.permits.put(nsId, new Semaphore(maxPermits));
  }

  @Override
  public int getAvailablePermits(String nsId) {
    return this.permits.get(nsId).availablePermits();
  }

  @Override
  public int getAvailableUserPermits(String nsId, String user) {
    return this.userPermits.get(combineNsIdUser(nsId, user)).availablePermits();
  }

  @Override
  public String getAvailableHandlerOnPerNs() {
    JSONObject json = new JSONObject();
    permits.forEach((k, v) -> {
      try {
        json.put(k, v.availablePermits());
      } catch (JSONException e) {
        LOG.warn("Cannot put {} into JSONObject", k, e);
      }
    });
    return json.toString();
  }

  @Override
  public String getAvailableHandlerOnPerNsUser() {
    JSONObject json = new JSONObject();
    userPermits.forEach((k, v) -> {
      try {
        json.put(k, v.availablePermits());
      } catch (JSONException e) {
        LOG.warn("Cannot put {} into JSONObject", k, e);
      }
    });
    return json.toString();
  }

  public Map<String, Semaphore> getUserPermits() {
    return userPermits;
  }
}
