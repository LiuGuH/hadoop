package org.apache.hadoop.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.apache.hadoop.security.bzl.util.BzlJsonUtils;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT;

public class RpcRateLimiterGlobalEnableThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(RpcRateLimiterGlobalEnableThread.class);
  private static volatile boolean enable = true;

  public RpcRateLimiterGlobalEnableThread() {
    this.setName("RpcRateLimiterGlobalEnableThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {
    LOG.info("RpcRateLimiterGlobalEnableThread is starting.");

    while (true) {
      try {
        String url = BzlDynamicConfiguration.getInstance()
            .get(CommonConfigurationKeysPublic.IPC_SERVER_RATE_LIMIT_ENABLE_URI, "");

        String result = BzlJsonUtils.getBzlGlobalEnableFromHttp(url, "[BDH]rpcRateLimiterEnable");

        if (result.equalsIgnoreCase("false") && enable) {
          LOG.warn("Disable rpcRateLimiter globally from {}", url);
          enable = false;
        } else if (result.equalsIgnoreCase("true") && !enable) {
          LOG.info("Enable rpcRateLimiter globally from {}", url);
          enable = true;
        }

      } catch (Exception e) {
        LOG.warn("RpcRateLimiterGlobalEnableThread throw exception:", e);
      }

      try {
        Thread.sleep(BzlDynamicConfiguration.getInstance()
            .getLong(IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD,
                IPC_SERVER_RATE_LIMIT_RULES_DYNAMIC_UPDATE_PERIOD_DEFAULT));
      } catch (InterruptedException e) {
        LOG.warn("RpcRateLimiterGlobalEnableThread catch interruptedException:", e);
        Thread.currentThread().interrupt();
      }
    }

  }

  public static boolean isGlobalEnable() {
    return enable;
  }

}

