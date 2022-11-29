package org.apache.hadoop.security.bzl.dynamicconfig;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BzlDynamicConfigLoaderThread extends Thread {

  private static final Logger LOG =
      LoggerFactory.getLogger(BzlDynamicConfigLoaderThread.class);
  private final Configuration conf = new Configuration(false);
  private long period;

  public BzlDynamicConfigLoaderThread(long period) {
    this.period = period;
    this.setName("BzlDynamicConfigLoaderThread");
    this.setDaemon(true);
  }

  @Override
  public void run() {

    while (true) {
      try {
        conf.clear();
        conf.clearResource();
        conf.addResource("bzl-dynamic.xml");

        if (LOG.isDebugEnabled()) {
          LOG.debug("BZl dynamicconfig is {}.", conf);
        }

        BZLDynamicConfiguration.getInstance().updateConfiguration(conf);
      } catch (Exception e) {
        LOG.error("BzlDynamicConfigLoaderThread throw exception. The detail is {}.",
            e.getMessage());
      }

      try {
        Thread.sleep(period);
      } catch (InterruptedException e) {
        LOG.warn("BzlDynamicConfigLoaderThread InterruptedException. The detail is {}.",
            e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
  }
}