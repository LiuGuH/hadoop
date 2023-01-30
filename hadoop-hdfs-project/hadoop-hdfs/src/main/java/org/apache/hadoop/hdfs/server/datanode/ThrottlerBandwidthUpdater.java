package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.bzl.dynamicconfig.BzlDynamicConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrottlerBandwidthUpdater implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ThrottlerBandwidthUpdater.class);

  private Configuration conf;
  private DataXceiverServer xserver;
  private DataXceiverServer localXceiverServer;


  public ThrottlerBandwidthUpdater(DataXceiverServer xserver, DataXceiverServer localXceiverServer, Configuration conf) {
    this.xserver = xserver;
    this.localXceiverServer = localXceiverServer;
    this.conf = conf;
  }

  @Override
  public void run() {
    while (true) {
      if (xserver != null) {
        long bandwidthPerSec = BzlDynamicConfiguration.getInstance()
            .getLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT);
        if (bandwidthPerSec <= 0) {
          bandwidthPerSec = DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT;
        }
        if (xserver.getTransferThrottler().getBandwidth() != bandwidthPerSec) {
          xserver.getTransferThrottler().setBandwidth(bandwidthPerSec);
        }

        bandwidthPerSec = BzlDynamicConfiguration.getInstance()
            .getLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT);
        if (bandwidthPerSec <= 0) {
          bandwidthPerSec = DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT;
        }
        if (xserver.getWriteThrottler().getBandwidth() != bandwidthPerSec) {
          xserver.getWriteThrottler().setBandwidth(bandwidthPerSec);
        }
      }

      if (localXceiverServer != null) {
        long bandwidthPerSec = BzlDynamicConfiguration.getInstance()
            .getLong(DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT);
        if (bandwidthPerSec <= 0) {
          bandwidthPerSec = DFSConfigKeys.DFS_DATANODE_DATA_TRANSFER_BANDWIDTHPERSEC_DEFAULT;
        }
        if (localXceiverServer.getTransferThrottler().getBandwidth() != bandwidthPerSec) {
          localXceiverServer.getTransferThrottler().setBandwidth(bandwidthPerSec);
        }

        bandwidthPerSec = BzlDynamicConfiguration.getInstance()
            .getLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
                DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT);
        if (bandwidthPerSec <= 0) {
          bandwidthPerSec = DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_DEFAULT;
        }
        if (localXceiverServer.getWriteThrottler().getBandwidth() != bandwidthPerSec) {
          localXceiverServer.getWriteThrottler().setBandwidth(bandwidthPerSec);
        }
      }

      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn(e.toString());
      }
    }
  }
}
