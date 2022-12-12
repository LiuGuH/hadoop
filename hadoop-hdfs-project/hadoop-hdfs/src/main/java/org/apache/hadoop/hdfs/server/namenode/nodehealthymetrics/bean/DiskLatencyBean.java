package org.apache.hadoop.hdfs.server.namenode.nodehealthymetrics.bean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;

import java.util.Map;

public class DiskLatencyBean {
  @JsonProperty("SlowDiskID")
  private String slowDiskID;
  @JsonProperty("Latencies")
  private Map<SlowDiskReports.DiskOp, Double> latencyMap;
  @JsonIgnore
  private long timestamp;

  public DiskLatencyBean() { }

  public DiskLatencyBean(String slowDiskID, Map<SlowDiskReports.DiskOp, Double> latencyMap,
                         long timestamp) {
    this.slowDiskID = slowDiskID;
    this.latencyMap = latencyMap;
    this.timestamp = timestamp;
  }

  public DiskLatencyBean(String slowDiskID) {
    this.slowDiskID = slowDiskID;
  }

  public String getSlowDiskID() {
    return slowDiskID;
  }

  public void setSlowDiskID(String slowDiskID) {
    this.slowDiskID = slowDiskID;
  }

  public Map<SlowDiskReports.DiskOp, Double> getLatencyMap() {
    return latencyMap;
  }

  public void setLatencyMap(Map<SlowDiskReports.DiskOp, Double> latencyMap) {
    this.latencyMap = latencyMap;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
