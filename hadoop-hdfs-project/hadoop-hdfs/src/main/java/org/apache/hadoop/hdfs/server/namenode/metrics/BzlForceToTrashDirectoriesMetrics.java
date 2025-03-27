package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(name = "BzlForceToTrashDirectories", context = "dfs")
public class BzlForceToTrashDirectoriesMetrics {
  final MetricsRegistry registry;
  final String name;
  @Metric("Number of bzlForceToTrashDirectories fetch successes")
  MutableCounterLong bzlForceToTrashDirectoriesFetchSuccesses;
  @Metric("Number of bzlForceToTrashDirectories fetch failures")
  MutableCounterLong bzlForceToTrashDirectoriesFetchFailures;
  @Metric("Number of bzlForceToTrashDirectories check successes")
  MutableCounterLong bzlForceToTrashDirectoriesCheckSuccesses;
  @Metric("Number of bzlForceToTrashDirectories check failures")
  MutableCounterLong bzlForceToTrashDirectoriesCheckFailures;
  int bzlForceToTrashDirectoriesSizeExceeded;
  int bzlForceToTrashDirectoriesNums;
  @Metric("ProcessingTime of bzlForceToTrashDirectories check")
  private MutableRate bzlForceToTrashDirectoriesProcessingTime;


  private BzlForceToTrashDirectoriesMetrics() {
    name = "BzlForceToTrashDirectories";
    registry = new MetricsRegistry(name);
    bzlForceToTrashDirectoriesProcessingTime = registry.newRate("bzlForceToTrashDirectoriesProcessingTime");
  }

  public static BzlForceToTrashDirectoriesMetrics create() {
    BzlForceToTrashDirectoriesMetrics m = new BzlForceToTrashDirectoriesMetrics();
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  public void incrBzlForceToTrashDirectoriesFetchSuccesses() {
    bzlForceToTrashDirectoriesFetchSuccesses.incr();
  }

  public void incrBzlForceToTrashDirectoriesFetchFailures() {
    bzlForceToTrashDirectoriesFetchFailures.incr();
  }

  public void incrBzlForceToTrashDirectoriesCheckSuccesses() {
    bzlForceToTrashDirectoriesCheckSuccesses.incr();
  }

  public void incrBzlForceToTrashDirectoriesCheckFailures() {
    bzlForceToTrashDirectoriesCheckFailures.incr();
  }

  @Metric({"ForceToTrashDirectoriesNums", "Number of bzlForceToTrashDirectories size"})
  public int getBzlForceToTrashDirectoriesNums() {
    return bzlForceToTrashDirectoriesNums;
  }

  public void setBzlForceToTrashDirectoriesNums(int nums) {
    this.bzlForceToTrashDirectoriesNums = nums;
  }

  @Metric({"ForceToTrashDirectoriesSizeExceeded", "Number of bzlForceToTrashDirectories size exceeds max size"})
  public int getBzlForceToTrashDirectoriesSizeExceeded() {
    return bzlForceToTrashDirectoriesSizeExceeded;
  }

  public void setBzlForceToTrashDirectoriesSizeExceeded(
      int bzlForceToTrashDirectoriesSizeExceeded) {
    this.bzlForceToTrashDirectoriesSizeExceeded = bzlForceToTrashDirectoriesSizeExceeded;
  }

  public MutableRate getBzlForceToTrashDirectoriesProcessingTime() {
    return bzlForceToTrashDirectoriesProcessingTime;
  }
}
