package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(name = "BzlProtectedDirectories", context = "dfs")
public class BzlProtectedDirectoriesMetrics {
  final MetricsRegistry registry;
  final String name;
  @Metric("Number of bzlProtectedDirectories fetch successes")
  MutableCounterLong bzlProtectedDirectoriesFetchSuccesses;
  @Metric("Number of bzlProtectedDirectories fetch failures")
  MutableCounterLong bzlProtectedDirectoriesFetchFailures;
  @Metric("Number of bzlProtectedDirectories check successes")
  MutableCounterLong bzlProtectedDirectoriesCheckSuccesses;
  @Metric("Number of bzlProtectedDirectories check failures")
  MutableCounterLong bzlProtectedDirectoriesCheckFailures;
  int bzlProtectedDirectoriesNums;
  int bzlProtectedDirectoriesSizeExceeded;
  @Metric("ProcessingTime of bzlProtectedDirectories check")
  private MutableRate bzlProtectedDirectoriesProcessingTime;

  private BzlProtectedDirectoriesMetrics() {
    name = "BzlProtectedDirectories";
    registry = new MetricsRegistry(name);
    bzlProtectedDirectoriesProcessingTime = registry.newRate("bzlProtectedDirectoriesProcessingTime");
  }

  public static BzlProtectedDirectoriesMetrics create() {
    BzlProtectedDirectoriesMetrics m = new BzlProtectedDirectoriesMetrics();
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  public void incrBzlProtectedDirectoriesFetchSuccesses() {
    bzlProtectedDirectoriesFetchSuccesses.incr();
  }

  public void incrBzlProtectedDirectoriesFetchFailures() {
    bzlProtectedDirectoriesFetchFailures.incr();
  }

  public void incrBzlProtectedDirectoriesCheckSuccesses() {
    bzlProtectedDirectoriesCheckSuccesses.incr();
  }

  public void incrBzlProtectedDirectoriesCheckFailures() {
    bzlProtectedDirectoriesCheckFailures.incr();
  }

  @Metric({"ProtectedDirectoriesNums", "Number of bzlProtectedDirectories size"})
  public int getBzlProtectedDirectoriesNums() {
    return bzlProtectedDirectoriesNums;
  }

  public void setBzlProtectedDirectoriesNums(int nums) {
    this.bzlProtectedDirectoriesNums = nums;
  }

  @Metric({"ProtectedDirectoriesSizeExceeded", "Number of bzlProtectedDirectories size exceeds max size"})
  public int getBzlProtectedDirectoriesSizeExceeded() {
    return bzlProtectedDirectoriesSizeExceeded;
  }

  public void setBzlProtectedDirectoriesSizeExceeded(int bzlProtectedDirectoriesSizeExceeded) {
    this.bzlProtectedDirectoriesSizeExceeded = bzlProtectedDirectoriesSizeExceeded;
  }

  public MutableRate getBzlProtectedDirectoriesProcessingTime() {
    return bzlProtectedDirectoriesProcessingTime;
  }
}
