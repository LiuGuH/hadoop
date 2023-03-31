package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

@Metrics(name = "BzlProtectedDirectors", context = "dfs")
public class BzlProtectedDirectoriesMetrics {
  final MetricsRegistry registry;
  final String name;
  @Metric("Number of bzlProtectedDirectors fetch successes")
  MutableCounterLong bzlProtectedDirectoriesFetchSuccesses;
  @Metric("Number of bzlProtectedDirectors fetch failures")
  MutableCounterLong bzlProtectedDirectoriesFetchFailures;
  @Metric("Number of bzlProtectedDirectors check successes")
  MutableCounterLong bzlProtectedDirectoriesCheckSuccesses;
  @Metric("Number of bzlProtectedDirectors check failures")
  MutableCounterLong bzlProtectedDirectoriesCheckFailures;
  int bzlProtectedDirectoriesNums;

  private BzlProtectedDirectoriesMetrics() {
    name = "BzlProtectedDirectors";
    registry = new MetricsRegistry(name);
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

  @Metric({"ProtectedDirectoriesNums", "Number of bzlProtectedDirectors size"})
  public int getBzlProtectedDirectoriesNums() {
    return bzlProtectedDirectoriesNums;
  }

  public void setBzlProtectedDirectoriesNums(int nums) {
    this.bzlProtectedDirectoriesNums = nums;
  }
}
