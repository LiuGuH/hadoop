package org.apache.hadoop.hdfs;

import java.util.LinkedList;

public class SlowMetrics implements SlowPipelineNodeStrategy {
  /* the threshold for slow metric */
  private long threshold;
  /* the count of slow metrics in the interval */
  private long count;
  /*  */
  private long intervalNs;

  private LinkedList<SlowMetric> slowMetricList = new LinkedList();

  private class SlowMetric {
    private long nanoTime;
    private long value;

    SlowMetric(long value) {
      this.value = value;
      this.nanoTime = System.nanoTime();
    }

    public long getValue() {
      return value;
    }

    public long getNanoTime() {
      return nanoTime;
    }
  }

  public SlowMetrics(long intervalNs, long threshold, long count) {
    this.intervalNs = intervalNs;
    this.threshold = threshold;
    this.count = count;
  }

  /**
   * @param value metric value
   * @return true if value > threshold, otherwise false.
   */
  public synchronized boolean putMetric(long value) {
    if (slowCounts() > 0) {
      // delete metric beyond the interval.
      deleteTimeoutMetric();
    }
    if (value >= threshold) {
      SlowMetric metric = new SlowMetric(value);
      slowMetricList.add(metric);
      return true;
    }
    return false;
  }

  private void deleteTimeoutMetric() {
    SlowMetric metric = slowMetricList.getFirst();
    long now = System.nanoTime();
    while (slowMetricList.size() > 0 &&
        metric.getNanoTime() + intervalNs < now) {
      //delete the metric beyond the interval.
      slowMetricList.removeFirst();
      if (slowMetricList.size() > 0) {
        metric = slowMetricList.getFirst();
      }
    }
  }

  public synchronized boolean inSlowState() {
    if (slowCounts() > 0) {
      // delete metric beyond the interval.
      deleteTimeoutMetric();
    }
    return slowMetricList.size() >= count;
  }

  public synchronized long slowCounts() {
    return slowMetricList.size();
  }

  public synchronized double getAvg() {
    if (slowMetricList.size() > 0) {
      double sum = 0;
      SlowMetric[] metrics = new SlowMetric[slowMetricList.size()];
      slowMetricList.toArray(metrics);
      for (SlowMetric metric : metrics) {
        sum += metric.getValue();
      }
      return sum / metrics.length;
    } else {
      return 0;
    }
  }

  public void clear() {
    slowMetricList.clear();
  }
}
