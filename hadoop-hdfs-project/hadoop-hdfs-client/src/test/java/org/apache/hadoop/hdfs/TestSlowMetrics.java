package org.apache.hadoop.hdfs;

import org.junit.Test;

public class TestSlowMetrics {
  @Test(timeout = 30000)
  public void testSlowMetrics() throws Exception {
    SlowMetrics metrics = new SlowMetrics(100000000,10, 5);
    metrics.putMetric(1);
    assert metrics.slowCounts() == 0;
    assert metrics.inSlowState() == false;
    metrics.putMetric(10);
    metrics.putMetric(11);
    metrics.putMetric(12);
    Thread.sleep(10);
    metrics.putMetric(13);
    metrics.putMetric(14);
    metrics.putMetric(15);
    assert metrics.slowCounts() == 6;
    assert metrics.inSlowState() == true;
    assert metrics.getAvg() == 12.5;
    Thread.sleep(1000);
    metrics.putMetric(16);
    assert metrics.slowCounts() == 1;
    assert metrics.inSlowState() == false;
    metrics.clear();
    assert metrics.slowCounts() == 0;
  }
}
