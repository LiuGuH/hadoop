package org.apache.hadoop.hdfs;

public interface SlowPipelineNodeStrategy {

  /**
   * check whether a SlowPipelineNodeStrategy object is slow or not.
   * @return true if SlowPipelineNodeStrategy object is slow.
   */
  boolean inSlowState();

  /**
   * put a metric value into SlowPipelineNodeStrategy object.
   * It will be stored in some data structures which subclass defines.
   * @param value metric value
   * @return
   */
  boolean putMetric(long value);

  /**
   * clear slow metrics data structure.
   */
  void clear();

  /**
   * get slow metrics avg value.
   * @return
   */
  double getAvg();
}
