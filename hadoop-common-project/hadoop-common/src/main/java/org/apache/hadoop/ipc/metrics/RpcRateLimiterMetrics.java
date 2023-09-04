package org.apache.hadoop.ipc.metrics;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(about = "rpc RateLimter metrics", context = "ratelimter")
public class RpcRateLimiterMetrics {
  final MetricsRegistry registry;
  final String name;
  @Metric("rpcRatelimit time")
  MutableRate rpcRateLimit;
  @Metric("Number of rpcRatelimit suppressed Num")
  MutableCounterLong rpcRateLimitSuppressedNum;
  @Metric("Number of rpcRatelimit refused Num")
  MutableCounterLong rpcRateLimitRefusedNum;
  @Metric("rpcRateLimitMismatch time")
  MutableRate rpcRateLimitMismatch;
  @Metric("rpcLimitConditionReadLock time")
  MutableRate rpcLimitConditionReadLock;
  @Metric("rpcLimitConditionWriteLock time")
  MutableRate rpcLimitConditionWriteLock;
  @Metric("rpcRateLimitTryAcquire time")
  MutableRate rpcRateLimitTryAcquire;

  @Metric("Number of rpcRateLimit fetch successes")
  MutableCounterLong rpcRateLimitFetchSuccesses;
  @Metric("Number of rpcRateLimit fetch failures")
  MutableCounterLong rpcRateLimitFetchFailures;
  @Metric("Number of rpcRateLimit parsingFormat successes")
  MutableCounterLong rpcRateLimitParsingFormatSuccesses;
  @Metric("Number of rpcRateLimit parsingFormat failures")
  MutableCounterLong rpcRateLimitParsingFormatFailures;

  public RpcRateLimiterMetrics() {
    registry = new MetricsRegistry("ratelimter");
    name = "ratelimter";
  }

  public static RpcRateLimiterMetrics create() {
    RpcRateLimiterMetrics m = new RpcRateLimiterMetrics();
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  public String name() {
    return name;
  }

  public void addRpcRateLimit(long time) {
    rpcRateLimit.add(time);
  }

  public void incrRpcRateLimitSuppressedNum() {
    rpcRateLimitSuppressedNum.incr();
  }

  public void incrRpcRateLimitRefusedNum() {
    rpcRateLimitRefusedNum.incr();
  }

  public void addRpcLimitConditionReadLock(long time) {
    rpcLimitConditionReadLock.add(time);
  }

  public void addRpcLimitConditionWriteLock(long time) {
    rpcLimitConditionWriteLock.add(time);
  }

  public void addRpcRateLimitTryAcquire(long time) {
    rpcRateLimitTryAcquire.add(time);
  }

  public void addRpcRateLimitMismatch(long time) {
    rpcRateLimitMismatch.add(time);
  }

  public void addRpcRateLimitFetchSuccesses() {
    rpcRateLimitFetchSuccesses.incr();
  }

  public void addRpcRateLimitFetchFailures() {
    rpcRateLimitFetchFailures.incr();
  }

  public void addRpcRateLimitParsingFormatSuccesses() {
    rpcRateLimitParsingFormatSuccesses.incr();
  }

  public void addRpcRateLimitParsingFormatFailures() {
    rpcRateLimitParsingFormatFailures.incr();
  }

}
