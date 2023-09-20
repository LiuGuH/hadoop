package org.apache.hadoop.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

public class ConcurrentLightWeightResizableGSet<K, E extends K>{

  private LightWeightResizableGSet<K, E> lightWeightResizableGSet = new LightWeightResizableGSet<>();

  public synchronized E put(final E element) {
    return lightWeightResizableGSet.put(element);
  }

  public synchronized E get(K key) {
    return lightWeightResizableGSet.get(key);
  }

  public synchronized E remove(K key) {
    return lightWeightResizableGSet.remove(key);
  }

  public synchronized int size() {
    return lightWeightResizableGSet.size();
  }

  public synchronized void getIterator(Consumer<Iterator<E>> consumer) {
    consumer.accept(lightWeightResizableGSet.values().iterator());
  }

  /**
   * Resize the internal table to given capacity.
   */
  @SuppressWarnings("unchecked")
  protected synchronized void resize(int cap) {
    lightWeightResizableGSet.resize(cap);
  }

  /**
   * Checks if we need to expand, and expands if necessary.
   */
  protected synchronized void expandIfNecessary() {
    lightWeightResizableGSet.expandIfNecessary();
  }

  /**
   * Not thread-safe!
   * This method must be only used by 
   * {@link org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.ReplicaMap#replicas(java.lang.String)}
   */
  public Collection<E> values() {
    return lightWeightResizableGSet.values();
  }

  public LightWeightResizableGSet<K, E> getLightWeightResizableGSet() {
    return lightWeightResizableGSet;
  } 

}
