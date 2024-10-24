/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * A low memory footprint {@link GSet} implementation,
 * which uses an array for storing the elements
 * and linked lists for collision resolution.
 *
 * No rehash will be performed.
 * Therefore, the internal array will never be resized.
 *
 * This class does not support null element.
 *
 * This class is thread safe.
 *
 * @param <K> Key type for looking up the elements
 * @param <E> Element type, which must be
 *       (1) a subclass of K, and
 *       (2) implementing {@link LinkedElement} interface.
 */
public class ThreadSafeLightWeightGSet<K, E extends K> extends LightWeightGSet<K, E> {

  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(false);

  public ThreadSafeLightWeightGSet() {}
  
  public ThreadSafeLightWeightGSet(int recommendLength) {
    super(recommendLength);
  }

  public void readLock() {
    rwLock.readLock().lock();
  }

  public void readUnLock() {
    rwLock.readLock().unlock();
  }

  public void writeLock() {
    rwLock.writeLock().lock();
  }

  public void writeUnLock() {
    rwLock.writeLock().unlock();
  }
  
  public void iterateGsetForRead(Consumer<Iterator<E>> consumer) {
    readLock();
    try {
      consumer.accept(super.values().iterator());
    } finally {
      readUnLock();
    }
  }

  public void iterateGsetForUpdate(Consumer<Iterator<E>> consumer) {
    writeLock();
    try {
      consumer.accept(super.values().iterator());
    } finally {
      writeUnLock();
    }
  }

  @Override
  public E get(K key) {
    readLock();
    try {
      return super.get(key);
    } finally {
      readUnLock();
    }
  }

  @Override
  public E put(final E element) {
    writeLock();
    try {
      E existing = super.put(element);
      return existing;
    } finally {
      writeUnLock();
    }
  }

  @Override
  public E remove(K key) {
    writeLock();
    try {
      return super.remove(key);
    } finally {
      writeUnLock();
    }
  }

  @Override
  public int size() {
    readLock();
    try {
      return super.size();
    } finally {
      readUnLock();
    }
  }

}
