/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Implements a memory manager that keeps a global context of how many Parquet
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 *
 * This class balances the allocation size of each writer by resize them averagely.
 * When the sum of each writer's allocation size  is less than total memory pool,
 * keep their original value.
 * When the sum exceeds, decrease each writer's allocation size by a ratio.
 */
public class MemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
  static final float DEFAULT_MEMORY_POOL_RATIO = 0.95f;
  static final long DEFAULT_MIN_MEMORY_ALLOCATION = 1 * 1024 * 1024; // 1MB
  private final float memoryPoolRatio;

  private final long totalMemoryPool;
  private final long minMemoryAllocation;
  private final Map<InternalParquetRecordWriter<?>, Long> writerList =
      new HashMap<>();
  private final Map<String, Runnable> callBacks = new HashMap<String, Runnable>();
  private double scale = 1.0;

  public MemoryManager(float ratio, long minAllocation) {
    checkRatio(ratio);

    memoryPoolRatio = ratio;
    minMemoryAllocation = minAllocation;
    totalMemoryPool = Math.round((double) ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax
        () * ratio);
    LOG.debug("Allocated total memory pool is: {}", totalMemoryPool);
  }

  private void checkRatio(float ratio) {
    if (ratio <= 0 || ratio > 1) {
      throw new IllegalArgumentException("The configured memory pool ratio " + ratio + " is " +
          "not between 0 and 1.");
    }
  }

  /**
   * Add a new writer and its memory allocation to the memory manager.
   * @param writer the new created writer
   * @param allocation the requested buffer size
   */
  synchronized void addWriter(InternalParquetRecordWriter<?> writer, Long allocation) {
    Long oldValue = writerList.get(writer);
    if (oldValue == null) {
      writerList.put(writer, allocation);
    } else {
      throw new IllegalArgumentException("[BUG] The Parquet Memory Manager should not add an " +
          "instance of InternalParquetRecordWriter more than once. The Manager already contains " +
          "the writer: " + writer);
    }
    updateAllocation();
  }

  /**
   * Remove the given writer from the memory manager.
   * @param writer the writer that has been closed
   */
  synchronized void removeWriter(InternalParquetRecordWriter<?> writer) {
    writerList.remove(writer);
    if (!writerList.isEmpty()) {
      updateAllocation();
    }
  }

  /**
   * Update the allocated size of each writer based on the current allocations and pool size.
   */
  private void updateAllocation() {
    long totalAllocations = 0;
    for (Long allocation : writerList.values()) {
      totalAllocations += allocation;
    }
    if (totalAllocations <= totalMemoryPool) {
      scale = 1.0;
    } else {
      scale = (double) totalMemoryPool / totalAllocations;
      LOG.warn(String.format(
          "Total allocation exceeds %.2f%% (%,d bytes) of heap memory\n" +
          "Scaling row group sizes to %.2f%% for %d writers",
          100*memoryPoolRatio, totalMemoryPool, 100*scale, writerList.size()));
      for (Runnable callBack : callBacks.values()) {
        // we do not really want to start a new thread here.
        callBack.run();
      }
    }

    int maxColCount = 0;
    for (InternalParquetRecordWriter<?> w : writerList.keySet()) {
      maxColCount = Math.max(w.getSchema().getColumns().size(), maxColCount);
    }

    for (Map.Entry<InternalParquetRecordWriter<?>, Long> entry : writerList
        .entrySet()) {
      long newSize = (long) Math.floor(entry.getValue() * scale);
      if (scale < 1.0 && minMemoryAllocation > 0
          && newSize < minMemoryAllocation) {
        throw new ParquetMemoryManagerRuntimeException(String.format(
            "New Memory allocation %d bytes"
                + " is smaller than the minimum allocation size of %d bytes.",
            newSize, minMemoryAllocation));
      }
      entry.getKey().setRowGroupSizeThreshold(newSize);
      LOG.debug(String.format("Adjust block size from %,d to %,d for writer: %s",
            entry.getValue(), newSize, entry.getKey()));
    }
  }

  /**
   * Get the total memory pool size that is available for writers.
   * @return the number of bytes in the memory pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * Get the writers list
   * @return the writers in this memory manager
   */
  Map<InternalParquetRecordWriter<?>, Long> getWriterList() {
    return writerList;
  }

  /**
   * Get the ratio of memory allocated for all the writers.
   * @return the memory pool ratio
   */
  float getMemoryPoolRatio() {
    return memoryPoolRatio;
  }

  /**
   * Register callback and deduplicate it if any.
   * @param callBackName the name of callback. It should be identical.
   * @param callBack the callback passed in from upper layer, such as Hive.
   */
  public void registerScaleCallBack(String callBackName, Runnable callBack) {
    Objects.requireNonNull(callBackName, "callBackName cannot be null");
    Objects.requireNonNull(callBack, "callBack cannot be null");

    if (callBacks.containsKey(callBackName)) {
      throw new IllegalArgumentException("The callBackName " + callBackName +
          " is duplicated and has been registered already.");
    } else {
      callBacks.put(callBackName, callBack);
    }
  }

  /**
   * Get the registered callbacks.
   * @return
   */
  Map<String, Runnable> getScaleCallBacks() {
    return Collections.unmodifiableMap(callBacks);
  }

  /**
   * Get the internal scale value of MemoryManger
   * @return
   */
  double getScale() {
    return scale;
  }
}
