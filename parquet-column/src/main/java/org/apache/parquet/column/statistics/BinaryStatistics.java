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
package org.apache.parquet.column.statistics;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class BinaryStatistics extends Statistics<Binary> {

  private Binary max;
  private Binary min;

  public BinaryStatistics() {
    // Creating a fake primitive type to have the proper comparator
    this(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(""));
  }

  BinaryStatistics(Type type) {
    super(type.<Binary>comparator());
  }

  @Override
  public void updateStats(Binary value) {
    if (!this.hasNonNullValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BinaryStatistics binaryStats = (BinaryStatistics)stats;
    if (!this.hasNonNullValue()) {
      initializeStats(binaryStats.getMin(), binaryStats.getMax());
    } else {
      updateStats(binaryStats.getMin(), binaryStats.getMax());
    }
  }

  /**
   * Sets min and max values, re-uses the byte[] passed in.
   * Any changes made to byte[] will be reflected in min and max values as well.
   * @param minBytes byte array to set the min value to
   * @param maxBytes byte array to set the max value to
   */
  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Binary.fromReusedByteArray(maxBytes);
    min = Binary.fromReusedByteArray(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return max == null ? null : max.getBytes();
  }

  @Override
  public byte[] getMinBytes() {
    return min == null ? null : min.getBytes();
  }

  @Override
  String toString(Binary value) {
    // TODO: have separate toString for different logical types?
    return value == null ? "null" : value.toStringUsingUTF8();
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || ((min.length() + max.length()) < size);
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void updateStats(Binary min_value, Binary max_value) {
    if (comparator().compare(min, min_value) > 0) { min = min_value.copy(); }
    if (comparator().compare(max, max_value) < 0) { max = max_value.copy(); }
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void initializeStats(Binary min_value, Binary max_value) {
      min = min_value.copy();
      max = max_value.copy();
      this.markAsNotEmpty();
  }

  @Override
  public Binary genericGetMin() {
    return min;
  }

  @Override
  public Binary genericGetMax() {
    return max;
  }

  /**
   * @deprecated use {@link #genericGetMax()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMax() {
    return max;
  }

  /**
   * @deprecated use {@link #genericGetMin()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMin() {
    return min;
  }

  /**
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void setMinMax(Binary min, Binary max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
