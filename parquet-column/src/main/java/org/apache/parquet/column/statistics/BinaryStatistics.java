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

import org.apache.parquet.column.statistics.geometry.GeometryStatistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class BinaryStatistics extends Statistics<Binary> {

  // A fake type object to be used to generate the proper comparator
  private static final PrimitiveType DEFAULT_FAKE_TYPE =
      Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("fake_binary_type");

  private Binary max;
  private Binary min;
  private GeometryStatistics geometryStatistics = null;

  /**
   * @deprecated will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead
   */
  @Deprecated
  public BinaryStatistics() {
    this(DEFAULT_FAKE_TYPE);
  }

  BinaryStatistics(PrimitiveType type) {
    super(type);
    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
    if (logicalType instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) {
      geometryStatistics = new GeometryStatistics();
    }
  }

  private BinaryStatistics(BinaryStatistics other) {
    super(other.type());
    if (other.hasNonNullValue()) {
      initializeStats(other.min, other.max);
    }
    setNumNulls(other.getNumNulls());
    if (other.geometryStatistics != null) {
      geometryStatistics = other.geometryStatistics.copy();
    }
  }

  @Override
  public void updateStats(Binary value) {
    if (!this.hasNonNullValue()) {
      min = value.copy();
      max = value.copy();
      this.markAsNotEmpty();
    } else if (comparator().compare(min, value) > 0) {
      min = value.copy();
    } else if (comparator().compare(max, value) < 0) {
      max = value.copy();
    }
    if (geometryStatistics != null) {
      geometryStatistics.update(value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    BinaryStatistics binaryStats = (BinaryStatistics) stats;
    if (!this.hasNonNullValue()) {
      initializeStats(binaryStats.getMin(), binaryStats.getMax());
    } else {
      updateStats(binaryStats.getMin(), binaryStats.getMax());
    }
    if (geometryStatistics != null) {
      geometryStatistics.merge(binaryStats.geometryStatistics);
    }
  }

  /**
   * Sets min and max values, re-uses the byte[] passed in.
   * Any changes made to byte[] will be reflected in min and max values as well.
   *
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
  String stringify(Binary value) {
    return stringifier.stringify(value);
  }

  @Override
  public boolean isSmallerThan(long size) {
    return !hasNonNullValue() || ((min.length() + max.length()) < size);
  }

  public boolean isSmallerThanWithTruncation(long size, int truncationLength) {
    if (!hasNonNullValue()) {
      return true;
    }

    int minTruncateLength = Math.min(min.length(), truncationLength);
    int maxTruncateLength = Math.min(max.length(), truncationLength);

    return minTruncateLength + maxTruncateLength < size;
  }

  /**
   * @param min_value a min binary
   * @param max_value a max binary
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void updateStats(Binary min_value, Binary max_value) {
    if (comparator().compare(min, min_value) > 0) {
      min = min_value.copy();
    }
    if (comparator().compare(max, max_value) < 0) {
      max = max_value.copy();
    }
  }

  /**
   * @param min_value a min binary
   * @param max_value a max binary
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
   * @return the max binary
   * @deprecated use {@link #genericGetMax()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMax() {
    return max;
  }

  /**
   * @return the min binary
   * @deprecated use {@link #genericGetMin()}, will be removed in 2.0.0
   */
  @Deprecated
  public Binary getMin() {
    return min;
  }

  /**
   * @param min a min binary
   * @param max a max binary
   * @deprecated use {@link #updateStats(Binary)}, will be removed in 2.0.0
   */
  @Deprecated
  public void setMinMax(Binary min, Binary max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }

  @Override
  public BinaryStatistics copy() {
    return new BinaryStatistics(this);
  }

  public void setGeometryStatistics(GeometryStatistics geometryStatistics) {
    this.geometryStatistics = geometryStatistics;
  }

  public GeometryStatistics getGeometryStatistics() {
    return geometryStatistics;
  }
}
