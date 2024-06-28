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

import java.util.Arrays;
import org.apache.parquet.column.UnknownColumnTypeException;
import org.apache.parquet.column.statistics.geometry.GeometryStatistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @param <T> the Java type described by this Statistics instance
 */
public abstract class Statistics<T extends Comparable<T>> {

  /**
   * Builder class to build Statistics objects. Used to read the statistics from the Parquet file.
   */
  public static class Builder {
    private final PrimitiveType type;
    private byte[] min;
    private byte[] max;
    private long numNulls = -1;

    private Builder(PrimitiveType type) {
      this.type = type;
    }

    public Builder withMin(byte[] min) {
      this.min = min;
      return this;
    }

    public Builder withMax(byte[] max) {
      this.max = max;
      return this;
    }

    public Builder withNumNulls(long numNulls) {
      this.numNulls = numNulls;
      return this;
    }

    public Builder withGeometryStatistics(GeometryStatistics geometryStatistics) {
      throw new UnsupportedOperationException("Please use the GeometryBuilder");
    }

    public Statistics<?> build() {
      Statistics<?> stats = createStats(type);
      if (min != null && max != null) {
        stats.setMinMaxFromBytes(min, max);
      }
      stats.num_nulls = this.numNulls;
      return stats;
    }
  }

  // Builder for FLOAT type to handle special cases of min/max values like NaN, -0.0, and 0.0
  private static class FloatBuilder extends Builder {
    public FloatBuilder(PrimitiveType type) {
      super(type);
      assert type.getPrimitiveTypeName() == PrimitiveTypeName.FLOAT;
    }

    @Override
    public Statistics<?> build() {
      FloatStatistics stats = (FloatStatistics) super.build();
      if (stats.hasNonNullValue()) {
        Float min = stats.genericGetMin();
        Float max = stats.genericGetMax();
        // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
        if (min.isNaN() || max.isNaN()) {
          stats.setMinMax(0.0f, 0.0f);
          ((Statistics<?>) stats).hasNonNullValue = false;
        } else {
          // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
          if (Float.compare(min, 0.0f) == 0) {
            min = -0.0f;
            stats.setMinMax(min, max);
          }
          if (Float.compare(max, -0.0f) == 0) {
            max = 0.0f;
            stats.setMinMax(min, max);
          }
        }
      }
      return stats;
    }
  }

  // Builder for DOUBLE type to handle special cases of min/max values like NaN, -0.0, and 0.0
  private static class DoubleBuilder extends Builder {
    public DoubleBuilder(PrimitiveType type) {
      super(type);
      assert type.getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE;
    }

    @Override
    public Statistics<?> build() {
      DoubleStatistics stats = (DoubleStatistics) super.build();
      if (stats.hasNonNullValue()) {
        Double min = stats.genericGetMin();
        Double max = stats.genericGetMax();
        // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
        if (min.isNaN() || max.isNaN()) {
          stats.setMinMax(0.0, 0.0);
          ((Statistics<?>) stats).hasNonNullValue = false;
        } else {
          // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
          if (Double.compare(min, 0.0) == 0) {
            min = -0.0;
            stats.setMinMax(min, max);
          }
          if (Double.compare(max, -0.0) == 0) {
            max = 0.0;
            stats.setMinMax(min, max);
          }
        }
      }
      return stats;
    }
  }

  // Builder for FLOAT16 type to handle special cases of min/max values like NaN, -0.0, and 0.0
  private static class Float16Builder extends Builder {
    private static final Binary POSITIVE_ZERO_LITTLE_ENDIAN = Binary.fromConstantByteArray(new byte[] {0x00, 0x00});
    private static final Binary NEGATIVE_ZERO_LITTLE_ENDIAN =
        Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x80});

    public Float16Builder(PrimitiveType type) {
      super(type);
      assert type.getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
      assert type.getTypeLength() == 2;
    }

    @Override
    public Statistics<?> build() {
      BinaryStatistics stats = (BinaryStatistics) super.build();
      if (stats.hasNonNullValue()) {
        Binary bMin = stats.genericGetMin();
        Binary bMax = stats.genericGetMax();
        short min = bMin.get2BytesLittleEndian();
        short max = bMax.get2BytesLittleEndian();
        // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
        if (Float16.isNaN(min) || Float16.isNaN(max)) {
          stats.setMinMax(POSITIVE_ZERO_LITTLE_ENDIAN, NEGATIVE_ZERO_LITTLE_ENDIAN);
          ((Statistics<?>) stats).hasNonNullValue = false;
        } else {
          // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
          if (min == (short) 0x0000) {
            stats.setMinMax(NEGATIVE_ZERO_LITTLE_ENDIAN, bMax);
          }
          if (max == (short) 0x8000) {
            stats.setMinMax(bMin, POSITIVE_ZERO_LITTLE_ENDIAN);
          }
        }
      }
      return stats;
    }
  }

  // Builder for GEOMETRY type to handle GeometryStatistics
  private static class GeometryBuilder extends Builder {

    private GeometryStatistics geometryStatistics;

    public GeometryBuilder(PrimitiveType type) {
      super(type);
      assert type.getPrimitiveTypeName() == PrimitiveTypeName.BINARY;
    }

    @Override
    public Builder withGeometryStatistics(GeometryStatistics geometryStatistics) {
      this.geometryStatistics = geometryStatistics;
      return this;
    }

    @Override
    public Statistics<?> build() {
      BinaryStatistics stats = (BinaryStatistics) super.build();
      stats.setGeometryStatistics(geometryStatistics);
      return stats;
    }
  }

  private final PrimitiveType type;
  private final PrimitiveComparator<T> comparator;
  private boolean hasNonNullValue;
  private long num_nulls;
  final PrimitiveStringifier stringifier;

  Statistics(PrimitiveType type) {
    this.type = type;
    this.comparator = type.comparator();
    this.stringifier = type.stringifier();
    hasNonNullValue = false;
    num_nulls = 0;
  }

  /**
   * Returns the typed statistics object based on the passed type parameter
   *
   * @param type PrimitiveTypeName type of the column
   * @return instance of a typed statistics class
   * @deprecated Use {@link #createStats(Type)} instead
   */
  @Deprecated
  public static Statistics getStatsBasedOnType(PrimitiveTypeName type) {
    switch (type) {
      case INT32:
        return new IntStatistics();
      case INT64:
        return new LongStatistics();
      case FLOAT:
        return new FloatStatistics();
      case DOUBLE:
        return new DoubleStatistics();
      case BOOLEAN:
        return new BooleanStatistics();
      case BINARY:
        return new BinaryStatistics();
      case INT96:
        return new BinaryStatistics();
      case FIXED_LEN_BYTE_ARRAY:
        return new BinaryStatistics();
      default:
        throw new UnknownColumnTypeException(type);
    }
  }

  /**
   * Creates an empty {@code Statistics} instance for the specified type to be
   * used for reading/writing the new min/max statistics used in the V2 format.
   *
   * @param type type of the column
   * @return instance of a typed statistics class
   */
  public static Statistics<?> createStats(Type type) {
    PrimitiveType primitive = type.asPrimitiveType();
    switch (primitive.getPrimitiveTypeName()) {
      case INT32:
        return new IntStatistics(primitive);
      case INT64:
        return new LongStatistics(primitive);
      case FLOAT:
        return new FloatStatistics(primitive);
      case DOUBLE:
        return new DoubleStatistics(primitive);
      case BOOLEAN:
        return new BooleanStatistics(primitive);
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return new BinaryStatistics(primitive);
      default:
        throw new UnknownColumnTypeException(primitive.getPrimitiveTypeName());
    }
  }

  /**
   * Returns a builder to create new statistics object. Used to read the statistics from the parquet file.
   *
   * @param type type of the column
   * @return builder to create new statistics object
   */
  public static Builder getBuilderForReading(PrimitiveType type) {
    switch (type.getPrimitiveTypeName()) {
      case FLOAT:
        return new FloatBuilder(type);
      case DOUBLE:
        return new DoubleBuilder(type);
      case FIXED_LEN_BYTE_ARRAY:
        LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
          return new Float16Builder(type);
        }
        return new Builder(type);
      case BINARY:
        if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) {
          return new GeometryBuilder(type);
        }
      default:
        return new Builder(type);
    }
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  /**
   * updates statistics min and max using the passed value
   *
   * @param value value to use to update min and max
   */
  public void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Equality comparison method to compare two statistics objects.
   *
   * @param other Object to compare against
   * @return true if objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Statistics)) return false;
    Statistics stats = (Statistics) other;
    return type.equals(stats.type)
        && Arrays.equals(stats.getMaxBytes(), this.getMaxBytes())
        && Arrays.equals(stats.getMinBytes(), this.getMinBytes())
        && stats.getNumNulls() == this.getNumNulls();
  }

  /**
   * Hash code for the statistics object
   *
   * @return hash code int
   */
  @Override
  public int hashCode() {
    return 31 * type.hashCode()
        + 31 * Arrays.hashCode(getMaxBytes())
        + 17 * Arrays.hashCode(getMinBytes())
        + Long.valueOf(this.getNumNulls()).hashCode();
  }

  /**
   * Method to merge this statistics object with the object passed
   * as parameter. Merging keeps the smallest of min values, largest of max
   * values and combines the number of null counts.
   *
   * @param stats Statistics object to merge with
   */
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    // Merge stats only if they have the same type
    if (type.equals(stats.type)) {
      incrementNumNulls(stats.getNumNulls());
      if (stats.hasNonNullValue()) {
        mergeStatisticsMinMax(stats);
        markAsNotEmpty();
      }
    } else {
      throw StatisticsClassException.create(this, stats);
    }
  }

  /**
   * Abstract method to merge this statistics min and max with the values
   * of the parameter object. Does not do any checks, only called internally.
   *
   * @param stats Statistics object to merge with
   */
  protected abstract void mergeStatisticsMinMax(Statistics stats);

  /**
   * Abstract method to set min and max values from byte arrays.
   *
   * @param minBytes byte array to set the min value to
   * @param maxBytes byte array to set the max value to
   * @deprecated will be removed in 2.0.0. Use {@link #getBuilderForReading(PrimitiveType)} instead.
   */
  @Deprecated
  public abstract void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  /**
   * Returns the min value in the statistics. The java natural order of the returned type defined by {@link
   * Comparable#compareTo(Object)} might not be the proper one. For example, UINT_32 requires unsigned comparison instead of the
   * natural signed one. Use {@link #compareMinToValue(Comparable)} or the comparator returned by {@link #comparator()} to
   * always get the proper ordering.
   *
   * @return the min value
   */
  public abstract T genericGetMin();

  /**
   * Returns the max value in the statistics. The java natural order of the returned type defined by {@link
   * Comparable#compareTo(Object)} might not be the proper one. For example, UINT_32 requires unsigned comparison instead of the
   * natural signed one. Use {@link #compareMaxToValue(Comparable)} or the comparator returned by {@link #comparator()} to
   * always get the proper ordering.
   *
   * @return the max value
   */
  public abstract T genericGetMax();

  /**
   * Returns the {@link PrimitiveComparator} implementation to be used to compare two generic values in the proper way
   * (for example, unsigned comparison for UINT_32).
   *
   * @return the comparator for data described by this Statistics instance
   */
  public final PrimitiveComparator<T> comparator() {
    return comparator;
  }

  /**
   * Compares min to the specified value in the proper way. It does the same as invoking
   * {@code comparator().compare(genericGetMin(), value)}. The corresponding statistics implementations overload this
   * method so the one with the primitive argument shall be used to avoid boxing/unboxing.
   *
   * @param value the value which {@code min} is to be compared to
   * @return a negative integer, zero, or a positive integer as {@code min} is less than, equal to, or greater than
   * {@code value}.
   */
  public final int compareMinToValue(T value) {
    return comparator.compare(genericGetMin(), value);
  }

  /**
   * Compares max to the specified value in the proper way. It does the same as invoking
   * {@code comparator().compare(genericGetMax(), value)}. The corresponding statistics implementations overload this
   * method so the one with the primitive argument shall be used to avoid boxing/unboxing.
   *
   * @param value the value which {@code max} is to be compared to
   * @return a negative integer, zero, or a positive integer as {@code max} is less than, equal to, or greater than
   * {@code value}.
   */
  public final int compareMaxToValue(T value) {
    return comparator.compare(genericGetMax(), value);
  }

  /**
   * Abstract method to return the max value as a byte array
   *
   * @return byte array corresponding to the max value
   */
  public abstract byte[] getMaxBytes();

  /**
   * Abstract method to return the min value as a byte array
   *
   * @return byte array corresponding to the min value
   */
  public abstract byte[] getMinBytes();

  /**
   * Returns the string representation of min for debugging/logging purposes.
   *
   * @return the min value as a string
   */
  public String minAsString() {
    return stringify(genericGetMin());
  }

  /**
   * Returns the string representation of max for debugging/logging purposes.
   *
   * @return the max value as a string
   */
  public String maxAsString() {
    return stringify(genericGetMax());
  }

  abstract String stringify(T value);

  /**
   * Abstract method to return whether the min and max values fit in the given
   * size.
   *
   * @param size a size in bytes
   * @return true iff the min and max values are less than size bytes
   */
  public abstract boolean isSmallerThan(long size);

  @Override
  public String toString() {
    if (this.hasNonNullValue()) {
      if (isNumNullsSet()) {
        return String.format(
            "min: %s, max: %s, num_nulls: %d", minAsString(), maxAsString(), this.getNumNulls());
      } else {
        return String.format("min: %s, max: %s, num_nulls not defined", minAsString(), maxAsString());
      }
    } else if (!this.isEmpty()) return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else return "no stats for this column";
  }

  /**
   * Increments the null count by one
   */
  public void incrementNumNulls() {
    num_nulls++;
  }

  /**
   * Increments the null count by the parameter value
   *
   * @param increment value to increment the null count by
   */
  public void incrementNumNulls(long increment) {
    num_nulls += increment;
  }

  /**
   * Returns the null count
   *
   * @return null count or {@code -1} if the null count is not set
   */
  public long getNumNulls() {
    return num_nulls;
  }

  /**
   * Sets the number of nulls to the parameter value
   *
   * @param nulls null count to set the count to
   * @deprecated will be removed in 2.0.0. Use {@link #getBuilderForReading(PrimitiveType)} instead.
   */
  @Deprecated
  public void setNumNulls(long nulls) {
    num_nulls = nulls;
  }

  /**
   * Returns a boolean specifying if the Statistics object is empty,
   * i.e does not contain valid statistics for the page/column yet
   *
   * @return true if object is empty, false otherwise
   */
  public boolean isEmpty() {
    return !hasNonNullValue && !isNumNullsSet();
  }

  /**
   * Returns whether there have been non-null values added to this statistics
   *
   * @return true if the values contained at least one non-null value
   */
  public boolean hasNonNullValue() {
    return hasNonNullValue;
  }

  /**
   * @return whether numNulls is set and can be used
   */
  public boolean isNumNullsSet() {
    return num_nulls >= 0;
  }

  /**
   * Sets the page/column as having a valid non-null value
   * kind of misnomer here
   */
  protected void markAsNotEmpty() {
    hasNonNullValue = true;
  }

  /**
   * @return a new independent statistics instance of this class.
   */
  public abstract Statistics<T> copy();

  /**
   * @return the primitive type object which this statistics is created for
   */
  public PrimitiveType type() {
    return type;
  }
}
