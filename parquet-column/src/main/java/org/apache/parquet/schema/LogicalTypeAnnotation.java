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
package org.apache.parquet.schema;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static org.apache.parquet.schema.ColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER;
import static org.apache.parquet.schema.ColumnOrder.ColumnOrderName.UNDEFINED;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MICROS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MICROS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MILLIS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_MILLIS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_NANOS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIMESTAMP_NANOS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_NANOS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_NANOS_UTC_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_UTC_STRINGIFIER;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.parquet.Preconditions;

public abstract class LogicalTypeAnnotation {
  enum LogicalTypeToken {
    MAP {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return mapType();
      }
    },
    LIST {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return listType();
      }
    },
    STRING {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return stringType();
      }
    },
    MAP_KEY_VALUE {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return MapKeyValueTypeAnnotation.getInstance();
      }
    },
    ENUM {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return enumType();
      }
    },
    DECIMAL {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        if (params.size() != 2) {
          throw new RuntimeException("Expecting 2 parameters for decimal logical type, got " + params.size());
        }
        return decimalType(Integer.parseInt(params.get(1)), Integer.parseInt(params.get(0)));
      }
    },
    DATE {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return dateType();
      }
    },
    TIME {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        if (params.size() != 2) {
          throw new RuntimeException("Expecting 2 parameters for time logical type, got " + params.size());
        }
        return timeType(Boolean.parseBoolean(params.get(1)), TimeUnit.valueOf(params.get(0)));
      }
    },
    TIMESTAMP {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        if (params.size() != 2) {
          throw new RuntimeException(
              "Expecting 2 parameters for timestamp logical type, got " + params.size());
        }
        return timestampType(Boolean.parseBoolean(params.get(1)), TimeUnit.valueOf(params.get(0)));
      }
    },
    INTEGER {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        if (params.size() != 2) {
          throw new RuntimeException("Expecting 2 parameters for integer logical type, got " + params.size());
        }
        return intType(Integer.parseInt(params.get(0)), Boolean.parseBoolean(params.get(1)));
      }
    },
    JSON {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return jsonType();
      }
    },
    BSON {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return bsonType();
      }
    },
    UUID {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return uuidType();
      }
    },
    INTERVAL {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return IntervalLogicalTypeAnnotation.getInstance();
      }
    },
    FLOAT16 {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        return float16Type();
      }
    },
    GEOMETRY {
      @Override
      protected LogicalTypeAnnotation fromString(List<String> params) {
        if (params.size() < 2) {
          throw new RuntimeException(
              "Expecting at least 2 parameters for geometry logical type, got " + params.size());
        }
        GeometryEncoding encoding = GeometryEncoding.valueOf(params.get(0));
        Edges edges = Edges.valueOf(params.get(1));
        String crs = params.size() > 2 ? params.get(2) : null;
        ByteBuffer metadata =
            params.size() > 3 ? ByteBuffer.wrap(params.get(3).getBytes()) : null;
        return geometryType(encoding, edges, crs, metadata);
      }
    };

    protected abstract LogicalTypeAnnotation fromString(List<String> params);
  }

  /**
   * Convert this logical type to old logical type representation in parquet-mr (if there's any).
   * Those logical type implementations, which don't have a corresponding mapping should return null.
   * <p>
   * API should be considered private
   *
   * @return the OriginalType representation of the new logical type, or null if there's none
   * @deprecated Please use the LogicalTypeAnnotation itself
   */
  @Deprecated
  public abstract OriginalType toOriginalType();

  /**
   * Visits this logical type with the given visitor
   *
   * @param logicalTypeAnnotationVisitor the visitor to visit this type
   */
  public abstract <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor);

  abstract LogicalTypeToken getType();

  String typeParametersAsString() {
    return "";
  }

  boolean isValidColumnOrder(ColumnOrder columnOrder) {
    return columnOrder.getColumnOrderName() == UNDEFINED || columnOrder.getColumnOrderName() == TYPE_DEFINED_ORDER;
  }

  @Override
  public String toString() {
    return getType() + typeParametersAsString();
  }

  PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
    throw new UnsupportedOperationException("Stringifier is not supported for the logical type: " + this);
  }

  /**
   * Helper method to convert the old representation of logical types (OriginalType) to new logical type.
   * <p>
   * API should be considered private
   */
  public static LogicalTypeAnnotation fromOriginalType(OriginalType originalType, DecimalMetadata decimalMetadata) {
    if (originalType == null) {
      return null;
    }
    switch (originalType) {
      case UTF8:
        return stringType();
      case MAP:
        return mapType();
      case DECIMAL:
        int scale = (decimalMetadata == null ? 0 : decimalMetadata.getScale());
        int precision = (decimalMetadata == null ? 0 : decimalMetadata.getPrecision());
        return decimalType(scale, precision);
      case LIST:
        return listType();
      case DATE:
        return dateType();
      case INTERVAL:
        return IntervalLogicalTypeAnnotation.getInstance();
      case TIMESTAMP_MILLIS:
        return timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case TIME_MILLIS:
        return timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIME_MICROS:
        return timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case UINT_8:
        return intType(8, false);
      case UINT_16:
        return intType(16, false);
      case UINT_32:
        return intType(32, false);
      case UINT_64:
        return intType(64, false);
      case INT_8:
        return intType(8, true);
      case INT_16:
        return intType(16, true);
      case INT_32:
        return intType(32, true);
      case INT_64:
        return intType(64, true);
      case ENUM:
        return enumType();
      case JSON:
        return jsonType();
      case BSON:
        return bsonType();
      case MAP_KEY_VALUE:
        return MapKeyValueTypeAnnotation.getInstance();
      default:
        throw new RuntimeException(
            "Can't convert original type to logical type, unknown original type " + originalType);
    }
  }

  public static StringLogicalTypeAnnotation stringType() {
    return StringLogicalTypeAnnotation.INSTANCE;
  }

  public static MapLogicalTypeAnnotation mapType() {
    return MapLogicalTypeAnnotation.INSTANCE;
  }

  public static ListLogicalTypeAnnotation listType() {
    return ListLogicalTypeAnnotation.INSTANCE;
  }

  public static EnumLogicalTypeAnnotation enumType() {
    return EnumLogicalTypeAnnotation.INSTANCE;
  }

  public static DecimalLogicalTypeAnnotation decimalType(final int scale, final int precision) {
    return new DecimalLogicalTypeAnnotation(scale, precision);
  }

  public static DateLogicalTypeAnnotation dateType() {
    return DateLogicalTypeAnnotation.INSTANCE;
  }

  public static TimeLogicalTypeAnnotation timeType(final boolean isAdjustedToUTC, final TimeUnit unit) {
    return new TimeLogicalTypeAnnotation(isAdjustedToUTC, unit);
  }

  public static TimestampLogicalTypeAnnotation timestampType(final boolean isAdjustedToUTC, final TimeUnit unit) {
    return new TimestampLogicalTypeAnnotation(isAdjustedToUTC, unit);
  }

  public static IntLogicalTypeAnnotation intType(final int bitWidth) {
    return new IntLogicalTypeAnnotation(bitWidth, true);
  }

  public static IntLogicalTypeAnnotation intType(final int bitWidth, final boolean isSigned) {
    Preconditions.checkArgument(
        bitWidth == 8 || bitWidth == 16 || bitWidth == 32 || bitWidth == 64,
        "Invalid bit width for integer logical type, %s is not allowed, "
            + "valid bit width values: 8, 16, 32, 64",
        bitWidth);
    return new IntLogicalTypeAnnotation(bitWidth, isSigned);
  }

  public static IntervalLogicalTypeAnnotation intervalType() {
    return new IntervalLogicalTypeAnnotation();
  }

  public static JsonLogicalTypeAnnotation jsonType() {
    return JsonLogicalTypeAnnotation.INSTANCE;
  }

  public static BsonLogicalTypeAnnotation bsonType() {
    return BsonLogicalTypeAnnotation.INSTANCE;
  }

  public static UUIDLogicalTypeAnnotation uuidType() {
    return UUIDLogicalTypeAnnotation.INSTANCE;
  }

  public static Float16LogicalTypeAnnotation float16Type() {
    return Float16LogicalTypeAnnotation.INSTANCE;
  }

  public static GeometryLogicalTypeAnnotation geometryType(
      GeometryEncoding encoding, Edges edges, String crs, ByteBuffer metadata) {
    return new GeometryLogicalTypeAnnotation(encoding, edges, crs, metadata);
  }

  public static class StringLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final StringLogicalTypeAnnotation INSTANCE = new StringLogicalTypeAnnotation();

    private StringLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.UTF8;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.STRING;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof StringLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.UTF8_STRINGIFIER;
    }
  }

  public static class MapLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final MapLogicalTypeAnnotation INSTANCE = new MapLogicalTypeAnnotation();

    private MapLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.MAP;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.MAP;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }
  }

  public static class ListLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final ListLogicalTypeAnnotation INSTANCE = new ListLogicalTypeAnnotation();

    private ListLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.LIST;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.LIST;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ListLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }
  }

  public static class EnumLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final EnumLogicalTypeAnnotation INSTANCE = new EnumLogicalTypeAnnotation();

    private EnumLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.ENUM;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.ENUM;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof EnumLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.UTF8_STRINGIFIER;
    }
  }

  public static class DecimalLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private final PrimitiveStringifier stringifier;
    private final int scale;
    private final int precision;

    private DecimalLogicalTypeAnnotation(int scale, int precision) {
      this.scale = scale;
      this.precision = precision;
      stringifier = PrimitiveStringifier.createDecimalStringifier(scale);
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.DECIMAL;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.DECIMAL;
    }

    @Override
    protected String typeParametersAsString() {
      return "(" + precision + "," + scale + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DecimalLogicalTypeAnnotation)) {
        return false;
      }
      DecimalLogicalTypeAnnotation other = (DecimalLogicalTypeAnnotation) obj;
      return scale == other.scale && precision == other.precision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(scale, precision);
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return stringifier;
    }
  }

  public static class DateLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final DateLogicalTypeAnnotation INSTANCE = new DateLogicalTypeAnnotation();

    private DateLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.DATE;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.DATE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof DateLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.DATE_STRINGIFIER;
    }
  }

  public enum TimeUnit {
    MILLIS,
    MICROS,
    NANOS
  }

  public static class TimeLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    private TimeLogicalTypeAnnotation(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIME_MILLIS;
        case MICROS:
          return OriginalType.TIME_MICROS;
        default:
          return null;
      }
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.TIME;
    }

    @Override
    protected String typeParametersAsString() {
      return "(" + unit + "," + isAdjustedToUTC + ")";
    }

    public TimeUnit getUnit() {
      return unit;
    }

    public boolean isAdjustedToUTC() {
      return isAdjustedToUTC;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimeLogicalTypeAnnotation)) {
        return false;
      }
      TimeLogicalTypeAnnotation other = (TimeLogicalTypeAnnotation) obj;
      return isAdjustedToUTC == other.isAdjustedToUTC && unit == other.unit;
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      switch (unit) {
        case MICROS:
        case MILLIS:
          return isAdjustedToUTC ? TIME_UTC_STRINGIFIER : TIME_STRINGIFIER;
        case NANOS:
          return isAdjustedToUTC ? TIME_NANOS_UTC_STRINGIFIER : TIME_NANOS_STRINGIFIER;
        default:
          return super.valueStringifier(primitiveType);
      }
    }
  }

  public static class TimestampLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private final boolean isAdjustedToUTC;
    private final TimeUnit unit;

    private TimestampLogicalTypeAnnotation(boolean isAdjustedToUTC, TimeUnit unit) {
      this.isAdjustedToUTC = isAdjustedToUTC;
      this.unit = unit;
    }

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      switch (unit) {
        case MILLIS:
          return OriginalType.TIMESTAMP_MILLIS;
        case MICROS:
          return OriginalType.TIMESTAMP_MICROS;
        default:
          return null;
      }
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.TIMESTAMP;
    }

    @Override
    protected String typeParametersAsString() {
      return "(" + unit + "," + isAdjustedToUTC + ")";
    }

    public TimeUnit getUnit() {
      return unit;
    }

    public boolean isAdjustedToUTC() {
      return isAdjustedToUTC;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TimestampLogicalTypeAnnotation)) {
        return false;
      }
      TimestampLogicalTypeAnnotation other = (TimestampLogicalTypeAnnotation) obj;
      return (isAdjustedToUTC == other.isAdjustedToUTC) && (unit == other.unit);
    }

    @Override
    public int hashCode() {
      return Objects.hash(isAdjustedToUTC, unit);
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      switch (unit) {
        case MICROS:
          return isAdjustedToUTC ? TIMESTAMP_MICROS_UTC_STRINGIFIER : TIMESTAMP_MICROS_STRINGIFIER;
        case MILLIS:
          return isAdjustedToUTC ? TIMESTAMP_MILLIS_UTC_STRINGIFIER : TIMESTAMP_MILLIS_STRINGIFIER;
        case NANOS:
          return isAdjustedToUTC ? TIMESTAMP_NANOS_UTC_STRINGIFIER : TIMESTAMP_NANOS_STRINGIFIER;
        default:
          return super.valueStringifier(primitiveType);
      }
    }
  }

  public static class IntLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final Set<Integer> VALID_BIT_WIDTH =
        Collections.unmodifiableSet(new HashSet<>(asList(8, 16, 32, 64)));

    private final int bitWidth;
    private final boolean isSigned;

    private IntLogicalTypeAnnotation(int bitWidth, boolean isSigned) {
      if (!VALID_BIT_WIDTH.contains(bitWidth)) {
        throw new IllegalArgumentException("Invalid integer bit width: " + bitWidth);
      }
      this.bitWidth = bitWidth;
      this.isSigned = isSigned;
    }

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      switch (bitWidth) {
        case 8:
          return isSigned ? OriginalType.INT_8 : OriginalType.UINT_8;
        case 16:
          return isSigned ? OriginalType.INT_16 : OriginalType.UINT_16;
        case 32:
          return isSigned ? OriginalType.INT_32 : OriginalType.UINT_32;
        case 64:
          return isSigned ? OriginalType.INT_64 : OriginalType.UINT_64;
        default:
          return null;
      }
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.INTEGER;
    }

    @Override
    protected String typeParametersAsString() {
      return "(" + bitWidth + "," + isSigned + ")";
    }

    public int getBitWidth() {
      return bitWidth;
    }

    public boolean isSigned() {
      return isSigned;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof IntLogicalTypeAnnotation)) {
        return false;
      }
      IntLogicalTypeAnnotation other = (IntLogicalTypeAnnotation) obj;
      return (bitWidth == other.bitWidth) && (isSigned == other.isSigned);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bitWidth, isSigned);
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return isSigned ? PrimitiveStringifier.DEFAULT_STRINGIFIER : PrimitiveStringifier.UNSIGNED_STRINGIFIER;
    }
  }

  public static class JsonLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final JsonLogicalTypeAnnotation INSTANCE = new JsonLogicalTypeAnnotation();

    private JsonLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.JSON;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.JSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof JsonLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.UTF8_STRINGIFIER;
    }
  }

  public static class BsonLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final BsonLogicalTypeAnnotation INSTANCE = new BsonLogicalTypeAnnotation();

    private BsonLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.BSON;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.BSON;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof BsonLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.DEFAULT_STRINGIFIER;
    }
  }

  public static class UUIDLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final UUIDLogicalTypeAnnotation INSTANCE = new UUIDLogicalTypeAnnotation();
    public static final int BYTES = 16;

    private UUIDLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      // No OriginalType for UUID
      return null;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.UUID;
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.UUID_STRINGIFIER;
    }
  }

  public static class Float16LogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static final Float16LogicalTypeAnnotation INSTANCE = new Float16LogicalTypeAnnotation();
    public static final int BYTES = 2;

    private Float16LogicalTypeAnnotation() {}

    @Override
    public OriginalType toOriginalType() {
      // No OriginalType for Float16
      return null;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.FLOAT16;
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.FLOAT16_STRINGIFIER;
    }
  }

  // This logical type annotation is implemented to support backward compatibility with ConvertedType.
  // The new logical type representation in parquet-format doesn't have any interval type,
  // thus this annotation is mapped to UNKNOWN.
  public static class IntervalLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private static IntervalLogicalTypeAnnotation INSTANCE = new IntervalLogicalTypeAnnotation();

    public static LogicalTypeAnnotation getInstance() {
      return INSTANCE;
    }

    private IntervalLogicalTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.INTERVAL;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.INTERVAL;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof IntervalLogicalTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      return PrimitiveStringifier.INTERVAL_STRINGIFIER;
    }

    @Override
    boolean isValidColumnOrder(ColumnOrder columnOrder) {
      return columnOrder.getColumnOrderName() == UNDEFINED;
    }
  }

  // This logical type annotation is implemented to support backward compatibility with ConvertedType.
  // The new logical type representation in parquet-format doesn't have any key-value type,
  // thus this annotation is mapped to UNKNOWN. This type shouldn't be used.
  public static class MapKeyValueTypeAnnotation extends LogicalTypeAnnotation {
    private static MapKeyValueTypeAnnotation INSTANCE = new MapKeyValueTypeAnnotation();

    public static MapKeyValueTypeAnnotation getInstance() {
      return INSTANCE;
    }

    private MapKeyValueTypeAnnotation() {}

    /**
     * API Should be considered private
     *
     * @return the original type
     * @deprecated Please use the LogicalTypeAnnotation itself
     */
    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return OriginalType.MAP_KEY_VALUE;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.MAP_KEY_VALUE;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof MapKeyValueTypeAnnotation;
    }

    @Override
    public int hashCode() {
      // This type doesn't have any parameters, thus using class hashcode
      return getClass().hashCode();
    }
  }

  public enum GeometryEncoding {
    WKB
  }

  public enum Edges {
    PLANAR,
    SPHERICAL
  }

  public static class GeometryLogicalTypeAnnotation extends LogicalTypeAnnotation {
    private final GeometryEncoding encoding;
    private final Edges edges;
    private final String crs;
    private final ByteBuffer metadata;

    private GeometryLogicalTypeAnnotation(GeometryEncoding encoding, Edges edges, String crs, ByteBuffer metadata) {
      Preconditions.checkArgument(encoding != null, "Geometry encoding is required");
      Preconditions.checkArgument(edges != null, "Geometry edges is required");
      this.encoding = encoding;
      this.edges = edges;
      this.crs = crs;
      this.metadata = metadata;
    }

    @Override
    @Deprecated
    public OriginalType toOriginalType() {
      return null;
    }

    @Override
    public <T> Optional<T> accept(LogicalTypeAnnotationVisitor<T> logicalTypeAnnotationVisitor) {
      return logicalTypeAnnotationVisitor.visit(this);
    }

    @Override
    LogicalTypeToken getType() {
      return LogicalTypeToken.GEOMETRY;
    }

    @Override
    protected String typeParametersAsString() {
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(encoding);
      sb.append(",");
      sb.append(edges);
      if (crs != null && !crs.isEmpty()) {
        sb.append(",");
        sb.append(crs);
      }
      if (metadata != null) {
        sb.append(",");
        sb.append(metadata);
      }
      sb.append(")");
      return sb.toString();
    }

    public GeometryEncoding getEncoding() {
      return encoding;
    }

    public Edges getEdges() {
      return edges;
    }

    public String getCrs() {
      return crs;
    }

    public ByteBuffer getMetadata() {
      return metadata;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof GeometryLogicalTypeAnnotation)) {
        return false;
      }
      GeometryLogicalTypeAnnotation other = (GeometryLogicalTypeAnnotation) obj;
      return (encoding == other.encoding) && (edges == other.edges) && crs.equals(other.crs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(encoding, crs, edges);
    }

    @Override
    PrimitiveStringifier valueStringifier(PrimitiveType primitiveType) {
      if (encoding == GeometryEncoding.WKB) {
        return PrimitiveStringifier.WKB_STRINGIFIER;
      }
      return super.valueStringifier(primitiveType);
    }
  }

  /**
   * Implement this interface to visit a logical type annotation in the schema.
   * The default implementation for each logical type specific visitor method is empty.
   * <p>
   * Example usage: logicalTypeAnnotation.accept(new LogicalTypeAnnotationVisitor() { ... });
   * <p>
   * Every visit method returns {@link Optional#empty()} by default.
   * It means that for the given logical type no specific action is needed.
   * Client code can use {@link Optional#orElse(Object)} to return a default value for unhandled types,
   * or {@link Optional#orElseThrow(Supplier)} to throw exception if omitting a type is not allowed.
   */
  public interface LogicalTypeAnnotationVisitor<T> {
    default Optional<T> visit(StringLogicalTypeAnnotation stringLogicalType) {
      return empty();
    }

    default Optional<T> visit(MapLogicalTypeAnnotation mapLogicalType) {
      return empty();
    }

    default Optional<T> visit(ListLogicalTypeAnnotation listLogicalType) {
      return empty();
    }

    default Optional<T> visit(EnumLogicalTypeAnnotation enumLogicalType) {
      return empty();
    }

    default Optional<T> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
      return empty();
    }

    default Optional<T> visit(DateLogicalTypeAnnotation dateLogicalType) {
      return empty();
    }

    default Optional<T> visit(TimeLogicalTypeAnnotation timeLogicalType) {
      return empty();
    }

    default Optional<T> visit(TimestampLogicalTypeAnnotation timestampLogicalType) {
      return empty();
    }

    default Optional<T> visit(IntLogicalTypeAnnotation intLogicalType) {
      return empty();
    }

    default Optional<T> visit(JsonLogicalTypeAnnotation jsonLogicalType) {
      return empty();
    }

    default Optional<T> visit(BsonLogicalTypeAnnotation bsonLogicalType) {
      return empty();
    }

    default Optional<T> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
      return empty();
    }

    default Optional<T> visit(IntervalLogicalTypeAnnotation intervalLogicalType) {
      return empty();
    }

    default Optional<T> visit(MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
      return empty();
    }

    default Optional<T> visit(Float16LogicalTypeAnnotation float16LogicalType) {
      return empty();
    }

    default Optional<T> visit(GeometryLogicalTypeAnnotation geometryLogicalType) {
      return empty();
    }
  }
}
