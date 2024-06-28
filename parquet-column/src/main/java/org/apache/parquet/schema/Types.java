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

import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides fluent builders that produce Parquet schema Types.
 * <p>
 * The most basic use is to build primitive types:
 * <pre>
 *   Types.required(INT64).named("id");
 *   Types.optional(INT32).named("number");
 * </pre>
 * <p>
 * The required({@link PrimitiveTypeName}) factory method produces a primitive
 * type builder, and the {@link PrimitiveBuilder#named(String)} builds the
 * {@link PrimitiveType}. Between {@code required} and {@code named}, other
 * builder methods can be used to add type annotations or other type metadata:
 * <pre>
 *   Types.required(BINARY).as(UTF8).named("username");
 *   Types.optional(FIXED_LEN_BYTE_ARRAY).length(20).named("sha1");
 * </pre>
 * <p>
 * Optional types are built using {@link #optional(PrimitiveTypeName)} to get
 * the builder.
 * <p>
 * Groups are built similarly, using {@code requiredGroup()} (or the optional
 * version) to return a group builder. Group builders provide {@code required}
 * and {@code optional} to add primitive types, which return primitive builders
 * like the versions above.
 * <pre>
 *   // This produces:
 *   // required group User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   // }
 *   Types.requiredGroup()
 *            .required(INT64).named("id")
 *            .optional(BINARY).as(UTF8).named("email")
 *        .named("User")
 * </pre>
 * <p>
 * When {@code required} is called on a group builder, the builder it returns
 * will add the type to the parent group when it is built and {@code named} will
 * return its parent group builder (instead of the type) so more fields can be
 * added.
 * <p>
 * Sub-groups can be created using {@code requiredGroup()} to get a group
 * builder that will create the group type, add it to the parent builder, and
 * return the parent builder for more fields.
 * <pre>
 *   // required group User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   //   optional group address {
 *   //     required binary street (UTF8);
 *   //     required int32 zipcode;
 *   //   }
 *   // }
 *   Types.requiredGroup()
 *            .required(INT64).named("id")
 *            .optional(BINARY).as(UTF8).named("email")
 *            .optionalGroup()
 *                .required(BINARY).as(UTF8).named("street")
 *                .required(INT32).named("zipcode")
 *            .named("address")
 *        .named("User")
 * </pre>
 * <p>
 * Maps are built similarly, using {@code requiredMap()} (or the  {@link #optionalMap()}
 * version) to return a map builder. Map builders provide {@code key} to add
 * a primitive as key or a {@code groupKey} to add a group as key. {@code key()}
 * returns a MapKey builder, which extends a primitive builder. On the other hand,
 * {@code groupKey()} returns a MapGroupKey builder, which extends a group builder.
 * A key in a map is always required.
 * <p>
 * Once a key is built, a primitive map value can be built using {@code requiredValue()}
 * (or the optionalValue() version) that returns MapValue builder. A group map value
 * can be built using {@code requiredGroupValue()} (or the optionalGroupValue()
 * version) that returns MapGroupValue builder.
 *
 * <pre>
 *   // required group zipMap (MAP) {
 *   //   repeated group map (MAP_KEY_VALUE) {
 *   //     required float key
 *   //     optional int32 value
 *   //   }
 *   // }
 *   Types.requiredMap()
 *            .key(FLOAT)
 *            .optionalValue(INT32)
 *        .named("zipMap")
 *
 *
 *   // required group zipMap (MAP) {
 *   //   repeated group map (MAP_KEY_VALUE) {
 *   //     required group key {
 *   //       optional int64 first;
 *   //       required group second {
 *   //         required float inner_id_1;
 *   //         optional int32 inner_id_2;
 *   //       }
 *   //     }
 *   //     optional group value {
 *   //       optional group localGeoInfo {
 *   //         required float inner_value_1;
 *   //         optional int32 inner_value_2;
 *   //       }
 *   //       optional int32 zipcode;
 *   //     }
 *   //   }
 *   // }
 *   Types.requiredMap()
 *            .groupKey()
 *              .optional(INT64).named("id")
 *              .requiredGroup()
 *                .required(FLOAT).named("inner_id_1")
 *                .required(FLOAT).named("inner_id_2")
 *              .named("second")
 *            .optionalGroup()
 *              .optionalGroup()
 *                .required(FLOAT).named("inner_value_1")
 *                .optional(INT32).named("inner_value_2")
 *              .named("localGeoInfo")
 *              .optional(INT32).named("zipcode")
 *        .named("zipMap")
 * </pre>
 * <p>
 * Message types are built using {@link #buildMessage()} and function just like
 * group builders.
 * <pre>
 *   // message User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   //   optional group address {
 *   //     required binary street (UTF8);
 *   //     required int32 zipcode;
 *   //   }
 *   // }
 *   Types.buildMessage()
 *            .required(INT64).named("id")
 *            .optional(BINARY).as(UTF8).named("email")
 *            .optionalGroup()
 *                .required(BINARY).as(UTF8).named("street")
 *                .required(INT32).named("zipcode")
 *            .named("address")
 *        .named("User")
 * </pre>
 * <p>
 * These builders enforce consistency checks based on the specifications in
 * the parquet-format documentation. For example, if DECIMAL is used to annotate
 * a FIXED_LEN_BYTE_ARRAY that is not long enough for its maximum precision,
 * these builders will throw an IllegalArgumentException:
 * <pre>
 *   // throws IllegalArgumentException with message:
 *   // "FIXED(4) is not long enough to store 10 digits"
 *   Types.required(FIXED_LEN_BYTE_ARRAY).length(4)
 *        .as(DECIMAL).precision(10)
 *        .named("badDecimal");
 * </pre>
 */
public class Types {
  private static final int NOT_SET = 0;

  /**
   * A base builder for {@link Type} objects.
   *
   * @param <P> The type that this builder will return from
   *            {@link #named(String)} when the type is built.
   */
  public abstract static class Builder<THIS extends Builder, P> {
    protected final P parent;
    protected final Class<? extends P> returnClass;

    protected Type.Repetition repetition = null;
    protected LogicalTypeAnnotation logicalTypeAnnotation = null;
    protected Type.ID id = null;
    private boolean repetitionAlreadySet = false;

    /**
     * Construct a type builder that returns a "parent" object when the builder
     * is finished. The {@code parent} will be returned by
     * {@link #named(String)} so that builders can be chained.
     *
     * @param parent a non-null object to return from {@link #named(String)}
     */
    protected Builder(P parent) {
      this.parent = parent;
      this.returnClass = null;
    }

    /**
     * Construct a type builder that returns the {@link Type} that was built
     * when the builder is finished. The {@code returnClass} must be the
     * expected {@code Type} class.
     *
     * @param returnClass a {@code Type} to return from {@link #named(String)}
     */
    protected Builder(Class<P> returnClass) {
      Preconditions.checkArgument(
          Type.class.isAssignableFrom(returnClass), "The requested return class must extend Type");
      this.returnClass = returnClass;
      this.parent = null;
    }

    protected abstract THIS self();

    protected final THIS repetition(Type.Repetition repetition) {
      Preconditions.checkArgument(!repetitionAlreadySet, "Repetition has already been set");
      this.repetition = Objects.requireNonNull(repetition, "Repetition cannot be null");
      this.repetitionAlreadySet = true;
      return self();
    }

    /**
     * Adds a type annotation ({@link OriginalType}) to the type being built.
     * <p>
     * Type annotations are used to extend the types that parquet can store, by
     * specifying how the primitive types should be interpreted. This keeps the
     * set of primitive types to a minimum and reuses parquet's efficient
     * encodings. For example, strings are stored as byte arrays (binary) with
     * a UTF8 annotation.
     *
     * @param type an {@code OriginalType}
     * @return this builder for method chaining
     * @deprecated use {@link #as(LogicalTypeAnnotation)} with the corresponding logical type instead
     */
    @Deprecated
    public THIS as(OriginalType type) {
      this.logicalTypeAnnotation = LogicalTypeAnnotation.fromOriginalType(type, null);
      return self();
    }

    protected boolean newLogicalTypeSet;

    /**
     * Adds a type annotation ({@link LogicalTypeAnnotation}) to the type being built.
     * <p>
     * Type annotations are used to extend the types that parquet can store, by
     * specifying how the primitive types should be interpreted. This keeps the
     * set of primitive types to a minimum and reuses parquet's efficient
     * encodings. For example, strings are stored as byte arrays (binary) with
     * a UTF8 annotation.
     *
     * @param type an {@code {@link LogicalTypeAnnotation}}
     * @return this builder for method chaining
     */
    public THIS as(LogicalTypeAnnotation type) {
      this.logicalTypeAnnotation = type;
      this.newLogicalTypeSet = true;
      return self();
    }

    /**
     * adds an id annotation to the type being built.
     * <p>
     * ids are used to capture the original id when converting from models using ids (thrift, protobufs)
     *
     * @param id the id of the field
     * @return this builder for method chaining
     */
    public THIS id(int id) {
      this.id = new ID(id);
      return self();
    }

    protected abstract Type build(String name);

    /**
     * Builds a {@link Type} and returns the parent builder, if given, or the
     * {@code Type} that was built. If returning a parent object that is a
     * GroupBuilder, the constructed type will be added to it as a field.
     * <p>
     * <em>Note:</em> Any configuration for this type builder should be done
     * before calling this method.
     *
     * @param name a name for the constructed type
     * @return the parent {@code GroupBuilder} or the constructed {@code Type}
     */
    public P named(String name) {
      Objects.requireNonNull(name, "Name is required");
      Objects.requireNonNull(repetition, "Repetition is required");

      Type type = build(name);
      if (parent != null) {
        // if the parent is a BaseGroupBuilder, add type to it
        if (BaseGroupBuilder.class.isAssignableFrom(parent.getClass())) {
          BaseGroupBuilder.class.cast(parent).addField(type);
        }
        return parent;
      } else if (returnClass != null) {
        // no parent indicates that the Type object should be returned
        // the constructor check guarantees that returnClass is a Type
        return returnClass.cast(type);
      } else {
        throw new IllegalStateException("[BUG] Parent and return type are null: must override named");
      }
    }

    protected OriginalType getOriginalType() {
      return logicalTypeAnnotation == null ? null : logicalTypeAnnotation.toOriginalType();
    }
  }

  public abstract static class BasePrimitiveBuilder<P, THIS extends BasePrimitiveBuilder<P, THIS>>
      extends Builder<THIS, P> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasePrimitiveBuilder.class);
    private static final long MAX_PRECISION_INT32 = maxPrecision(4);
    private static final long MAX_PRECISION_INT64 = maxPrecision(8);
    private static final String LOGICAL_TYPES_DOC_URL =
        "https://github.com/apache/parquet-format/blob/master/LogicalTypes.md";
    private final PrimitiveTypeName primitiveType;
    private int length = NOT_SET;
    private int precision = NOT_SET;
    private int scale = NOT_SET;
    private ColumnOrder columnOrder;

    private BasePrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent);
      this.primitiveType = type;
    }

    private BasePrimitiveBuilder(Class<P> returnType, PrimitiveTypeName type) {
      super(returnType);
      this.primitiveType = type;
    }

    @Override
    protected abstract THIS self();

    /**
     * Adds the length for a FIXED_LEN_BYTE_ARRAY.
     *
     * @param length an int length
     * @return this builder for method chaining
     */
    public THIS length(int length) {
      this.length = length;
      return self();
    }

    private boolean precisionAlreadySet;
    private boolean scaleAlreadySet;

    /**
     * Adds the precision for a DECIMAL.
     * <p>
     * This value is required for decimals and must be less than or equal to
     * the maximum number of base-10 digits in the underlying type. A 4-byte
     * fixed, for example, can store up to 9 base-10 digits.
     *
     * @param precision an int precision value for the DECIMAL
     * @return this builder for method chaining
     * @deprecated use {@link #as(LogicalTypeAnnotation)} with the corresponding decimal type instead
     */
    @Deprecated
    public THIS precision(int precision) {
      this.precision = precision;
      precisionAlreadySet = true;
      return self();
    }

    /**
     * Adds the scale for a DECIMAL.
     * <p>
     * This value must be less than the maximum precision of the type and must
     * be a positive number. If not set, the default scale is 0.
     * <p>
     * The scale specifies the number of digits of the underlying unscaled
     * that are to the right of the decimal point. The decimal interpretation
     * of values in this column is: {@code value*10^(-scale)}.
     *
     * @param scale an int scale value for the DECIMAL
     * @return this builder for method chaining
     * @deprecated use {@link #as(LogicalTypeAnnotation)} with the corresponding decimal type instead
     */
    @Deprecated
    public THIS scale(int scale) {
      this.scale = scale;
      scaleAlreadySet = true;
      return self();
    }

    /**
     * Adds the column order for the primitive type.
     * <p>
     * In case of not set the default column order is {@link ColumnOrderName#TYPE_DEFINED_ORDER} except the type
     * {@link PrimitiveTypeName#INT96} and the types annotated by {@link OriginalType#INTERVAL} where the default column
     * order is {@link ColumnOrderName#UNDEFINED}.
     *
     * @param columnOrder the column order for the primitive type
     * @return this builder for method chaining
     */
    public THIS columnOrder(ColumnOrder columnOrder) {
      this.columnOrder = columnOrder;
      return self();
    }

    @Override
    protected PrimitiveType build(String name) {
      if (length == 0 && logicalTypeAnnotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
        length = LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.BYTES;
      }
      if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == primitiveType) {
        Preconditions.checkArgument(length > 0, "Invalid FIXED_LEN_BYTE_ARRAY length: %s", length);
      }

      DecimalMetadata meta = decimalMetadata();

      // validate type annotations and required metadata
      if (logicalTypeAnnotation != null) {
        logicalTypeAnnotation
            .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Boolean>() {
              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                return checkBinaryPrimitiveType(stringLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
                return checkBinaryPrimitiveType(jsonLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
                return checkBinaryPrimitiveType(bsonLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
                return checkFixedPrimitiveType(
                    LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.BYTES, uuidLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.Float16LogicalTypeAnnotation float16LogicalType) {
                return checkFixedPrimitiveType(
                    LogicalTypeAnnotation.Float16LogicalTypeAnnotation.BYTES, float16LogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                Preconditions.checkState(
                    (primitiveType == PrimitiveTypeName.INT32)
                        || (primitiveType == PrimitiveTypeName.INT64)
                        || (primitiveType == PrimitiveTypeName.BINARY)
                        || (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY),
                    "DECIMAL can only annotate INT32, INT64, BINARY, and FIXED");
                if (primitiveType == PrimitiveTypeName.INT32) {
                  Preconditions.checkState(
                      meta.getPrecision() <= MAX_PRECISION_INT32,
                      "INT32 cannot store %s digits (max %s)",
                      meta.getPrecision(),
                      MAX_PRECISION_INT32);
                } else if (primitiveType == PrimitiveTypeName.INT64) {
                  Preconditions.checkState(
                      meta.getPrecision() <= MAX_PRECISION_INT64,
                      "INT64 cannot store %s digits (max %s)",
                      meta.getPrecision(),
                      MAX_PRECISION_INT64);
                  if (meta.getPrecision() <= MAX_PRECISION_INT32) {
                    LOGGER.warn(
                        "Decimal with {} digits is stored in an INT64, but fits in an INT32. See {}.",
                        precision,
                        LOGICAL_TYPES_DOC_URL);
                  }
                } else if (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                  Preconditions.checkState(
                      meta.getPrecision() <= maxPrecision(length),
                      "FIXED(%s) cannot store %s digits (max %s)",
                      length,
                      meta.getPrecision(),
                      maxPrecision(length));
                }
                return Optional.of(true);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
                return checkInt32PrimitiveType(dateLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                LogicalTypeAnnotation.TimeUnit unit = timeLogicalType.getUnit();
                switch (unit) {
                  case MILLIS:
                    checkInt32PrimitiveType(timeLogicalType);
                    break;
                  case MICROS:
                  case NANOS:
                    checkInt64PrimitiveType(timeLogicalType);
                    break;
                  default:
                    throw new RuntimeException("Invalid time unit: " + unit);
                }
                return Optional.of(true);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
                int bitWidth = intLogicalType.getBitWidth();
                switch (bitWidth) {
                  case 8:
                  case 16:
                  case 32:
                    checkInt32PrimitiveType(intLogicalType);
                    break;
                  case 64:
                    checkInt64PrimitiveType(intLogicalType);
                    break;
                  default:
                    throw new RuntimeException("Invalid bit width: " + bitWidth);
                }
                return Optional.of(true);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                return checkInt64PrimitiveType(timestampLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
                return checkFixedPrimitiveType(12, intervalLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
                return checkBinaryPrimitiveType(enumLogicalType);
              }

              @Override
              public Optional<Boolean> visit(
                  LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryLogicalType) {
                if (geometryLogicalType.getEncoding() != LogicalTypeAnnotation.GeometryEncoding.WKB) {
                  throw new RuntimeException("Only WKB geometry encoding is supported for now");
                }
                return checkBinaryPrimitiveType(geometryLogicalType);
              }

              private Optional<Boolean> checkFixedPrimitiveType(
                  int l, LogicalTypeAnnotation logicalTypeAnnotation) {
                Preconditions.checkState(
                    primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY && length == l,
                    "%s can only annotate FIXED_LEN_BYTE_ARRAY(%s)",
                    logicalTypeAnnotation,
                    l);
                return Optional.of(true);
              }

              private Optional<Boolean> checkBinaryPrimitiveType(
                  LogicalTypeAnnotation logicalTypeAnnotation) {
                Preconditions.checkState(
                    primitiveType == PrimitiveTypeName.BINARY,
                    "%s can only annotate BINARY",
                    logicalTypeAnnotation);
                return Optional.of(true);
              }

              private Optional<Boolean> checkInt32PrimitiveType(
                  LogicalTypeAnnotation logicalTypeAnnotation) {
                Preconditions.checkState(
                    primitiveType == PrimitiveTypeName.INT32,
                    "%s can only annotate INT32",
                    logicalTypeAnnotation);
                return Optional.of(true);
              }

              private Optional<Boolean> checkInt64PrimitiveType(
                  LogicalTypeAnnotation logicalTypeAnnotation) {
                Preconditions.checkState(
                    primitiveType == PrimitiveTypeName.INT64,
                    "%s can only annotate INT64",
                    logicalTypeAnnotation);
                return Optional.of(true);
              }
            })
            .orElseThrow(() -> new IllegalStateException(
                logicalTypeAnnotation + " can not be applied to a primitive type"));
      }

      if (newLogicalTypeSet) {
        return new PrimitiveType(
            repetition, primitiveType, length, name, logicalTypeAnnotation, id, columnOrder);
      } else {
        return new PrimitiveType(
            repetition, primitiveType, length, name, getOriginalType(), meta, id, columnOrder);
      }
    }

    private static long maxPrecision(int numBytes) {
      return Math.round( // convert double to long
          Math.floor(
              Math.log10( // number of base-10 digits
                  Math.pow(2, 8 * numBytes - 1) - 1) // max value stored in numBytes
              ));
    }

    protected DecimalMetadata decimalMetadata() {
      DecimalMetadata meta = null;
      if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
        if (newLogicalTypeSet) {
          if (scaleAlreadySet) {
            Preconditions.checkArgument(
                this.scale == decimalType.getScale(),
                "Decimal scale should match with the scale of the logical type");
          }
          if (precisionAlreadySet) {
            Preconditions.checkArgument(
                this.precision == decimalType.getPrecision(),
                "Decimal precision should match with the precision of the logical type");
          }
          scale = decimalType.getScale();
          precision = decimalType.getPrecision();
        }
        Preconditions.checkArgument(precision > 0, "Invalid DECIMAL precision: %s", precision);
        Preconditions.checkArgument(this.scale >= 0, "Invalid DECIMAL scale: %s", this.scale);
        Preconditions.checkArgument(
            this.scale <= precision, "Invalid DECIMAL scale: cannot be greater than precision");
        meta = new DecimalMetadata(precision, scale);
      }
      return meta;
    }
  }

  /**
   * A builder for {@link PrimitiveType} objects.
   *
   * @param <P> The type that this builder will return from
   *            {@link #named(String)} when the type is built.
   */
  public static class PrimitiveBuilder<P> extends BasePrimitiveBuilder<P, PrimitiveBuilder<P>> {

    private PrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent, type);
    }

    private PrimitiveBuilder(Class<P> returnType, PrimitiveTypeName type) {
      super(returnType, type);
    }

    @Override
    protected PrimitiveBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseGroupBuilder<P, THIS extends BaseGroupBuilder<P, THIS>> extends Builder<THIS, P> {
    protected final List<Type> fields;

    private BaseGroupBuilder(P parent) {
      super(parent);
      this.fields = new ArrayList<>();
    }

    private BaseGroupBuilder(Class<P> returnType) {
      super(returnType);
      this.fields = new ArrayList<>();
    }

    @Override
    protected abstract THIS self();

    public PrimitiveBuilder<THIS> primitive(PrimitiveTypeName type, Type.Repetition repetition) {
      return new PrimitiveBuilder<>(self(), type).repetition(repetition);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the required primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     * builder for additional fields.
     */
    public PrimitiveBuilder<THIS> required(PrimitiveTypeName type) {
      return new PrimitiveBuilder<>(self(), type).repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the optional primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     * builder for additional fields.
     */
    public PrimitiveBuilder<THIS> optional(PrimitiveTypeName type) {
      return new PrimitiveBuilder<>(self(), type).repetition(Type.Repetition.OPTIONAL);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the repeated primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     * builder for additional fields.
     */
    public PrimitiveBuilder<THIS> repeated(PrimitiveTypeName type) {
      return new PrimitiveBuilder<>(self(), type).repetition(Type.Repetition.REPEATED);
    }

    public GroupBuilder<THIS> group(Type.Repetition repetition) {
      return new GroupBuilder<>(self()).repetition(repetition);
    }

    /**
     * Returns a {@link GroupBuilder} to build a required sub-group.
     *
     * @return a group builder that will return this builder for additional
     * fields.
     */
    public GroupBuilder<THIS> requiredGroup() {
      return new GroupBuilder<>(self()).repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Returns a {@link GroupBuilder} to build an optional sub-group.
     *
     * @return a group builder that will return this builder for additional
     * fields.
     */
    public GroupBuilder<THIS> optionalGroup() {
      return new GroupBuilder<>(self()).repetition(Type.Repetition.OPTIONAL);
    }

    /**
     * Returns a {@link GroupBuilder} to build a repeated sub-group.
     *
     * @return a group builder that will return this builder for additional
     * fields.
     */
    public GroupBuilder<THIS> repeatedGroup() {
      return new GroupBuilder<>(self()).repetition(Type.Repetition.REPEATED);
    }

    /**
     * Adds {@code type} as a sub-field to the group configured by this builder.
     *
     * @param type the type to add as a field
     * @return this builder for additional fields.
     */
    public THIS addField(Type type) {
      fields.add(type);
      return self();
    }

    /**
     * Adds {@code types} as sub-fields of the group configured by this builder.
     *
     * @param types an array of types to add as fields
     * @return this builder for additional fields.
     */
    public THIS addFields(Type... types) {
      Collections.addAll(fields, types);
      return self();
    }

    @Override
    protected GroupType build(String name) {
      if (newLogicalTypeSet) {
        return new GroupType(repetition, name, logicalTypeAnnotation, fields, id);
      } else {
        return new GroupType(repetition, name, getOriginalType(), fields, id);
      }
    }

    public MapBuilder<THIS> map(Type.Repetition repetition) {
      return new MapBuilder<>(self()).repetition(repetition);
    }

    public MapBuilder<THIS> requiredMap() {
      return new MapBuilder<>(self()).repetition(Type.Repetition.REQUIRED);
    }

    public MapBuilder<THIS> optionalMap() {
      return new MapBuilder<>(self()).repetition(Type.Repetition.OPTIONAL);
    }

    public ListBuilder<THIS> list(Type.Repetition repetition) {
      return new ListBuilder<>(self()).repetition(repetition);
    }

    public ListBuilder<THIS> requiredList() {
      return list(Type.Repetition.REQUIRED);
    }

    public ListBuilder<THIS> optionalList() {
      return list(Type.Repetition.OPTIONAL);
    }
  }

  /**
   * A builder for {@link GroupType} objects.
   *
   * @param <P> The type that this builder will return from
   *            {@link #named(String)} when the type is built.
   */
  public static class GroupBuilder<P> extends BaseGroupBuilder<P, GroupBuilder<P>> {

    private GroupBuilder(P parent) {
      super(parent);
    }

    private GroupBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected GroupBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseMapBuilder<P, THIS extends BaseMapBuilder<P, THIS>> extends Builder<THIS, P> {
    private static final Type STRING_KEY =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("key");

    public static class KeyBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BasePrimitiveBuilder<MP, KeyBuilder<MP, M>> {
      private final M mapBuilder;

      public KeyBuilder(M mapBuilder, PrimitiveTypeName type) {
        super(mapBuilder.parent, type);
        this.mapBuilder = mapBuilder;
        repetition(Type.Repetition.REQUIRED);
      }

      public ValueBuilder<MP, M> value(PrimitiveTypeName type, Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new ValueBuilder<>(mapBuilder, type).repetition(repetition);
      }

      public ValueBuilder<MP, M> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public ValueBuilder<MP, M> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public GroupValueBuilder<MP, M> groupValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new GroupValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public GroupValueBuilder<MP, M> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public GroupValueBuilder<MP, M> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public MapValueBuilder<MP, M> mapValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new MapValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public MapValueBuilder<MP, M> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public MapValueBuilder<MP, M> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public ListValueBuilder<MP, M> listValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new ListValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public ListValueBuilder<MP, M> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public ListValueBuilder<MP, M> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }

      public M value(Type type) {
        mapBuilder.setKeyType(build("key"));
        mapBuilder.setValueType(type);
        return this.mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setKeyType(build("key"));
        return mapBuilder.named(name);
      }

      @Override
      protected KeyBuilder<MP, M> self() {
        return this;
      }
    }

    public static class ValueBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BasePrimitiveBuilder<MP, ValueBuilder<MP, M>> {
      private final M mapBuilder;

      public ValueBuilder(M mapBuilder, PrimitiveTypeName type) {
        super(mapBuilder.parent, type);
        this.mapBuilder = mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setValueType(build("value"));
        return mapBuilder.named(name);
      }

      @Override
      protected ValueBuilder<MP, M> self() {
        return this;
      }
    }

    public static class GroupKeyBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BaseGroupBuilder<MP, GroupKeyBuilder<MP, M>> {
      private final M mapBuilder;

      public GroupKeyBuilder(M mapBuilder) {
        super(mapBuilder.parent);
        this.mapBuilder = mapBuilder;
        repetition(Type.Repetition.REQUIRED);
      }

      @Override
      protected GroupKeyBuilder<MP, M> self() {
        return this;
      }

      public ValueBuilder<MP, M> value(PrimitiveTypeName type, Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new ValueBuilder<>(mapBuilder, type).repetition(repetition);
      }

      public ValueBuilder<MP, M> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public ValueBuilder<MP, M> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public GroupValueBuilder<MP, M> groupValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new GroupValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public GroupValueBuilder<MP, M> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public GroupValueBuilder<MP, M> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public MapValueBuilder<MP, M> mapValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new MapValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public MapValueBuilder<MP, M> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public MapValueBuilder<MP, M> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public ListValueBuilder<MP, M> listValue(Type.Repetition repetition) {
        mapBuilder.setKeyType(build("key"));
        return new ListValueBuilder<>(mapBuilder).repetition(repetition);
      }

      public ListValueBuilder<MP, M> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public ListValueBuilder<MP, M> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }

      public M value(Type type) {
        mapBuilder.setKeyType(build("key"));
        mapBuilder.setValueType(type);
        return this.mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setKeyType(build("key"));
        return mapBuilder.named(name);
      }
    }

    public static class GroupValueBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BaseGroupBuilder<MP, GroupValueBuilder<MP, M>> {
      private final M mapBuilder;

      public GroupValueBuilder(M mapBuilder) {
        super(mapBuilder.parent);
        this.mapBuilder = mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setValueType(build("value"));
        return mapBuilder.named(name);
      }

      @Override
      protected GroupValueBuilder<MP, M> self() {
        return this;
      }
    }

    public static class MapValueBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BaseMapBuilder<MP, MapValueBuilder<MP, M>> {
      private final M mapBuilder;

      public MapValueBuilder(M mapBuilder) {
        super(mapBuilder.parent);
        this.mapBuilder = mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setValueType(build("value"));
        return mapBuilder.named(name);
      }

      @Override
      protected MapValueBuilder<MP, M> self() {
        return this;
      }
    }

    public static class ListValueBuilder<MP, M extends BaseMapBuilder<MP, M>>
        extends BaseListBuilder<MP, ListValueBuilder<MP, M>> {
      private final M mapBuilder;

      public ListValueBuilder(M mapBuilder) {
        super(mapBuilder.parent);
        this.mapBuilder = mapBuilder;
      }

      @Override
      public MP named(String name) {
        mapBuilder.setValueType(build("value"));
        return mapBuilder.named(name);
      }

      @Override
      protected ListValueBuilder<MP, M> self() {
        return this;
      }
    }

    protected void setKeyType(Type keyType) {
      Preconditions.checkState(this.keyType == null, "Only one key type can be built with a MapBuilder");
      this.keyType = keyType;
    }

    protected void setValueType(Type valueType) {
      Preconditions.checkState(this.valueType == null, "Only one key type can be built with a ValueBuilder");
      this.valueType = valueType;
    }

    private Type keyType = null;
    private Type valueType = null;

    public BaseMapBuilder(P parent) {
      super(parent);
    }

    private BaseMapBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected abstract THIS self();

    public KeyBuilder<P, THIS> key(PrimitiveTypeName type) {
      return new KeyBuilder<>(self(), type);
    }

    public THIS key(Type type) {
      setKeyType(type);
      return self();
    }

    public GroupKeyBuilder<P, THIS> groupKey() {
      return new GroupKeyBuilder<>(self());
    }

    public ValueBuilder<P, THIS> value(PrimitiveTypeName type, Type.Repetition repetition) {
      return new ValueBuilder<>(self(), type).repetition(repetition);
    }

    public ValueBuilder<P, THIS> requiredValue(PrimitiveTypeName type) {
      return value(type, Type.Repetition.REQUIRED);
    }

    public ValueBuilder<P, THIS> optionalValue(PrimitiveTypeName type) {
      return value(type, Type.Repetition.OPTIONAL);
    }

    public GroupValueBuilder<P, THIS> groupValue(Type.Repetition repetition) {
      return new GroupValueBuilder<>(self()).repetition(repetition);
    }

    public GroupValueBuilder<P, THIS> requiredGroupValue() {
      return groupValue(Type.Repetition.REQUIRED);
    }

    public GroupValueBuilder<P, THIS> optionalGroupValue() {
      return groupValue(Type.Repetition.OPTIONAL);
    }

    public MapValueBuilder<P, THIS> mapValue(Type.Repetition repetition) {
      return new MapValueBuilder<>(self()).repetition(repetition);
    }

    public MapValueBuilder<P, THIS> requiredMapValue() {
      return mapValue(Type.Repetition.REQUIRED);
    }

    public MapValueBuilder<P, THIS> optionalMapValue() {
      return mapValue(Type.Repetition.OPTIONAL);
    }

    public ListValueBuilder<P, THIS> listValue(Type.Repetition repetition) {
      return new ListValueBuilder<>(self()).repetition(repetition);
    }

    public ListValueBuilder<P, THIS> requiredListValue() {
      return listValue(Type.Repetition.REQUIRED);
    }

    public ListValueBuilder<P, THIS> optionalListValue() {
      return listValue(Type.Repetition.OPTIONAL);
    }

    public THIS value(Type type) {
      setValueType(type);
      return self();
    }

    @Override
    protected Type build(String name) {
      Preconditions.checkState(
          logicalTypeAnnotation == null, "MAP is already a logical type and can't be changed.");
      if (keyType == null) {
        keyType = STRING_KEY;
      }

      GroupBuilder<GroupType> builder = buildGroup(repetition).as(mapType());
      if (id != null) {
        builder.id(id.intValue());
      }

      if (valueType != null) {
        return builder.repeatedGroup()
            .addFields(keyType, valueType)
            .named(ConversionPatterns.MAP_REPEATED_NAME)
            .named(name);
      } else {
        return builder.repeatedGroup()
            .addFields(keyType)
            .named(ConversionPatterns.MAP_REPEATED_NAME)
            .named(name);
      }
    }
  }

  public static class MapBuilder<P> extends BaseMapBuilder<P, MapBuilder<P>> {
    public MapBuilder(P parent) {
      super(parent);
    }

    private MapBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected MapBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseListBuilder<P, THIS extends BaseListBuilder<P, THIS>> extends Builder<THIS, P> {
    private Type elementType = null;
    private P parent;

    public BaseListBuilder(P parent) {
      super(parent);
      this.parent = parent;
    }

    public BaseListBuilder(Class<P> returnType) {
      super(returnType);
    }

    public THIS setElementType(Type elementType) {
      Preconditions.checkState(this.elementType == null, "Only one element can be built with a ListBuilder");
      this.elementType = elementType;
      return self();
    }

    public static class ElementBuilder<LP, L extends BaseListBuilder<LP, L>>
        extends BasePrimitiveBuilder<LP, ElementBuilder<LP, L>> {
      private final BaseListBuilder<LP, L> listBuilder;

      public ElementBuilder(L listBuilder, PrimitiveTypeName type) {
        super(((BaseListBuilder<LP, L>) listBuilder).parent, type);
        this.listBuilder = listBuilder;
      }

      @Override
      public LP named(String name) {
        listBuilder.setElementType(build("element"));
        return listBuilder.named(name);
      }

      @Override
      protected ElementBuilder<LP, L> self() {
        return this;
      }
    }

    public static class GroupElementBuilder<LP, L extends BaseListBuilder<LP, L>>
        extends BaseGroupBuilder<LP, GroupElementBuilder<LP, L>> {
      private final L listBuilder;

      public GroupElementBuilder(L listBuilder) {
        super(((BaseListBuilder<LP, L>) listBuilder).parent);
        this.listBuilder = listBuilder;
      }

      @Override
      public LP named(String name) {
        listBuilder.setElementType(build("element"));
        return listBuilder.named(name);
      }

      @Override
      protected GroupElementBuilder<LP, L> self() {
        return this;
      }
    }

    public static class MapElementBuilder<LP, L extends BaseListBuilder<LP, L>>
        extends BaseMapBuilder<LP, MapElementBuilder<LP, L>> {

      private final L listBuilder;

      public MapElementBuilder(L listBuilder) {
        super(((BaseListBuilder<LP, L>) listBuilder).parent);
        this.listBuilder = listBuilder;
      }

      @Override
      protected MapElementBuilder<LP, L> self() {
        return this;
      }

      @Override
      public LP named(String name) {
        listBuilder.setElementType(build("element"));
        return listBuilder.named(name);
      }
    }

    public static class ListElementBuilder<LP, L extends BaseListBuilder<LP, L>>
        extends BaseListBuilder<LP, ListElementBuilder<LP, L>> {

      private final L listBuilder;

      public ListElementBuilder(L listBuilder) {
        super(((BaseListBuilder<LP, L>) listBuilder).parent);
        this.listBuilder = listBuilder;
      }

      @Override
      protected ListElementBuilder<LP, L> self() {
        return this;
      }

      @Override
      public LP named(String name) {
        listBuilder.setElementType(build("element"));
        return listBuilder.named(name);
      }
    }

    @Override
    protected abstract THIS self();

    @Override
    protected Type build(String name) {
      Preconditions.checkState(
          logicalTypeAnnotation == null, "LIST is already the logical type and can't be changed");
      Objects.requireNonNull(elementType, "List element type cannot be null");

      GroupBuilder<GroupType> builder = buildGroup(repetition).as(OriginalType.LIST);
      if (id != null) {
        builder.id(id.intValue());
      }

      return builder.repeatedGroup().addFields(elementType).named("list").named(name);
    }

    public ElementBuilder<P, THIS> element(PrimitiveTypeName type, Type.Repetition repetition) {
      return new ElementBuilder<>(self(), type).repetition(repetition);
    }

    public ElementBuilder<P, THIS> requiredElement(PrimitiveTypeName type) {
      return element(type, Type.Repetition.REQUIRED);
    }

    public ElementBuilder<P, THIS> optionalElement(PrimitiveTypeName type) {
      return element(type, Type.Repetition.OPTIONAL);
    }

    public GroupElementBuilder<P, THIS> groupElement(Type.Repetition repetition) {
      return new GroupElementBuilder<>(self()).repetition(repetition);
    }

    public GroupElementBuilder<P, THIS> requiredGroupElement() {
      return groupElement(Type.Repetition.REQUIRED);
    }

    public GroupElementBuilder<P, THIS> optionalGroupElement() {
      return groupElement(Type.Repetition.OPTIONAL);
    }

    public MapElementBuilder<P, THIS> mapElement(Type.Repetition repetition) {
      return new MapElementBuilder<>(self()).repetition(repetition);
    }

    public MapElementBuilder<P, THIS> requiredMapElement() {
      return mapElement(Type.Repetition.REQUIRED);
    }

    public MapElementBuilder<P, THIS> optionalMapElement() {
      return mapElement(Type.Repetition.OPTIONAL);
    }

    public ListElementBuilder<P, THIS> listElement(Type.Repetition repetition) {
      return new ListElementBuilder<>(self()).repetition(repetition);
    }

    public ListElementBuilder<P, THIS> requiredListElement() {
      return listElement(Type.Repetition.REQUIRED);
    }

    public ListElementBuilder<P, THIS> optionalListElement() {
      return listElement(Type.Repetition.OPTIONAL);
    }

    public BaseListBuilder<P, THIS> element(Type type) {
      setElementType(type);
      return self();
    }
  }

  public static class ListBuilder<P> extends BaseListBuilder<P, ListBuilder<P>> {
    public ListBuilder(P parent) {
      super(parent);
    }

    public ListBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected ListBuilder<P> self() {
      return this;
    }
  }

  public static class MessageTypeBuilder extends GroupBuilder<MessageType> {
    private MessageTypeBuilder() {
      super(MessageType.class);
      repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Builds and returns the {@link MessageType} configured by this builder.
     * <p>
     * <em>Note:</em> All primitive types and sub-groups should be added before
     * calling this method.
     *
     * @param name a name for the constructed type
     * @return the final {@code MessageType} configured by this builder.
     */
    @Override
    public MessageType named(String name) {
      Objects.requireNonNull(name, "Name is required");
      return new MessageType(name, fields);
    }
  }

  /**
   * Returns a builder to construct a {@link MessageType}.
   *
   * @return a {@link MessageTypeBuilder}
   */
  public static MessageTypeBuilder buildMessage() {
    return new MessageTypeBuilder();
  }

  public static PrimitiveBuilder<PrimitiveType> primitive(PrimitiveTypeName type, Type.Repetition repetition) {
    return new PrimitiveBuilder<>(PrimitiveType.class, type).repetition(repetition);
  }

  /**
   * Returns a builder to construct a required {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> required(PrimitiveTypeName type) {
    return new PrimitiveBuilder<>(PrimitiveType.class, type).repetition(Type.Repetition.REQUIRED);
  }

  /**
   * Returns a builder to construct an optional {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> optional(PrimitiveTypeName type) {
    return new PrimitiveBuilder<>(PrimitiveType.class, type).repetition(Type.Repetition.OPTIONAL);
  }

  /**
   * Returns a builder to construct a repeated {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> repeated(PrimitiveTypeName type) {
    return new PrimitiveBuilder<>(PrimitiveType.class, type).repetition(Type.Repetition.REPEATED);
  }

  public static GroupBuilder<GroupType> buildGroup(Type.Repetition repetition) {
    return new GroupBuilder<>(GroupType.class).repetition(repetition);
  }

  /**
   * Returns a builder to construct a required {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> requiredGroup() {
    return new GroupBuilder<>(GroupType.class).repetition(Type.Repetition.REQUIRED);
  }

  /**
   * Returns a builder to construct an optional {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> optionalGroup() {
    return new GroupBuilder<>(GroupType.class).repetition(Type.Repetition.OPTIONAL);
  }

  /**
   * Returns a builder to construct a repeated {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> repeatedGroup() {
    return new GroupBuilder<>(GroupType.class).repetition(Type.Repetition.REPEATED);
  }

  public static MapBuilder<GroupType> map(Type.Repetition repetition) {
    return new MapBuilder<>(GroupType.class).repetition(repetition);
  }

  public static MapBuilder<GroupType> requiredMap() {
    return map(Type.Repetition.REQUIRED);
  }

  public static MapBuilder<GroupType> optionalMap() {
    return map(Type.Repetition.OPTIONAL);
  }

  public static ListBuilder<GroupType> list(Type.Repetition repetition) {
    return new ListBuilder<>(GroupType.class).repetition(repetition);
  }

  public static ListBuilder<GroupType> requiredList() {
    return list(Type.Repetition.REQUIRED);
  }

  public static ListBuilder<GroupType> optionalList() {
    return list(Type.Repetition.OPTIONAL);
  }
}
