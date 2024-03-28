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
package org.apache.parquet.arrow.schema;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaMapping.ListTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.PrimitiveTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.RepeatedTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.StructTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.UnionTypeMapping;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.GroupBuilder;

/**
 * Logic to convert Parquet and Arrow Schemas back and forth and maintain the mapping
 */
public class SchemaConverter {

  public static final String CONVERT_INT96_TO_ARROW_TIMESTAMP = "parquet.arrow.convertInt96AsTimestamp";

  private final Configuration configuration;

  /**
   * For when we'll need this to be configurable
   */
  public SchemaConverter() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(CONVERT_INT96_TO_ARROW_TIMESTAMP, false);
    this.configuration = conf;
  }

  public SchemaConverter(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Creates a Parquet Schema from an Arrow one and returns the mapping
   *
   * @param arrowSchema the provided Arrow Schema
   * @return the mapping between the 2
   */
  public SchemaMapping fromArrow(Schema arrowSchema) {
    List<Field> fields = arrowSchema.getFields();
    List<TypeMapping> parquetFields = fromArrow(fields);
    MessageType parquetType =
        addToBuilder(parquetFields, Types.buildMessage()).named("root");
    return new SchemaMapping(arrowSchema, parquetType, parquetFields);
  }

  private <T> GroupBuilder<T> addToBuilder(List<TypeMapping> parquetFields, GroupBuilder<T> builder) {
    for (TypeMapping type : parquetFields) {
      builder = builder.addField(type.getParquetType());
    }
    return builder;
  }

  private List<TypeMapping> fromArrow(List<Field> fields) {
    List<TypeMapping> result = new ArrayList<>(fields.size());
    for (Field field : fields) {
      result.add(fromArrow(field));
    }
    return result;
  }

  private TypeMapping fromArrow(final Field field) {
    return fromArrow(field, field.getName());
  }

  /**
   * @param field     arrow field
   * @param fieldName overrides field.getName()
   * @return mapping
   */
  private TypeMapping fromArrow(final Field field, final String fieldName) {
    final List<Field> children = field.getChildren();
    return field.getType().accept(new ArrowTypeVisitor<TypeMapping>() {

      @Override
      public TypeMapping visit(Null type) {
        // TODO(PARQUET-757): null original type
        return primitive(BINARY);
      }

      @Override
      public TypeMapping visit(Struct type) {
        List<TypeMapping> parquetTypes = fromArrow(children);
        return new StructTypeMapping(
            field,
            addToBuilder(parquetTypes, Types.buildGroup(OPTIONAL)).named(fieldName),
            parquetTypes);
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        return createListTypeMapping();
      }

      @Override
      public TypeMapping visit(ArrowType.LargeList largeList) {
        return createListTypeMapping();
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList type) {
        return createListTypeMapping();
      }

      private ListTypeMapping createListTypeMapping() {
        if (children.size() != 1) {
          throw new IllegalArgumentException("list fields must have exactly one child: " + field);
        }
        TypeMapping parquetChild = fromArrow(children.get(0), "element");
        GroupType list = Types.optionalList()
            .element(parquetChild.getParquetType())
            .named(fieldName);
        return new ListTypeMapping(field, new List3Levels(list), parquetChild);
      }

      @Override
      public TypeMapping visit(Union type) {
        // TODO(PARQUET-756): add Union OriginalType
        List<TypeMapping> parquetTypes = fromArrow(children);
        return new UnionTypeMapping(
            field,
            addToBuilder(parquetTypes, Types.buildGroup(OPTIONAL)).named(fieldName),
            parquetTypes);
      }

      @Override
      public TypeMapping visit(ArrowType.Map map) {
        if (children.size() != 2) {
          throw new IllegalArgumentException("Map fields must have exactly two children: " + field);
        }
        TypeMapping keyChild = fromArrow(children.get(0), "key");
        TypeMapping valueChild = fromArrow(children.get(1), "value");
        GroupType groupType = Types.optionalMap()
            .key(keyChild.getParquetType())
            .value(valueChild.getParquetType())
            .named(fieldName);
        return new SchemaMapping.MapTypeMapping(field, new Map3Levels(groupType), keyChild, valueChild);
      }

      @Override
      public TypeMapping visit(Int type) {
        boolean signed = type.getIsSigned();
        switch (type.getBitWidth()) {
          case 8:
          case 16:
          case 32:
            return primitive(INT32, intType(type.getBitWidth(), signed));
          case 64:
            return primitive(INT64, intType(64, signed));
          default:
            throw new IllegalArgumentException("Illegal int type: " + field);
        }
      }

      @Override
      public TypeMapping visit(FloatingPoint type) {
        switch (type.getPrecision()) {
          case HALF:
            // TODO(PARQUET-757): original type HalfFloat
            return primitive(FLOAT);
          case SINGLE:
            return primitive(FLOAT);
          case DOUBLE:
            return primitive(DOUBLE);
          default:
            throw new IllegalArgumentException("Illegal float type: " + field);
        }
      }

      @Override
      public TypeMapping visit(Utf8 type) {
        return primitive(BINARY, stringType());
      }

      @Override
      public TypeMapping visit(ArrowType.LargeUtf8 largeUtf8) {
        return primitive(BINARY, stringType());
      }

      @Override
      public TypeMapping visit(Binary type) {
        return primitive(BINARY);
      }

      @Override
      public TypeMapping visit(ArrowType.LargeBinary largeBinary) {
        return primitive(BINARY);
      }

      @Override
      public TypeMapping visit(Bool type) {
        return primitive(BOOLEAN);
      }

      /**
       * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
       * @param type an arrow decimal type
       * @return a mapping from the arrow decimal to the Parquet type
       */
      @Override
      public TypeMapping visit(Decimal type) {
        int precision = type.getPrecision();
        int scale = type.getScale();
        if (1 <= precision && precision <= 9) {
          return decimal(INT32, precision, scale);
        } else if (1 <= precision && precision <= 18) {
          return decimal(INT64, precision, scale);
        } else {
          // Better: FIXED_LENGTH_BYTE_ARRAY with length
          return decimal(BINARY, precision, scale);
        }
      }

      @Override
      public TypeMapping visit(Date type) {
        return primitive(INT32, dateType());
      }

      @Override
      public TypeMapping visit(Time type) {
        int bitWidth = type.getBitWidth();
        TimeUnit timeUnit = type.getUnit();
        if (bitWidth == 32 && timeUnit == TimeUnit.MILLISECOND) {
          return primitive(INT32, timeType(false, MILLIS));
        } else if (bitWidth == 64 && timeUnit == TimeUnit.MICROSECOND) {
          return primitive(INT64, timeType(false, MICROS));
        } else if (bitWidth == 64 && timeUnit == TimeUnit.NANOSECOND) {
          return primitive(INT64, timeType(false, NANOS));
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
      }

      @Override
      public TypeMapping visit(Timestamp type) {
        TimeUnit timeUnit = type.getUnit();
        if (timeUnit == TimeUnit.MILLISECOND) {
          return primitive(INT64, timestampType(isUtcNormalized(type), MILLIS));
        } else if (timeUnit == TimeUnit.MICROSECOND) {
          return primitive(INT64, timestampType(isUtcNormalized(type), MICROS));
        } else if (timeUnit == TimeUnit.NANOSECOND) {
          return primitive(INT64, timestampType(isUtcNormalized(type), NANOS));
        }
        throw new UnsupportedOperationException("Unsupported type " + type);
      }

      private boolean isUtcNormalized(Timestamp timestamp) {
        String timeZone = timestamp.getTimezone();
        return timeZone != null && !timeZone.isEmpty();
      }

      /**
       * See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval
       */
      @Override
      public TypeMapping visit(Interval type) {
        // TODO(PARQUET-675): fix interval original types
        return primitiveFLBA(12, LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance());
      }

      @Override
      public TypeMapping visit(ArrowType.Duration duration) {
        return primitiveFLBA(12, LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance());
      }

      @Override
      public TypeMapping visit(ArrowType.ExtensionType type) {
        return ArrowTypeVisitor.super.visit(type);
      }

      @Override
      public TypeMapping visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
        return primitive(BINARY);
      }

      private TypeMapping mapping(PrimitiveType parquetType) {
        return new PrimitiveTypeMapping(field, parquetType);
      }

      private TypeMapping decimal(PrimitiveTypeName type, int precision, int scale) {
        return mapping(
            Types.optional(type).as(decimalType(scale, precision)).named(fieldName));
      }

      private TypeMapping primitive(PrimitiveTypeName type) {
        return mapping(Types.optional(type).named(fieldName));
      }

      private TypeMapping primitive(PrimitiveTypeName type, LogicalTypeAnnotation otype) {
        return mapping(Types.optional(type).as(otype).named(fieldName));
      }

      private TypeMapping primitiveFLBA(int length, LogicalTypeAnnotation otype) {
        return mapping(Types.optional(FIXED_LEN_BYTE_ARRAY)
            .length(length)
            .as(otype)
            .named(fieldName));
      }
    });
  }

  /**
   * Creates an Arrow Schema from an Parquet one and returns the mapping
   *
   * @param parquetSchema the provided Parquet Schema
   * @return the mapping between the 2
   */
  public SchemaMapping fromParquet(MessageType parquetSchema) {
    List<Type> fields = parquetSchema.getFields();
    List<TypeMapping> mappings = fromParquet(fields);
    List<Field> arrowFields = fields(mappings);
    return new SchemaMapping(new Schema(arrowFields), parquetSchema, mappings);
  }

  private List<Field> fields(List<TypeMapping> mappings) {
    List<Field> result = new ArrayList<>(mappings.size());
    for (TypeMapping typeMapping : mappings) {
      result.add(typeMapping.getArrowField());
    }
    return result;
  }

  private List<TypeMapping> fromParquet(List<Type> fields) {
    List<TypeMapping> result = new ArrayList<>(fields.size());
    for (Type type : fields) {
      result.add(fromParquet(type));
    }
    return result;
  }

  private TypeMapping fromParquet(Type type) {
    return fromParquet(type, type.getName(), type.getRepetition());
  }

  /**
   * @param type       parquet type
   * @param name       overrides parquet.getName)
   * @param repetition overrides parquet.getRepetition()
   * @return a type mapping from the Parquet type to an Arrow type
   */
  private TypeMapping fromParquet(Type type, String name, Repetition repetition) {
    if (repetition == REPEATED) {
      // case where we have a repeated field that is not in a List/Map
      TypeMapping child = fromParquet(type, null, REQUIRED);
      Field arrowField = new Field(
          name,
          FieldType.notNullable(new ArrowType.List()),
          Collections.singletonList(child.getArrowField()));
      return new RepeatedTypeMapping(arrowField, type, child);
    }
    if (type.isPrimitive()) {
      return fromParquetPrimitive(type.asPrimitiveType(), name);
    } else {
      return fromParquetGroup(type.asGroupType(), name);
    }
  }

  /**
   * @param type parquet types
   * @param name overrides parquet.getName()
   * @return the mapping
   */
  private TypeMapping fromParquetGroup(GroupType type, String name) {
    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
    if (logicalType == null) {
      final FieldType field;
      if (type.isRepetition(OPTIONAL)) {
        field = FieldType.nullable(new Struct());
      } else {
        field = FieldType.notNullable(new Struct());
      }
      List<TypeMapping> typeMappings = fromParquet(type.getFields());
      Field arrowField = new Field(name, field, fields(typeMappings));
      return new StructTypeMapping(arrowField, type, typeMappings);
    } else {
      return logicalType
          .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<TypeMapping>() {
            @Override
            public Optional<TypeMapping> visit(
                LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
              List3Levels list3Levels = new List3Levels(type);
              TypeMapping child = fromParquet(
                  list3Levels.getElement(),
                  null,
                  list3Levels.getElement().getRepetition());
              Field arrowField = new Field(
                  name,
                  FieldType.nullable(new ArrowType.List()),
                  Collections.singletonList(child.getArrowField()));
              return of(new ListTypeMapping(arrowField, list3Levels, child));
            }

            @Override
            public Optional<TypeMapping> visit(
                LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
              Map3Levels map3levels = new Map3Levels(type);
              TypeMapping keyType = fromParquet(
                  map3levels.getKey(),
                  null,
                  map3levels.getKey().getRepetition());
              TypeMapping valueType = fromParquet(
                  map3levels.getValue(),
                  null,
                  map3levels.getValue().getRepetition());
              Field arrowField = new Field(
                  name,
                  FieldType.nullable(new ArrowType.Map(false)),
                  asList(keyType.getArrowField(), valueType.getArrowField()));
              return of(new SchemaMapping.MapTypeMapping(arrowField, map3levels, keyType, valueType));
            }
          })
          .orElseThrow(() -> new UnsupportedOperationException("Unsupported type " + type));
    }
  }

  /**
   * @param type parquet types
   * @param name overrides parquet.getName()
   * @return the mapping
   */
  private TypeMapping fromParquetPrimitive(final PrimitiveType type, final String name) {
    return type.getPrimitiveTypeName()
        .convert(new PrimitiveType.PrimitiveTypeNameConverter<TypeMapping, RuntimeException>() {

          private TypeMapping field(ArrowType arrowType) {
            final Field field;
            if (type.isRepetition(OPTIONAL)) {
              field = Field.nullable(name, arrowType);
            } else {
              field = Field.notNullable(name, arrowType);
            }
            return new PrimitiveTypeMapping(field, type);
          }

          @Override
          public TypeMapping convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            return field(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
          }

          @Override
          public TypeMapping convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            return field(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
          }

          @Override
          public TypeMapping convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
              return integer(32, true);
            }
            return logicalTypeAnnotation
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<TypeMapping>() {
                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                    return of(decimal(
                        decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
                    return of(field(new ArrowType.Date(DateUnit.DAY)));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                    return timeLogicalType.getUnit() == MILLIS
                        ? of(field(new ArrowType.Time(TimeUnit.MILLISECOND, 32)))
                        : empty();
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
                    if (intLogicalType.getBitWidth() == 64) {
                      return empty();
                    }
                    return of(integer(intLogicalType.getBitWidth(), intLogicalType.isSigned()));
                  }
                })
                .orElseThrow(() -> new IllegalArgumentException("illegal type " + type));
          }

          @Override
          public TypeMapping convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
              return integer(64, true);
            }

            return logicalTypeAnnotation
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<TypeMapping>() {
                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
                    return of(field(new ArrowType.Date(DateUnit.DAY)));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                    return of(decimal(
                        decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
                    return of(integer(intLogicalType.getBitWidth(), intLogicalType.isSigned()));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                    if (timeLogicalType.getUnit() == MICROS) {
                      return of(field(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
                    } else if (timeLogicalType.getUnit() == NANOS) {
                      return of(field(new ArrowType.Time(TimeUnit.NANOSECOND, 64)));
                    }
                    return empty();
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                    switch (timestampLogicalType.getUnit()) {
                      case MICROS:
                        return of(field(new ArrowType.Timestamp(
                            TimeUnit.MICROSECOND, getTimeZone(timestampLogicalType))));
                      case MILLIS:
                        return of(field(new ArrowType.Timestamp(
                            TimeUnit.MILLISECOND, getTimeZone(timestampLogicalType))));
                      case NANOS:
                        return of(field(new ArrowType.Timestamp(
                            TimeUnit.NANOSECOND, getTimeZone(timestampLogicalType))));
                    }
                    return empty();
                  }

                  private String getTimeZone(
                      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                    return timestampLogicalType.isAdjustedToUTC() ? "UTC" : null;
                  }
                })
                .orElseThrow(() -> new IllegalArgumentException("illegal type " + type));
          }

          @Override
          public TypeMapping convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            if (configuration.getBoolean(CONVERT_INT96_TO_ARROW_TIMESTAMP, false)) {
              return field(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
            } else {
              return field(new ArrowType.Binary());
            }
          }

          @Override
          public TypeMapping convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName)
              throws RuntimeException {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
              return field(new ArrowType.Binary());
            }

            return logicalTypeAnnotation
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<TypeMapping>() {
                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                    return of(decimal(
                        decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
                  }
                })
                .orElseThrow(() -> new IllegalArgumentException("illegal type " + type));
          }

          @Override
          public TypeMapping convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            return field(new ArrowType.Bool());
          }

          @Override
          public TypeMapping convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
              return field(new ArrowType.Binary());
            }
            return logicalTypeAnnotation
                .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<TypeMapping>() {
                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                    return of(field(new ArrowType.Utf8()));
                  }

                  @Override
                  public Optional<TypeMapping> visit(
                      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                    return of(decimal(
                        decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
                  }
                })
                .orElseThrow(() -> new IllegalArgumentException("illegal type " + type));
          }

          private TypeMapping decimal(int precision, int scale) {
            return field(new ArrowType.Decimal(precision, scale));
          }

          private TypeMapping integer(int width, boolean signed) {
            return field(new ArrowType.Int(width, signed));
          }
        });
  }

  /**
   * Maps a Parquet and Arrow Schema
   * For now does not validate primitive type compatibility
   *
   * @param arrowSchema   an Arrow schema
   * @param parquetSchema a Parquet message type
   * @return the mapping between the 2
   */
  public SchemaMapping map(Schema arrowSchema, MessageType parquetSchema) {
    List<TypeMapping> children = map(arrowSchema.getFields(), parquetSchema.getFields());
    return new SchemaMapping(arrowSchema, parquetSchema, children);
  }

  private List<TypeMapping> map(List<Field> arrowFields, List<Type> parquetFields) {
    if (arrowFields.size() != parquetFields.size()) {
      throw new IllegalArgumentException(
          "Can not map schemas as sizes differ: " + arrowFields + " != " + parquetFields);
    }
    List<TypeMapping> result = new ArrayList<>(arrowFields.size());
    for (int i = 0; i < arrowFields.size(); i++) {
      Field arrowField = arrowFields.get(i);
      Type parquetField = parquetFields.get(i);
      result.add(map(arrowField, parquetField));
    }
    return result;
  }

  private TypeMapping map(final Field arrowField, final Type parquetField) {
    return arrowField.getType().accept(new ArrowTypeVisitor<TypeMapping>() {

      @Override
      public TypeMapping visit(Null type) {
        if (!parquetField.isRepetition(OPTIONAL)) {
          throw new IllegalArgumentException("Parquet type can't be null: " + parquetField);
        }
        return primitive();
      }

      @Override
      public TypeMapping visit(Struct type) {
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        GroupType groupType = parquetField.asGroupType();
        return new StructTypeMapping(
            arrowField, groupType, map(arrowField.getChildren(), groupType.getFields()));
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        return createListTypeMapping(type);
      }

      @Override
      public TypeMapping visit(ArrowType.LargeList largeList) {
        return createListTypeMapping(largeList);
      }

      @Override
      public TypeMapping visit(org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList type) {
        return createListTypeMapping(type);
      }

      private TypeMapping createListTypeMapping(ArrowType.ComplexType type) {
        if (arrowField.getChildren().size() != 1) {
          throw new IllegalArgumentException("Invalid list type: " + type);
        }
        Field arrowChild = arrowField.getChildren().get(0);
        if (parquetField.isRepetition(REPEATED)) {
          return new RepeatedTypeMapping(arrowField, parquetField, map(arrowChild, parquetField));
        }
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        List3Levels list3Levels = new List3Levels(parquetField.asGroupType());
        if (arrowField.getChildren().size() != 1) {
          throw new IllegalArgumentException("invalid arrow list: " + arrowField);
        }
        return new ListTypeMapping(arrowField, list3Levels, map(arrowChild, list3Levels.getElement()));
      }

      @Override
      public TypeMapping visit(Union type) {
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        GroupType groupType = parquetField.asGroupType();
        return new UnionTypeMapping(
            arrowField, groupType, map(arrowField.getChildren(), groupType.getFields()));
      }

      @Override
      public TypeMapping visit(ArrowType.Map map) {
        if (arrowField.getChildren().size() != 2) {
          throw new IllegalArgumentException("Invalid map type: " + map);
        }
        if (parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Parquet type not a group: " + parquetField);
        }
        Map3Levels map3levels = new Map3Levels(parquetField.asGroupType());
        if (arrowField.getChildren().size() != 2) {
          throw new IllegalArgumentException("invalid arrow map: " + arrowField);
        }
        Field keyChild = arrowField.getChildren().get(0);
        Field valueChild = arrowField.getChildren().get(1);
        return new SchemaMapping.MapTypeMapping(
            arrowField,
            map3levels,
            map(keyChild, map3levels.getKey()),
            map(valueChild, map3levels.getValue()));
      }

      @Override
      public TypeMapping visit(Int type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(FloatingPoint type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Utf8 type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(ArrowType.LargeUtf8 largeUtf8) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Binary type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(ArrowType.LargeBinary largeBinary) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Bool type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Decimal type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Date type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Time type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Timestamp type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(Interval type) {
        return primitive();
      }

      @Override
      public TypeMapping visit(ArrowType.Duration duration) {
        return primitive();
      }

      @Override
      public TypeMapping visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
        return primitive();
      }

      private TypeMapping primitive() {
        if (!parquetField.isPrimitive()) {
          throw new IllegalArgumentException("Can not map schemas as one is primitive and the other is not: "
              + arrowField + " != " + parquetField);
        }
        return new PrimitiveTypeMapping(arrowField, parquetField.asPrimitiveType());
      }
    });
  }
}
