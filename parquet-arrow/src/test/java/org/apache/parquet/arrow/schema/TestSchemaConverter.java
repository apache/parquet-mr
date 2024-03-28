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
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.arrow.schema.SchemaMapping.ListTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.PrimitiveTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.RepeatedTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.StructTypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMapping;
import org.apache.parquet.arrow.schema.SchemaMapping.TypeMappingVisitor;
import org.apache.parquet.arrow.schema.SchemaMapping.UnionTypeMapping;
import org.apache.parquet.example.Paper;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Test;

/**
 * @see SchemaConverter
 */
public class TestSchemaConverter {

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    if (nullable) {
      return new Field(name, FieldType.nullable(type), asList(children));
    } else {
      return new Field(name, FieldType.notNullable(type), asList(children));
    }
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  private final Schema complexArrowSchema = new Schema(asList(
      field("a", false, new ArrowType.Int(8, true)),
      field(
          "b",
          new ArrowType.Struct(),
          field("c", new ArrowType.Int(16, true)),
          field("d", new ArrowType.Utf8())),
      field("e", new ArrowType.List(), field(null, new ArrowType.Date(DateUnit.DAY))),
      field("f", new ArrowType.FixedSizeList(1), field(null, new ArrowType.Date(DateUnit.DAY))),
      field("g", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
      field("h", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
      field("i", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
      field("j", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
      field("k", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
      field("l", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
      field("m", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
      field(
          "e",
          new ArrowType.Map(false),
          field(null, false, new ArrowType.Date(DateUnit.DAY)),
          field(null, true, new ArrowType.Utf8()))));

  private final MessageType complexParquetSchema = Types.buildMessage()
      .addField(Types.optional(INT32).as(LogicalTypeAnnotation.intType(8)).named("a"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(INT32)
              .as(LogicalTypeAnnotation.intType(16))
              .named("c"))
          .addField(Types.optional(BINARY)
              .as(LogicalTypeAnnotation.stringType())
              .named("d"))
          .named("b"))
      .addField(Types.optionalList()
          .setElementType(Types.optional(INT32)
              .as(LogicalTypeAnnotation.dateType())
              .named("element"))
          .named("e"))
      .addField(Types.optionalList()
          .setElementType(Types.optional(INT32)
              .as(LogicalTypeAnnotation.dateType())
              .named("element"))
          .named("f"))
      .addField(Types.optional(FLOAT).named("g"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
          .named("h"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, NANOS))
          .named("i"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(false, MILLIS))
          .named("j"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, MICROS))
          .named("k"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(false, MICROS))
          .named("l"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY)
          .length(12)
          .as(LogicalTypeAnnotation.intervalType())
          .named("m"))
      .addField(Types.optionalMap()
          .key(Types.optional(INT32)
              .as(LogicalTypeAnnotation.dateType())
              .named("key"))
          .value(Types.optional(BINARY)
              .as(LogicalTypeAnnotation.stringType())
              .named("value"))
          .named("e"))
      .named("root");

  private final Schema allTypesArrowSchema = new Schema(asList(
      field("a", false, new ArrowType.Null()),
      field("b", new ArrowType.Struct(), field("ba", new ArrowType.Null())),
      field("c", new ArrowType.List(), field("ca", new ArrowType.Null())),
      field("d", new ArrowType.FixedSizeList(1), field("da", new ArrowType.Null())),
      field("e", new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("ea", new ArrowType.Null())),
      field("f", new ArrowType.Int(8, true)),
      field("f1", new ArrowType.Int(16, true)),
      field("f2", new ArrowType.Int(32, true)),
      field("f3", new ArrowType.Int(64, true)),
      field("f4", new ArrowType.Int(8, false)),
      field("f5", new ArrowType.Int(16, false)),
      field("f6", new ArrowType.Int(32, false)),
      field("f7", new ArrowType.Int(64, false)),
      field("g", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
      field("g1", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
      field("h", new ArrowType.Utf8()),
      field("i", new ArrowType.Binary()),
      field("j", new ArrowType.Bool()),
      field("k", new ArrowType.Decimal(5, 5)),
      field("k1", new ArrowType.Decimal(15, 5)),
      field("k2", new ArrowType.Decimal(25, 5)),
      field("l", new ArrowType.Date(DateUnit.DAY)),
      field("m", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
      field("n", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
      field("o", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
      field("o1", new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
      field("p", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
      field("q", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))));

  private final MessageType allTypesParquetSchema = Types.buildMessage()
      .addField(Types.optional(BINARY).named("a"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("ba"))
          .named("b"))
      .addField(Types.optionalList()
          .setElementType(Types.optional(BINARY).named("element"))
          .named("c"))
      .addField(Types.optionalList()
          .setElementType(Types.optional(BINARY).named("element"))
          .named("d"))
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("ea"))
          .named("e"))
      .addField(Types.optional(INT32).as(LogicalTypeAnnotation.intType(8)).named("f"))
      .addField(
          Types.optional(INT32).as(LogicalTypeAnnotation.intType(16)).named("f1"))
      .addField(
          Types.optional(INT32).as(LogicalTypeAnnotation.intType(32)).named("f2"))
      .addField(
          Types.optional(INT64).as(LogicalTypeAnnotation.intType(64)).named("f3"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(8, false))
          .named("f4"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(16, false))
          .named("f5"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(32, false))
          .named("f6"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.intType(64, false))
          .named("f7"))
      .addField(Types.optional(FLOAT).named("g"))
      .addField(Types.optional(DOUBLE).named("g1"))
      .addField(Types.optional(BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("h"))
      .addField(Types.optional(BINARY).named("i"))
      .addField(Types.optional(BOOLEAN).named("j"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.decimalType(5, 5))
          .named("k"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.decimalType(5, 15))
          .named("k1"))
      .addField(Types.optional(BINARY)
          .as(LogicalTypeAnnotation.decimalType(5, 25))
          .named("k2"))
      .addField(Types.optional(INT32).as(LogicalTypeAnnotation.dateType()).named("l"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.timeType(false, MILLIS))
          .named("m"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
          .named("n"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY)
          .length(12)
          .as(LogicalTypeAnnotation.intervalType())
          .named("o"))
      .addField(Types.optional(FIXED_LEN_BYTE_ARRAY)
          .length(12)
          .as(LogicalTypeAnnotation.intervalType())
          .named("o1"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timeType(false, NANOS))
          .named("p"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, NANOS))
          .named("q"))
      .named("root");

  private final Schema supportedTypesArrowSchema = new Schema(asList(
      field("b", new ArrowType.Struct(), field("ba", new ArrowType.Binary())),
      field("c", new ArrowType.List(), field(null, new ArrowType.Binary())),
      field("e", new ArrowType.Int(8, true)),
      field("e1", new ArrowType.Int(16, true)),
      field("e2", new ArrowType.Int(32, true)),
      field("e3", new ArrowType.Int(64, true)),
      field("e4", new ArrowType.Int(8, false)),
      field("e5", new ArrowType.Int(16, false)),
      field("e6", new ArrowType.Int(32, false)),
      field("e7", new ArrowType.Int(64, false)),
      field("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
      field("f1", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
      field("g", new ArrowType.Utf8()),
      field("h", new ArrowType.Binary()),
      field("i", new ArrowType.Bool()),
      field("j", new ArrowType.Decimal(5, 5)),
      field("j1", new ArrowType.Decimal(15, 5)),
      field("j2", new ArrowType.Decimal(25, 5)),
      field("k", new ArrowType.Date(DateUnit.DAY)),
      field("l", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
      field("m", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
      field("n", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
      field("o", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))));

  private final MessageType supportedTypesParquetSchema = Types.buildMessage()
      .addField(Types.optionalGroup()
          .addField(Types.optional(BINARY).named("ba"))
          .named("b"))
      .addField(Types.optionalList()
          .setElementType(Types.optional(BINARY).named("element"))
          .named("c"))
      .addField(Types.optional(INT32).as(LogicalTypeAnnotation.intType(8)).named("e"))
      .addField(
          Types.optional(INT32).as(LogicalTypeAnnotation.intType(16)).named("e1"))
      .addField(
          Types.optional(INT32).as(LogicalTypeAnnotation.intType(32)).named("e2"))
      .addField(
          Types.optional(INT64).as(LogicalTypeAnnotation.intType(64)).named("e3"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(8, false))
          .named("e4"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(16, false))
          .named("e5"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.intType(32, false))
          .named("e6"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.intType(64, false))
          .named("e7"))
      .addField(Types.optional(FLOAT).named("f"))
      .addField(Types.optional(DOUBLE).named("f1"))
      .addField(Types.optional(BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("g"))
      .addField(Types.optional(BINARY).named("h"))
      .addField(Types.optional(BOOLEAN).named("i"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.decimalType(5, 5))
          .named("j"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.decimalType(5, 15))
          .named("j1"))
      .addField(Types.optional(BINARY)
          .as(LogicalTypeAnnotation.decimalType(5, 25))
          .named("j2"))
      .addField(Types.optional(INT32).as(LogicalTypeAnnotation.dateType()).named("k"))
      .addField(Types.optional(INT32)
          .as(LogicalTypeAnnotation.timeType(false, MILLIS))
          .named("l"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
          .named("m"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timeType(true, NANOS))
          .named("n"))
      .addField(Types.optional(INT64)
          .as(LogicalTypeAnnotation.timestampType(true, NANOS))
          .named("o"))
      .named("root");

  private final Schema paperArrowSchema = new Schema(asList(
      field("DocId", false, new ArrowType.Int(64, true)),
      field(
          "Links",
          new ArrowType.Struct(),
          field("Backward", false, new ArrowType.List(), field(null, false, new ArrowType.Int(64, true))),
          field("Forward", false, new ArrowType.List(), field(null, false, new ArrowType.Int(64, true)))),
      field(
          "Name",
          false,
          new ArrowType.List(),
          field(
              null,
              false,
              new ArrowType.Struct(),
              field(
                  "Language",
                  false,
                  new ArrowType.List(),
                  field(
                      null,
                      false,
                      new ArrowType.Struct(),
                      field("Code", false, new ArrowType.Binary()),
                      field("Country", new ArrowType.Binary()))),
              field("Url", new ArrowType.Binary())))));

  private final SchemaConverter converter = new SchemaConverter();

  @Test
  public void testComplexArrowToParquet() {
    MessageType parquet = converter.fromArrow(complexArrowSchema).getParquetSchema();
    Assert.assertEquals(complexParquetSchema.toString(), parquet.toString()); // easier to read
    Assert.assertEquals(complexParquetSchema, parquet);
  }

  @Test
  public void testAllArrowToParquet() {
    MessageType parquet = converter.fromArrow(allTypesArrowSchema).getParquetSchema();
    Assert.assertEquals(allTypesParquetSchema.toString(), parquet.toString()); // easier to read
    Assert.assertEquals(allTypesParquetSchema, parquet);
  }

  @Test
  public void testSupportedParquetToArrow() {
    Schema arrow = converter.fromParquet(supportedTypesParquetSchema).getArrowSchema();
    assertEquals(supportedTypesArrowSchema, arrow);
  }

  @Test
  public void testRepeatedParquetToArrow() {
    Schema arrow = converter.fromParquet(Paper.schema).getArrowSchema();
    assertEquals(paperArrowSchema, arrow);
  }

  public void assertEquals(Schema left, Schema right) {
    compareFields(left.getFields(), right.getFields());
    Assert.assertEquals(left, right);
  }

  /**
   * for more pinpointed error on what is different
   */
  private void compareFields(List<Field> left, List<Field> right) {
    Assert.assertEquals(left + "\n" + right, left.size(), right.size());
    int size = left.size();
    for (int i = 0; i < size; i++) {
      Field expectedField = left.get(i);
      Field field = right.get(i);
      compareFields(expectedField.getChildren(), field.getChildren());
      Assert.assertEquals(expectedField, field);
    }
  }

  @Test
  public void testAllMap() {
    SchemaMapping map = converter.map(allTypesArrowSchema, allTypesParquetSchema);
    Assert.assertEquals(
        "p, s<p>, l<p>, l<p>, u<p>, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p, p",
        toSummaryString(map));
  }

  private String toSummaryString(SchemaMapping map) {
    List<TypeMapping> fields = map.getChildren();
    return toSummaryString(fields);
  }

  private String toSummaryString(List<TypeMapping> fields) {
    final StringBuilder sb = new StringBuilder();
    for (TypeMapping typeMapping : fields) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(typeMapping.accept(new TypeMappingVisitor<String>() {
        @Override
        public String visit(PrimitiveTypeMapping primitiveTypeMapping) {
          return "p";
        }

        @Override
        public String visit(StructTypeMapping structTypeMapping) {
          return "s";
        }

        @Override
        public String visit(UnionTypeMapping unionTypeMapping) {
          return "u";
        }

        @Override
        public String visit(ListTypeMapping listTypeMapping) {
          return "l";
        }

        @Override
        public String visit(SchemaMapping.MapTypeMapping mapTypeMapping) {
          return "m";
        }

        @Override
        public String visit(RepeatedTypeMapping repeatedTypeMapping) {
          return "r";
        }
      }));
      if (typeMapping.getChildren() != null && !typeMapping.getChildren().isEmpty()) {
        sb.append("<")
            .append(toSummaryString(typeMapping.getChildren()))
            .append(">");
      }
    }
    return sb.toString();
  }

  @Test
  public void testRepeatedMap() throws IOException {
    SchemaMapping map = converter.map(paperArrowSchema, Paper.schema);
    Assert.assertEquals("p, s<r<p>, r<p>>, r<s<r<s<p, p>>, p>>", toSummaryString(map));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testArrowTimeSecondToParquet() {
    converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Time(TimeUnit.SECOND, 32)))))
        .getParquetSchema();
  }

  @Test
  public void testArrowTimeMillisecondToParquet() {
    MessageType expected = converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Time(TimeUnit.MILLISECOND, 32)))))
        .getParquetSchema();
    Assert.assertEquals(
        expected,
        Types.buildMessage()
            .addField(Types.optional(INT32)
                .as(LogicalTypeAnnotation.timeType(false, MILLIS))
                .named("a"))
            .named("root"));
  }

  @Test
  public void testArrowTimeMicrosecondToParquet() {
    MessageType expected = converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Time(TimeUnit.MICROSECOND, 64)))))
        .getParquetSchema();
    Assert.assertEquals(
        expected,
        Types.buildMessage()
            .addField(Types.optional(INT64)
                .as(LogicalTypeAnnotation.timeType(false, MICROS))
                .named("a"))
            .named("root"));
  }

  @Test
  public void testParquetInt32TimeMillisToArrow() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(INT32)
            .as(LogicalTypeAnnotation.timeType(false, MILLIS))
            .named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Time(TimeUnit.MILLISECOND, 32))));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetInt64TimeMicrosToArrow() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timeType(false, MICROS))
            .named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Time(TimeUnit.MICROSECOND, 64))));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetFixedBinaryToArrow() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY).length(12).named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Binary())));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetMapToArrow() {
    GroupType mapType = Types.requiredMap().key(INT32).optionalValue(INT64).named("myMap");
    MessageType parquet = Types.buildMessage().addField(mapType).named("root");
    Schema expected = new Schema(asList(field(
        "myMap",
        new ArrowType.Map(false),
        field(null, false, new ArrowType.Int(32, true)),
        field(null, true, new ArrowType.Int(64, true)))));
    SchemaMapping mapping = converter.fromParquet(parquet);
    Schema actual = mapping.getArrowSchema();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParquetFixedBinaryToArrowDecimal() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY)
            .length(5)
            .as(LogicalTypeAnnotation.decimalType(2, 8))
            .named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Decimal(8, 2))));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetInt96ToArrowBinary() {
    MessageType parquet =
        Types.buildMessage().addField(Types.optional(INT96).named("a")).named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Binary())));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetInt96ToArrowTimestamp() {
    final Configuration configuration = new Configuration(false);
    configuration.setBoolean(SchemaConverter.CONVERT_INT96_TO_ARROW_TIMESTAMP, true);
    final SchemaConverter converterInt96ToTimestamp = new SchemaConverter(configuration);
    MessageType parquet =
        Types.buildMessage().addField(Types.optional(INT96).named("a")).named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))));
    Assert.assertEquals(
        expected, converterInt96ToTimestamp.fromParquet(parquet).getArrowSchema());
  }

  @Test(expected = IllegalStateException.class)
  public void testParquetInt64TimeMillisToArrow() {
    converter.fromParquet(Types.buildMessage()
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timeType(false, MILLIS))
            .named("a"))
        .named("root"));
  }

  @Test(expected = IllegalStateException.class)
  public void testParquetInt32TimeMicrosToArrow() {
    converter.fromParquet(Types.buildMessage()
        .addField(Types.optional(INT32)
            .as(LogicalTypeAnnotation.timeType(false, MICROS))
            .named("a"))
        .named("root"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testArrowTimestampSecondToParquet() {
    converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")))))
        .getParquetSchema();
  }

  @Test
  public void testArrowTimestampMillisecondToParquet() {
    MessageType expected = converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")))))
        .getParquetSchema();
    Assert.assertEquals(
        expected,
        Types.buildMessage()
            .addField(Types.optional(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
                .named("a"))
            .named("root"));
  }

  @Test
  public void testArrowTimestampMicrosecondToParquet() {
    MessageType expected = converter
        .fromArrow(new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")))))
        .getParquetSchema();
    Assert.assertEquals(
        expected,
        Types.buildMessage()
            .addField(Types.optional(INT64)
                .as(LogicalTypeAnnotation.timestampType(true, MICROS))
                .named("a"))
            .named("root"));
  }

  @Test
  public void testParquetInt64TimestampMillisToArrow() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
            .named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test
  public void testParquetInt64TimestampMicrosToArrow() {
    MessageType parquet = Types.buildMessage()
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, MICROS))
            .named("a"))
        .named("root");
    Schema expected = new Schema(asList(field("a", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));
    Assert.assertEquals(expected, converter.fromParquet(parquet).getArrowSchema());
  }

  @Test(expected = IllegalStateException.class)
  public void testParquetInt32TimestampMillisToArrow() {
    converter.fromParquet(Types.buildMessage()
        .addField(Types.optional(INT32)
            .as(LogicalTypeAnnotation.timestampType(false, MILLIS))
            .named("a"))
        .named("root"));
  }

  @Test(expected = IllegalStateException.class)
  public void testParquetInt32TimestampMicrosToArrow() {
    converter.fromParquet(Types.buildMessage()
        .addField(Types.optional(INT32)
            .as(LogicalTypeAnnotation.timestampType(false, MICROS))
            .named("a"))
        .named("root"));
  }
}
