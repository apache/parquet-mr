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
package org.apache.parquet.column.values.factory;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.required;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

public class DefaultValuesWriterFactoryTest {

  @Test
  public void testBoolean() {
    doTestValueWriter(
        PrimitiveTypeName.BOOLEAN,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        BooleanPlainValuesWriter.class);
  }

  @Test
  public void testBoolean_V2() {
    doTestValueWriter(
        PrimitiveTypeName.BOOLEAN,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        RunLengthBitPackingHybridValuesWriter.class);
  }

  @Test
  public void testFixedLenByteArray() {
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testFixedLenByteArray_WithByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        true,
        ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFixedLenByteArray_V2() {
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        DictionaryValuesWriter.class,
        DeltaByteArrayWriter.class);
  }

  @Test
  public void testFixedLenByteArray_V2_WithByteStreamSplit() {
    // DELTA_BYTE_ARRAY still selected by default
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        true,
        DictionaryValuesWriter.class,
        DeltaByteArrayWriter.class);
    // Some logical types select BYTE_STREAM_SPLIT as a heuristic
    LogicalTypeAnnotation[] logicalTypes = {
      LogicalTypeAnnotation.float16Type(), LogicalTypeAnnotation.decimalType(4, 18)
    };
    for (LogicalTypeAnnotation logicalType : logicalTypes) {
      ColumnDescriptor path = createColumnDescriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, logicalType);
      doTestValueWriter(
          path,
          WriterVersion.PARQUET_2_0,
          true,
          false,
          true,
          DictionaryValuesWriter.class,
          ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter.class);
    }
  }

  public void testFixedLenByteArray_V2_WithByteStreamSplit_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        true,
        DeltaByteArrayWriter.class);
    LogicalTypeAnnotation[] logicalTypes = {
      LogicalTypeAnnotation.float16Type(), LogicalTypeAnnotation.decimalType(4, 18)
    };
    for (LogicalTypeAnnotation logicalType : logicalTypes) {
      ColumnDescriptor path = createColumnDescriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, logicalType);
      doTestValueWriter(
          path,
          WriterVersion.PARQUET_2_0,
          false,
          false,
          true,
          ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter.class);
    }
  }

  @Test
  public void testFixedLenByteArray_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        false,
        DeltaByteArrayWriter.class);
  }

  @Test
  public void testBinary() {
    doTestValueWriter(
        PrimitiveTypeName.BINARY,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainBinaryDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testBinary_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.BINARY, WriterVersion.PARQUET_1_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testBinary_V2() {
    doTestValueWriter(
        PrimitiveTypeName.BINARY,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainBinaryDictionaryValuesWriter.class,
        DeltaByteArrayWriter.class);
  }

  @Test
  public void testBinary_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.BINARY, WriterVersion.PARQUET_2_0, false, false, false, DeltaByteArrayWriter.class);
  }

  @Test
  public void testInt32() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainIntegerDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testInt32_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT32, WriterVersion.PARQUET_1_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testInt32_ByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        true,
        PlainIntegerDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt32_ByteStreamSplit_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_1_0,
        false,
        false,
        true,
        ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt32_V2() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainIntegerDictionaryValuesWriter.class,
        DeltaBinaryPackingValuesWriter.class);
  }

  @Test
  public void testInt32_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        false,
        DeltaBinaryPackingValuesWriter.class);
  }

  public void testInt32_V2_ByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        true,
        PlainIntegerDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt32_V2_ByteStreamSplit_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT32,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        true,
        ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt64() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainLongDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testInt64_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT64, WriterVersion.PARQUET_1_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testInt64_ByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        true,
        PlainLongDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt64_ByteStreamSplit_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_1_0,
        false,
        false,
        true,
        ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt64_V2() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainLongDictionaryValuesWriter.class,
        DeltaBinaryPackingValuesWriterForLong.class);
  }

  @Test
  public void testInt64_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        false,
        DeltaBinaryPackingValuesWriterForLong.class);
  }

  @Test
  public void testInt64_V2_ByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        true,
        PlainLongDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt64_V2_ByteStreamSplit_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT64,
        WriterVersion.PARQUET_1_0,
        false,
        false,
        true,
        ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testInt96() {
    doTestValueWriter(
        PrimitiveTypeName.INT96,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainFixedLenArrayDictionaryValuesWriter.class,
        FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT96,
        WriterVersion.PARQUET_1_0,
        false,
        false,
        false,
        FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_V2() {
    doTestValueWriter(
        PrimitiveTypeName.INT96,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainFixedLenArrayDictionaryValuesWriter.class,
        FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testInt96_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.INT96,
        WriterVersion.PARQUET_2_0,
        false,
        false,
        false,
        FixedLenByteArrayPlainValuesWriter.class);
  }

  @Test
  public void testDouble() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainDoubleDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testDouble_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE, WriterVersion.PARQUET_1_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testDouble_V2() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainDoubleDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testDouble_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE, WriterVersion.PARQUET_2_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testFloat() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_1_0,
        true,
        false,
        false,
        PlainFloatDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testFloat_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT, WriterVersion.PARQUET_1_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V2() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_2_0,
        true,
        false,
        false,
        PlainFloatDictionaryValuesWriter.class,
        PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V2_NoDict() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT, WriterVersion.PARQUET_2_0, false, false, false, PlainValuesWriter.class);
  }

  @Test
  public void testFloat_V1_WithByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_1_0,
        false,
        true,
        false,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V1_WithByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_1_0,
        false,
        true,
        false,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFloat_V2_WithByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_2_0,
        false,
        true,
        false,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V2_WithByteStreamSplit() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_2_0,
        false,
        true,
        false,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFloat_V1_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_1_0,
        true,
        true,
        false,
        PlainFloatDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V1_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_1_0,
        true,
        true,
        false,
        PlainDoubleDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testFloat_V2_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
        PrimitiveTypeName.FLOAT,
        WriterVersion.PARQUET_2_0,
        true,
        true,
        false,
        PlainFloatDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testDouble_V2_WithByteStreamSplitAndDictionary() {
    doTestValueWriter(
        PrimitiveTypeName.DOUBLE,
        WriterVersion.PARQUET_2_0,
        true,
        true,
        false,
        PlainDoubleDictionaryValuesWriter.class,
        ByteStreamSplitValuesWriter.class);
  }

  @Test
  public void testColumnWiseDictionaryWithFalseDefault() {
    ValuesWriterFactory factory = getDefaultFactory(
        WriterVersion.PARQUET_2_0, false, "binary_dict", "boolean_dict", "float_dict", "int32_dict");
    validateFactory(
        factory, BINARY, "binary_dict", PlainBinaryDictionaryValuesWriter.class, DeltaByteArrayWriter.class);
    validateFactory(factory, BINARY, "binary_no_dict", DeltaByteArrayWriter.class);
    validateFactory(factory, BOOLEAN, "boolean_dict", RunLengthBitPackingHybridValuesWriter.class);
    validateFactory(factory, BOOLEAN, "boolean_no_dict", RunLengthBitPackingHybridValuesWriter.class);
    validateFactory(factory, FLOAT, "float_dict", PlainFloatDictionaryValuesWriter.class, PlainValuesWriter.class);
    validateFactory(factory, FLOAT, "float_no_dict", PlainValuesWriter.class);
    validateFactory(
        factory,
        INT32,
        "int32_dict",
        PlainIntegerDictionaryValuesWriter.class,
        DeltaBinaryPackingValuesWriter.class);
    validateFactory(factory, INT32, "int32_no_dict", DeltaBinaryPackingValuesWriter.class);
  }

  @Test
  public void testColumnWiseDictionaryWithTrueDefault() {
    ValuesWriterFactory factory = getDefaultFactory(
        WriterVersion.PARQUET_2_0, true, "binary_no_dict", "boolean_no_dict", "float_no_dict", "int32_no_dict");
    validateFactory(
        factory, BINARY, "binary_dict", PlainBinaryDictionaryValuesWriter.class, DeltaByteArrayWriter.class);
    validateFactory(factory, BINARY, "binary_no_dict", DeltaByteArrayWriter.class);
    validateFactory(factory, BOOLEAN, "boolean_dict", RunLengthBitPackingHybridValuesWriter.class);
    validateFactory(factory, BOOLEAN, "boolean_no_dict", RunLengthBitPackingHybridValuesWriter.class);
    validateFactory(factory, FLOAT, "float_dict", PlainFloatDictionaryValuesWriter.class, PlainValuesWriter.class);
    validateFactory(factory, FLOAT, "float_no_dict", PlainValuesWriter.class);
    validateFactory(
        factory,
        INT32,
        "int32_dict",
        PlainIntegerDictionaryValuesWriter.class,
        DeltaBinaryPackingValuesWriter.class);
    validateFactory(factory, INT32, "int32_no_dict", DeltaBinaryPackingValuesWriter.class);
  }

  private void validateFactory(
      ValuesWriterFactory factory,
      PrimitiveTypeName typeName,
      String colName,
      Class<? extends ValuesWriter> initialWriterClass,
      Class<? extends ValuesWriter> fallbackWriterClass) {
    ColumnDescriptor column = createColumnDescriptor(typeName, colName);
    ValuesWriter writer = factory.newValuesWriter(column);
    validateFallbackWriter(writer, initialWriterClass, fallbackWriterClass);
  }

  private void validateFactory(
      ValuesWriterFactory factory,
      PrimitiveTypeName typeName,
      String colName,
      Class<? extends ValuesWriter> expectedWriterClass) {
    ColumnDescriptor column = createColumnDescriptor(typeName, colName);
    ValuesWriter writer = factory.newValuesWriter(column);
    validateWriterType(writer, expectedWriterClass);
  }

  private void doTestValueWriter(
      PrimitiveTypeName typeName,
      WriterVersion version,
      boolean enableDictionary,
      boolean enableByteStreamSplit,
      boolean enableExtendedByteStreamSplit,
      Class<? extends ValuesWriter> expectedValueWriterClass) {
    ColumnDescriptor mockPath = createColumnDescriptor(typeName);
    doTestValueWriter(
        mockPath,
        version,
        enableDictionary,
        enableByteStreamSplit,
        enableExtendedByteStreamSplit,
        expectedValueWriterClass);
  }

  private void doTestValueWriter(
      ColumnDescriptor path,
      WriterVersion version,
      boolean enableDictionary,
      boolean enableByteStreamSplit,
      boolean enableExtendedByteStreamSplit,
      Class<? extends ValuesWriter> expectedValueWriterClass) {
    ValuesWriterFactory factory =
        getDefaultFactory(version, enableDictionary, enableByteStreamSplit, enableExtendedByteStreamSplit);
    ValuesWriter writer = factory.newValuesWriter(path);

    validateWriterType(writer, expectedValueWriterClass);
  }

  private void doTestValueWriter(
      PrimitiveTypeName typeName,
      WriterVersion version,
      boolean enableDictionary,
      boolean enableByteStreamSplit,
      boolean enableExtendedByteStreamSplit,
      Class<? extends ValuesWriter> initialValueWriterClass,
      Class<? extends ValuesWriter> fallbackValueWriterClass) {
    ColumnDescriptor mockPath = createColumnDescriptor(typeName);
    doTestValueWriter(
        mockPath,
        version,
        enableDictionary,
        enableByteStreamSplit,
        enableExtendedByteStreamSplit,
        initialValueWriterClass,
        fallbackValueWriterClass);
  }

  private void doTestValueWriter(
      ColumnDescriptor path,
      WriterVersion version,
      boolean enableDictionary,
      boolean enableByteStreamSplit,
      boolean enableExtendedByteStreamSplit,
      Class<? extends ValuesWriter> initialValueWriterClass,
      Class<? extends ValuesWriter> fallbackValueWriterClass) {
    ValuesWriterFactory factory =
        getDefaultFactory(version, enableDictionary, enableByteStreamSplit, enableExtendedByteStreamSplit);
    ValuesWriter writer = factory.newValuesWriter(path);

    validateFallbackWriter(writer, initialValueWriterClass, fallbackValueWriterClass);
  }

  private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName) {
    return createColumnDescriptor(typeName, (LogicalTypeAnnotation) null);
  }

  private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName, LogicalTypeAnnotation logicalType) {
    return createColumnDescriptor(typeName, "fake_" + typeName.name().toLowerCase() + "_col", logicalType);
  }

  private ColumnDescriptor createColumnDescriptor(PrimitiveTypeName typeName, String name) {
    return createColumnDescriptor(typeName, name, null);
  }

  private ColumnDescriptor createColumnDescriptor(
      PrimitiveTypeName typeName, String name, LogicalTypeAnnotation logicalType) {
    PrimitiveType type = required(typeName).length(1).named(name).withLogicalTypeAnnotation(logicalType);
    return new ColumnDescriptor(new String[] {name}, type, 0, 0);
  }

  private ValuesWriterFactory getDefaultFactory(
      WriterVersion writerVersion,
      boolean enableDictionary,
      boolean enableByteStreamSplit,
      boolean enableExtendedByteStreamSplit) {
    ValuesWriterFactory factory = new DefaultValuesWriterFactory();
    ParquetProperties.builder()
        .withDictionaryEncoding(enableDictionary)
        .withByteStreamSplitEncoding(enableByteStreamSplit)
        .withExtendedByteStreamSplitEncoding(enableExtendedByteStreamSplit)
        .withWriterVersion(writerVersion)
        .withValuesWriterFactory(factory)
        .build();

    return factory;
  }

  private ValuesWriterFactory getDefaultFactory(
      WriterVersion writerVersion, boolean dictEnabledDefault, String... dictInverseColumns) {
    ValuesWriterFactory factory = new DefaultValuesWriterFactory();
    ParquetProperties.Builder builder = ParquetProperties.builder()
        .withDictionaryEncoding(dictEnabledDefault)
        .withWriterVersion(writerVersion)
        .withValuesWriterFactory(factory);
    for (String column : dictInverseColumns) {
      builder.withDictionaryEncoding(column, !dictEnabledDefault);
    }
    builder.build();

    return factory;
  }

  private void validateWriterType(ValuesWriter writer, Class<? extends ValuesWriter> valuesWriterClass) {
    assertTrue("Not instance of: " + valuesWriterClass.getName(), valuesWriterClass.isInstance(writer));
  }

  private void validateFallbackWriter(
      ValuesWriter writer,
      Class<? extends ValuesWriter> initialWriterClass,
      Class<? extends ValuesWriter> fallbackWriterClass) {
    validateWriterType(writer, FallbackValuesWriter.class);

    FallbackValuesWriter wr = (FallbackValuesWriter) writer;
    validateWriterType(wr.initialWriter, initialWriterClass);
    validateWriterType(wr.fallBackWriter, fallbackWriterClass);
  }
}
