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

package org.apache.parquet.filter2.dictionarylevel;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.filter2.dictionarylevel.DictionaryFilter.canDrop;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(Parameterized.class)
public class DictionaryFilterTest {

  private static final int nElements = 1000;
  private static final Configuration conf = new Configuration();
  private static final Path FILE_V1 = new Path("target/test/TestDictionaryFilter/testParquetFileV1.parquet");
  private static final Path FILE_V2 = new Path("target/test/TestDictionaryFilter/testParquetFileV2.parquet");
  private static final MessageType schema = parseMessageType(
      "message test { "
          + "required binary binary_field; "
          + "required binary single_value_field; "
          + "optional binary optional_single_value_field; "
          + "required fixed_len_byte_array(17) fixed_field (DECIMAL(40,4)); "
          + "required int32 int32_field; "
          + "required int64 int64_field; "
          + "required double double_field; "
          + "required float float_field; "
          + "required int32 plain_int32_field; "
          + "required binary fallback_binary_field; "
          + "required int96 int96_field; "
          + "} ");

  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
  private static final int[] intValues = new int[] {
      -100, 302, 3333333, 7654321, 1234567, -2000, -77775, 0, 75, 22223,
      77, 22221, -444443, 205, 12, 44444, 889, 66665, -777889, -7,
      52, 33, -257, 1111, 775, 26};
  private static final long[] longValues = new long[] {
      -100L, 302L, 3333333L, 7654321L, 1234567L, -2000L, -77775L, 0L,
      75L, 22223L, 77L, 22221L, -444443L, 205L, 12L, 44444L, 889L, 66665L,
      -777889L, -7L, 52L, 33L, -257L, 1111L, 775L, 26L};
  private static final Binary[] DECIMAL_VALUES = new Binary[] {
      toBinary("-9999999999999999999999999999999999999999", 17),
      toBinary("-9999999999999999999999999999999999999998", 17),
      toBinary(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), 17),
      toBinary(BigInteger.valueOf(Long.MIN_VALUE), 17),
      toBinary(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE), 17),
      toBinary("-1", 17),
      toBinary("0", 17),
      toBinary(BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE), 17),
      toBinary(BigInteger.valueOf(Long.MAX_VALUE), 17),
      toBinary(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), 17),
      toBinary("999999999999999999999999999999999999999", 17),
      toBinary("9999999999999999999999999999999999999998", 17),
      toBinary("9999999999999999999999999999999999999999", 17)
  };
  private static final Binary[] INT96_VALUES = new Binary[] {
      toBinary("-9999999999999999999999999999", 12),
      toBinary("-9999999999999999999999999998", 12),
      toBinary("-1234567890", 12),
      toBinary("-1", 12),
      toBinary("-0", 12),
      toBinary("1", 12),
      toBinary("1234567890", 12),
      toBinary("-9999999999999999999999999998", 12),
      toBinary("9999999999999999999999999999", 12)
  };

  private static Binary toBinary(String decimalWithoutScale, int byteCount) {
    return toBinary(new BigInteger(decimalWithoutScale), byteCount);
  }

  private static Binary toBinary(BigInteger decimalWithoutScale, int byteCount) {
    return FixedBinaryTestUtils.getFixedBinary(byteCount, decimalWithoutScale);
  }

  private static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      int index = i % ALPHABET.length();

      Group group = f.newGroup()
          .append("binary_field", ALPHABET.substring(index, index+1))
          .append("single_value_field", "sharp")
          .append("fixed_field", DECIMAL_VALUES[i % DECIMAL_VALUES.length])
          .append("int32_field", intValues[i % intValues.length])
          .append("int64_field", longValues[i % longValues.length])
          .append("double_field", toDouble(intValues[i % intValues.length]))
          .append("float_field", toFloat(intValues[i % intValues.length]))
          .append("plain_int32_field", i)
          .append("fallback_binary_field", i < (nElements / 2) ?
              ALPHABET.substring(index, index+1) : UUID.randomUUID().toString())
          .append("int96_field", INT96_VALUES[i % INT96_VALUES.length]);

      // 10% of the time, leave the field null
      if (index % 10 > 0) {
        group.append("optional_single_value_field", "sharp");
      }

      writer.write(group);
    }
    writer.close();
  }

  @BeforeClass
  public static void prepareFile() throws IOException {
    cleanup();
    prepareFile(PARQUET_1_0, FILE_V1);
    prepareFile(PARQUET_2_0, FILE_V2);
  }

  private static void prepareFile(WriterVersion version, Path file) throws IOException {
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withWriterVersion(version)
        .withCompressionCodec(GZIP)
        .withRowGroupSize(1024*1024)
        .withPageSize(1024)
        .enableDictionaryEncoding()
        .withDictionaryPageSize(2*1024)
        .withConf(conf)
        .build();
    writeData(f, writer);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    deleteFile(FILE_V1);
    deleteFile(FILE_V2);
  }

  private static void deleteFile(Path file) throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  @Parameters
  public static Object[] params() {
    return new Object[] {PARQUET_1_0, PARQUET_2_0};
  }

  List<ColumnChunkMetaData> ccmd;
  ParquetFileReader reader;
  DictionaryPageReadStore dictionaries;
  private Path file;
  private WriterVersion version;

  public DictionaryFilterTest(WriterVersion version) {
    this.version = version;
    switch (version) {
    case PARQUET_1_0:
      file = FILE_V1;
      break;
    case PARQUET_2_0:
      file = FILE_V2;
      break;
    }
  }

  @Before
  public void setUp() throws Exception {
    reader = ParquetFileReader.open(conf, file);
    ParquetMetadata meta = reader.getFooter();
    ccmd = meta.getBlocks().get(0).getColumns();
    dictionaries = reader.getDictionaryReader(meta.getBlocks().get(0));
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
  }

  @Test
  public void testDictionaryEncodedColumns() throws Exception {
    switch (version) {
    case PARQUET_1_0:
      testDictionaryEncodedColumnsV1();
      break;
    case PARQUET_2_0:
      testDictionaryEncodedColumnsV2();
      break;
    }
  }

  @SuppressWarnings("deprecation")
  private void testDictionaryEncodedColumnsV1() throws Exception {
    Set<String> dictionaryEncodedColumns = new HashSet<String>(Arrays.asList(
        "binary_field", "single_value_field", "optional_single_value_field", "int32_field", "int64_field",
        "double_field", "float_field", "int96_field"));
    for (ColumnChunkMetaData column : ccmd) {
      String name = column.getPath().toDotString();
      if (dictionaryEncodedColumns.contains(name)) {
        assertTrue("Column should be dictionary encoded: " + name,
            column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        assertFalse("Column should not have plain data pages" + name,
            column.getEncodings().contains(Encoding.PLAIN));
      } else {
        assertTrue("Column should have plain encoding: " + name,
            column.getEncodings().contains(Encoding.PLAIN));
        if (name.startsWith("fallback")) {
          assertTrue("Column should have some dictionary encoding: " + name,
              column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        } else {
          assertFalse("Column should have no dictionary encoding: " + name,
              column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        }
      }
    }
  }

  private void testDictionaryEncodedColumnsV2() throws Exception {
    Set<String> dictionaryEncodedColumns = new HashSet<String>(Arrays.asList(
        "binary_field", "single_value_field", "optional_single_value_field", "fixed_field", "int32_field",
        "int64_field", "double_field", "float_field", "int96_field"));
    for (ColumnChunkMetaData column : ccmd) {
      EncodingStats encStats = column.getEncodingStats();
      String name = column.getPath().toDotString();
      if (dictionaryEncodedColumns.contains(name)) {
        assertTrue("Column should have dictionary pages: " + name, encStats.hasDictionaryPages());
        assertTrue("Column should have dictionary encoded pages: " + name, encStats.hasDictionaryEncodedPages());
        assertFalse("Column should not have non-dictionary encoded pages: " + name,
            encStats.hasNonDictionaryEncodedPages());
      } else {
        assertTrue("Column should have non-dictionary encoded pages: " + name,
            encStats.hasNonDictionaryEncodedPages());
        if (name.startsWith("fallback")) {
          assertTrue("Column should have dictionary pages: " + name, encStats.hasDictionaryPages());
          assertTrue("Column should have dictionary encoded pages: " + name, encStats.hasDictionaryEncodedPages());
        } else {
          assertFalse("Column should not have dictionary pages: " + name, encStats.hasDictionaryPages());
          assertFalse("Column should not have dictionary encoded pages: " + name, encStats.hasDictionaryEncodedPages());
        }
      }
    }
  }

  @Test
  public void testEqBinary() throws Exception {
    BinaryColumn b = binaryColumn("binary_field");
    FilterPredicate pred = eq(b, Binary.fromString("c"));

    assertFalse("Should not drop block for lower case letters",
        canDrop(pred, ccmd, dictionaries));

    assertTrue("Should drop block for upper case letters",
        canDrop(eq(b, Binary.fromString("A")), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testEqFixed() throws Exception {
    BinaryColumn b = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
      assertTrue("Should drop block for -2",
          canDrop(eq(b, toBinary("-2", 17)), ccmd, dictionaries));
    }

    assertFalse("Should not drop block for -1",
        canDrop(eq(b, toBinary("-1", 17)), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testEqInt96() throws Exception {
    BinaryColumn b = binaryColumn("int96_field");

    // INT96 ordering is undefined => no filtering shall be done
    assertFalse("Should not drop block for -2",
        canDrop(eq(b, toBinary("-2", 12)), ccmd, dictionaries));

    assertFalse("Should not drop block for -1",
        canDrop(eq(b, toBinary("-1", 12)), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testNotEqBinary() throws Exception {
    BinaryColumn sharp = binaryColumn("single_value_field");
    BinaryColumn sharpAndNull = binaryColumn("optional_single_value_field");
    BinaryColumn b = binaryColumn("binary_field");

    assertTrue("Should drop block with only the excluded value",
        canDrop(notEq(sharp, Binary.fromString("sharp")), ccmd, dictionaries));

    assertFalse("Should not drop block with any other value",
        canDrop(notEq(sharp, Binary.fromString("applause")), ccmd, dictionaries));

    assertFalse("Should not drop block with only the excluded value and null",
        canDrop(notEq(sharpAndNull, Binary.fromString("sharp")), ccmd, dictionaries));

    assertFalse("Should not drop block with any other value",
        canDrop(notEq(sharpAndNull, Binary.fromString("applause")), ccmd, dictionaries));

    assertFalse("Should not drop block with a known value",
        canDrop(notEq(b, Binary.fromString("x")), ccmd, dictionaries));

    assertFalse("Should not drop block with a known value",
        canDrop(notEq(b, Binary.fromString("B")), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(notEq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testLtInt() throws Exception {
    IntColumn i32 = intColumn("int32_field");
    int lowest = Integer.MAX_VALUE;
    for (int value : intValues) {
      lowest = Math.min(lowest, value);
    }

    assertTrue("Should drop: < lowest value",
        canDrop(lt(i32, lowest), ccmd, dictionaries));
    assertFalse("Should not drop: < (lowest value + 1)",
        canDrop(lt(i32, lowest + 1), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(lt(i32, Integer.MAX_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testLtFixed() throws Exception {
    BinaryColumn fixed = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
    assertTrue("Should drop: < lowest value",
        canDrop(lt(fixed, DECIMAL_VALUES[0]), ccmd, dictionaries));
    }

    assertFalse("Should not drop: < 2nd lowest value",
        canDrop(lt(fixed, DECIMAL_VALUES[1]), ccmd, dictionaries));
  }

  @Test
  public void testLtEqLong() throws Exception {
    LongColumn i64 = longColumn("int64_field");
    long lowest = Long.MAX_VALUE;
    for (long value : longValues) {
      lowest = Math.min(lowest, value);
    }

    assertTrue("Should drop: <= lowest - 1",
        canDrop(ltEq(i64, lowest - 1), ccmd, dictionaries));
    assertFalse("Should not drop: <= lowest",
        canDrop(ltEq(i64, lowest), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(ltEq(i64, Long.MAX_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testGtFloat() throws Exception {
    FloatColumn f = floatColumn("float_field");
    float highest = Float.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toFloat(value));
    }

    assertTrue("Should drop: > highest value",
        canDrop(gt(f, highest), ccmd, dictionaries));
    assertFalse("Should not drop: > (highest value - 1.0)",
        canDrop(gt(f, highest - 1.0f), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(gt(f, Float.MIN_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testGtEqDouble() throws Exception {
    DoubleColumn d = doubleColumn("double_field");
    double highest = Double.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toDouble(value));
    }

    assertTrue("Should drop: >= highest + 0.00000001",
        canDrop(gtEq(d, highest + 0.00000001), ccmd, dictionaries));
    assertFalse("Should not drop: >= highest",
        canDrop(gtEq(d, highest), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(gtEq(d, Double.MIN_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testInBinary() throws Exception {
    BinaryColumn b = binaryColumn("binary_field");

    Set<Binary> set1 = new HashSet<>();
    set1.add(Binary.fromString("F"));
    set1.add(Binary.fromString("C"));
    set1.add(Binary.fromString("h"));
    set1.add(Binary.fromString("E"));
    FilterPredicate predIn1 = in(b, set1);
    FilterPredicate predNotIn1 = notIn(b, set1);
    assertFalse("Should not drop block", canDrop(predIn1, ccmd, dictionaries));
    assertFalse("Should not drop block", canDrop(predNotIn1, ccmd, dictionaries));

    Set<Binary> set2 = new HashSet<>();
    for (int  i = 0; i < 26; i++) {
      set2.add(Binary.fromString(Character.toString((char) (i + 97))));
    }
    set2.add(Binary.fromString("A"));
    FilterPredicate predIn2 = in(b, set2);
    FilterPredicate predNotIn2 = notIn(b, set2);
    assertFalse("Should not drop block", canDrop(predIn2, ccmd, dictionaries));
    assertTrue("Should not drop block", canDrop(predNotIn2, ccmd, dictionaries));

    Set<Binary> set3 = new HashSet<>();
    set3.add(Binary.fromString("F"));
    set3.add(Binary.fromString("C"));
    set3.add(Binary.fromString("A"));
    set3.add(Binary.fromString("E"));
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertTrue("Should drop block", canDrop(predIn3, ccmd, dictionaries));
    assertFalse("Should not drop block", canDrop(predNotIn3, ccmd, dictionaries));

    Set<Binary> set4 = new HashSet<>();
    set4.add(null);
    FilterPredicate predIn4 = in(b, set4);
    FilterPredicate predNotIn4 = notIn(b, set4);
    assertFalse("Should not drop block for null", canDrop(predIn4, ccmd, dictionaries));
    assertFalse("Should not drop block for null", canDrop(predNotIn4, ccmd, dictionaries));

    BinaryColumn sharpAndNull = binaryColumn("optional_single_value_field");

    // Test the case that all non-null values are in the set but the column may have nulls and the set has no nulls.
    Set<Binary> set5 = new HashSet<>();
    set5.add(Binary.fromString("sharp"));
    FilterPredicate predNotIn5 = notIn(sharpAndNull, set5);
    FilterPredicate predIn5 = in(sharpAndNull, set5);
    assertFalse("Should not drop block",
      canDrop(predNotIn5, ccmd, dictionaries));
    assertFalse("Should not drop block", canDrop(predIn5, ccmd, dictionaries));
  }

  @Test
  public void testInFixed() throws Exception {
    BinaryColumn b = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
      Set<Binary> set1 = new HashSet<>();
      set1.add(toBinary("-2", 17));
      set1.add(toBinary("-22", 17));
      set1.add(toBinary("12345", 17));
      FilterPredicate predIn1 = in(b, set1);
      FilterPredicate predNotIn1 = notIn(b, set1);
      assertTrue("Should drop block for in (-2, -22, 12345)",
        canDrop(predIn1, ccmd, dictionaries));
      assertFalse("Should not drop block for notIn (-2, -22, 12345)",
        canDrop(predNotIn1, ccmd, dictionaries));

      Set<Binary> set2 = new HashSet<>();
      set2.add(toBinary("-1", 17));
      set2.add(toBinary("0", 17));
      set2.add(toBinary("12345", 17));
      assertFalse("Should not drop block for in (-1, 0, 12345)",
        canDrop(in(b, set2), ccmd, dictionaries));
      assertFalse("Should not drop block for in (-1, 0, 12345)",
        canDrop(notIn(b, set2), ccmd, dictionaries));
    }

    Set<Binary> set3 = new HashSet<>();
    set3.add(null);
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertFalse("Should not drop block for null",
      canDrop(predIn3, ccmd, dictionaries));
    assertFalse("Should not drop block for null",
      canDrop(predNotIn3, ccmd, dictionaries));
  }

  @Test
  public void testInInt96() throws Exception {
    // INT96 ordering is undefined => no filtering shall be done
    BinaryColumn b = binaryColumn("int96_field");

    Set<Binary> set1 = new HashSet<>();
    set1.add(toBinary("-2", 12));
    set1.add(toBinary("-0", 12));
    set1.add(toBinary("12345", 12));
    FilterPredicate predIn1 = in(b, set1);
    FilterPredicate predNotIn1 = notIn(b, set1);
    assertFalse("Should not drop block for in (-2, -0, 12345)",
      canDrop(predIn1, ccmd, dictionaries));
    assertFalse("Should not drop block for notIn (-2, -0, 12345)",
      canDrop(predNotIn1, ccmd, dictionaries));

    Set<Binary> set2 = new HashSet<>();
    set2.add(toBinary("-2", 17));
    set2.add(toBinary("12345", 17));
    set2.add(toBinary("-789", 17));
    FilterPredicate predIn2 = in(b, set2);
    FilterPredicate predNotIn2 = notIn(b, set2);
    assertFalse("Should not drop block for in (-2, 12345, -789)",
      canDrop(predIn2, ccmd, dictionaries));
    assertFalse("Should not drop block for notIn (-2, 12345, -789)",
      canDrop(predNotIn2, ccmd, dictionaries));

    Set<Binary> set3 = new HashSet<>();
    set3.add(null);
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertFalse("Should not drop block for null",
      canDrop(predIn3, ccmd, dictionaries));
    assertFalse("Should not drop block for null",
      canDrop(predNotIn3, ccmd, dictionaries));
  }

  @Test
  public void testAnd() throws Exception {
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertTrue("Should drop when either predicate must be false",
        canDrop(and(B, y), ccmd, dictionaries));
    assertTrue("Should drop when either predicate must be false",
        canDrop(and(x, C), ccmd, dictionaries));
    assertTrue("Should drop when either predicate must be false",
        canDrop(and(B, C), ccmd, dictionaries));
    assertFalse("Should not drop when either predicate could be true",
        canDrop(and(x, y), ccmd, dictionaries));
  }

  @Test
  public void testOr() throws Exception {
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(B, y), ccmd, dictionaries));
    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(x, C), ccmd, dictionaries));
    assertTrue("Should drop when both predicates must be false",
        canDrop(or(B, C), ccmd, dictionaries));
    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(x, y), ccmd, dictionaries));
  }


  @Test
  public void testUdp() throws Exception {
    InInt32UDP dropabble = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP undroppable = new InInt32UDP(ImmutableSet.of(205));

    assertTrue("Should drop block for non-matching UDP",
      canDrop(userDefined(intColumn("int32_field"), dropabble), ccmd, dictionaries));

    assertFalse("Should not drop block for matching UDP",
      canDrop(userDefined(intColumn("int32_field"), undroppable), ccmd, dictionaries));
  }

  @Test
  public void testInverseUdp() throws Exception {
    InInt32UDP droppable = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP undroppable = new InInt32UDP(ImmutableSet.of(205));
    Set<Integer> allValues = ImmutableSet.copyOf(Ints.asList(intValues));
    InInt32UDP completeMatch = new InInt32UDP(allValues);

    FilterPredicate inverse =
      LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), droppable)));
    FilterPredicate inverse1 =
      LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), undroppable)));
    FilterPredicate inverse2 =
      LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), completeMatch)));

    assertFalse("Should not drop block for inverse of non-matching UDP",
      canDrop(inverse, ccmd, dictionaries));

    assertFalse("Should not drop block for inverse of UDP with some matches",
      canDrop(inverse1, ccmd, dictionaries));

    assertTrue("Should drop block for inverse of UDP with all matches",
      canDrop(inverse2, ccmd, dictionaries));
  }

  @Test
  public void testColumnWithoutDictionary() throws Exception {
    IntColumn plain = intColumn("plain_int32_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertFalse("Should never drop block using plain encoding",
        canDrop(eq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(lt(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(ltEq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore));

    verifyZeroInteractions(dictionaryStore);
  }

  @Test
  public void testColumnWithDictionaryAndPlainEncodings() throws Exception {
    IntColumn plain = intColumn("fallback_binary_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertFalse("Should never drop block using plain encoding",
        canDrop(eq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(lt(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(ltEq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore));

    verifyZeroInteractions(dictionaryStore);
  }

  @Test
  public void testEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for non-null query",
        canDrop(eq(b, Binary.fromString("any")), ccmd, dictionaries));

    assertFalse("Should not drop block null query",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testNotEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertFalse("Should not drop block for non-null query",
        canDrop(notEq(b, Binary.fromString("any")), ccmd, dictionaries));

    assertTrue("Should not drop block null query",
        canDrop(notEq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testLtMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(lt(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testLtEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(ltEq(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testGtMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(gt(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testGtEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(gtEq(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testUdpMissingColumn() throws Exception {
    InInt32UDP nullRejecting = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP nullAccepting = new InInt32UDP(Sets.newHashSet((Integer) null));
    IntColumn fake = intColumn("missing_column");

    assertTrue("Should drop block for null rejecting udp",
      canDrop(userDefined(fake, nullRejecting), ccmd, dictionaries));
    assertFalse("Should not drop block for null accepting udp",
      canDrop(userDefined(fake, nullAccepting), ccmd, dictionaries));
  }


  @Test
  public void testInverseUdpMissingColumn() throws Exception {
    InInt32UDP nullRejecting = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP nullAccepting = new InInt32UDP(Sets.newHashSet((Integer) null));
    IntColumn fake = intColumn("missing_column");

    assertTrue("Should drop block for null accepting udp",
      canDrop(LogicalInverseRewriter.rewrite(not(userDefined(fake, nullAccepting))), ccmd, dictionaries));
    assertFalse("Should not drop block for null rejecting udp",
      canDrop(LogicalInverseRewriter.rewrite(not(userDefined(fake, nullRejecting))), ccmd, dictionaries));
  }


  private static final class InInt32UDP extends UserDefinedPredicate<Integer> implements Serializable {

    private final Set<Integer> ints;

    InInt32UDP(Set<Integer> ints) {
      this.ints = ints;
    }

    @Override
    public boolean keep(Integer value) {
      return ints.contains(value);
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
      return false;
    }
  }

  private static double toDouble(int value) {
    return (value * 1.0);
  }

  private static float toFloat(int value) {
    return (float) (value * 2.0);
  }
}
