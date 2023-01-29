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
package org.apache.parquet.proto;

import com.google.common.base.Joiner;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.Test;
import org.apache.parquet.proto.TestUtils;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.apache.parquet.proto.test.Trees;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;

public class ProtoSchemaConverterTest {

  private static final int PAR_RECURSION_DEPTH = 2;
  private static final Joiner JOINER = Joiner.on(System.getProperty("line.separator"));

  /**
   * Converts given pbClass to parquet schema and compares it with expected parquet schema.
   */
  private static void testConversion(Class<? extends Message> pbClass, String parquetSchemaString, boolean parquetSpecsCompliant) {
    testConversion(pbClass, parquetSchemaString, new ProtoSchemaConverter(parquetSpecsCompliant));
  }

  private static void testConversion(Class<? extends Message> pbClass, String parquetSchemaString) {
    testConversion(pbClass, parquetSchemaString, true);
  }

  private static void testConversion(Class<? extends Message> pbClass, String parquetSchemaString, ProtoSchemaConverter converter) {
    MessageType schema = converter.convert(pbClass);
    MessageType expectedMT = MessageTypeParser.parseMessageType(parquetSchemaString);
    assertEquals(expectedMT.toString(), schema.toString());
  }


  /**
   * Tests that all protocol buffer datatypes are converted to correct parquet datatypes.
   */
  @Test
  public void testConvertAllDatatypes() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.SchemaConverterAllDatatypes {",
        "  optional double optionalDouble = 1;",
        "  optional float optionalFloat = 2;",
        "  optional int32 optionalInt32 = 3;",
        "  optional int64 optionalInt64 = 4;",
        "  optional int32 optionalUInt32 = 5;",
        "  optional int64 optionalUInt64 = 6;",
        "  optional int32 optionalSInt32 = 7;",
        "  optional int64 optionalSInt64 = 8;",
        "  optional int32 optionalFixed32 = 9;",
        "  optional int64 optionalFixed64 = 10;",
        "  optional int32 optionalSFixed32 = 11;",
        "  optional int64 optionalSFixed64 = 12;",
        "  optional boolean optionalBool = 13;",
        "  optional binary optionalString (UTF8) = 14;",
        "  optional binary optionalBytes = 15;",
        "  optional group optionalMessage = 16 {",
        "    optional int32 someId = 3;",
        "  }",
        "  optional group pbgroup = 17 {",
        "    optional int32 groupInt = 2;",
        "  }",
        " optional binary optionalEnum (ENUM)  = 18;",
        "}");

    testConversion(TestProtobuf.SchemaConverterAllDatatypes.class, expectedSchema);
  }

  /**
   * Tests that all protocol buffer datatypes are converted to correct parquet datatypes.
   */
  @Test
  public void testProto3ConvertAllDatatypes() {
    String expectedSchema = JOINER.join(
        "message TestProto3.SchemaConverterAllDatatypes {",
        "  optional double optionalDouble = 1;",
        "  optional float optionalFloat = 2;",
        "  optional int32 optionalInt32 = 3;",
        "  optional int64 optionalInt64 = 4;",
        "  optional int32 optionalUInt32 = 5;",
        "  optional int64 optionalUInt64 = 6;",
        "  optional int32 optionalSInt32 = 7;",
        "  optional int64 optionalSInt64 = 8;",
        "  optional int32 optionalFixed32 = 9;",
        "  optional int64 optionalFixed64 = 10;",
        "  optional int32 optionalSFixed32 = 11;",
        "  optional int64 optionalSFixed64 = 12;",
        "  optional boolean optionalBool = 13;",
        "  optional binary optionalString (UTF8) = 14;",
        "  optional binary optionalBytes = 15;",
        "  optional group optionalMessage = 16 {",
        "    optional int32 someId = 3;",
        "  }",
        "  optional binary optionalEnum (ENUM) = 18;",
        "  optional int32 someInt32 = 19;",
        "  optional binary someString (UTF8) = 20;",
        "  optional group optionalMap (MAP) = 21 {",
        "    repeated group key_value {",
        "      required int64 key;",
        "      optional group value {",
        "        optional int32 someId = 3;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProto3.SchemaConverterAllDatatypes.class, expectedSchema);
  }

  @Test
  public void testConvertRepetition() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.SchemaConverterRepetition {",
        "  optional int32 optionalPrimitive = 1;",
        "  required int32 requiredPrimitive = 2;",
        "  optional group repeatedPrimitive (LIST) = 3 {",
        "    repeated group list {",
        "      required int32 element;",
        "    }",
        "  }",
        "  optional group optionalMessage = 7 {",
        "    optional int32 someId = 3;",
        "  }",
        "  required group requiredMessage = 8 {",
        "    optional int32 someId= 3;",
        "  }",
        "  optional group repeatedMessage (LIST) = 9 {",
        "    repeated group list {",
        "      optional group element {",
        "        optional int32 someId = 3;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProtobuf.SchemaConverterRepetition.class, expectedSchema);
  }

  @Test
  public void testProto3ConvertRepetition() {
    String expectedSchema = JOINER.join(
        "message TestProto3.SchemaConverterRepetition {",
        "  optional int32 optionalPrimitive = 1;",
        "  optional group repeatedPrimitive (LIST) = 3 {",
        "    repeated group list {",
        "      required int32 element;",
        "    }",
        "  }",
        "  optional group optionalMessage = 7 {",
        "    optional int32 someId = 3;",
        "  }",
        "  optional group repeatedMessage (LIST) = 9 {",
        "    repeated group list {",
        "      optional group element {",
        "        optional int32 someId = 3;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProto3.SchemaConverterRepetition.class, expectedSchema);
  }

  @Test
  public void testConvertRepeatedIntMessage() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.RepeatedIntMessage {",
        "  optional group repeatedInt (LIST) = 1 {",
        "    repeated group list {",
        "      required int32 element;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProtobuf.RepeatedIntMessage.class, expectedSchema);
  }

  @Test
  public void testConvertRepeatedIntMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.RepeatedIntMessage {",
        "  repeated int32 repeatedInt = 1;",
        "}");

    testConversion(TestProtobuf.RepeatedIntMessage.class, expectedSchema, false);
  }

  @Test
  public void testProto3ConvertRepeatedIntMessage() {
    String expectedSchema = JOINER.join(
        "message TestProto3.RepeatedIntMessage {",
        "  optional group repeatedInt (LIST) = 1 {",
        "    repeated group list {",
        "      required int32 element;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProto3.RepeatedIntMessage.class, expectedSchema);
  }

  @Test
  public void testProto3ConvertRepeatedIntMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProto3.RepeatedIntMessage {",
        "  repeated int32 repeatedInt = 1;",
        "}");

    testConversion(TestProto3.RepeatedIntMessage.class, expectedSchema, false);
  }

  @Test
  public void testConvertRepeatedInnerMessage() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.RepeatedInnerMessage {",
        "  optional group repeatedInnerMessage (LIST) = 1 {",
        "    repeated group list {",
        "      optional group element {",
        "        optional binary one (UTF8) = 1;",
        "        optional binary two (UTF8) = 2;",
        "        optional binary three (UTF8) = 3;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProtobuf.RepeatedInnerMessage.class, expectedSchema);
  }

  @Test
  public void testConvertRepeatedInnerMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.RepeatedInnerMessage {",
        "  repeated group repeatedInnerMessage = 1 {",
        "    optional binary one (UTF8) = 1;",
        "    optional binary two (UTF8) = 2;",
        "    optional binary three (UTF8) = 3;",
        "  }",
        "}");

    testConversion(TestProtobuf.RepeatedInnerMessage.class, expectedSchema, false);
  }

  @Test
  public void testProto3ConvertRepeatedInnerMessage() {
    String expectedSchema = JOINER.join(
        "message TestProto3.RepeatedInnerMessage {",
        "  optional group repeatedInnerMessage (LIST) = 1 {",
        "    repeated group list {",
        "      optional group element {",
        "        optional binary one (UTF8) = 1;",
        "        optional binary two (UTF8) = 2;",
        "        optional binary three (UTF8) = 3;",
        "      }",
        "    }",
        "  }",
        "}");

    testConversion(TestProto3.RepeatedInnerMessage.class, expectedSchema);
  }

  @Test
  public void testProto3ConvertRepeatedInnerMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProto3.RepeatedInnerMessage {",
        "  repeated group repeatedInnerMessage = 1 {",
        "    optional binary one (UTF8) = 1;",
        "    optional binary two (UTF8) = 2;",
        "    optional binary three (UTF8) = 3;",
        "  }",
        "}");

    testConversion(TestProto3.RepeatedInnerMessage.class, expectedSchema, false);
  }

  @Test
  public void testConvertMapIntMessage() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.MapIntMessage {",
        "  optional group mapInt (MAP) = 1 {",
        "    repeated group key_value {",
        "      required int32 key;",
        "      optional int32 value;",
        "    }",
        "  }",
        "}");

    testConversion(TestProtobuf.MapIntMessage.class, expectedSchema);
  }

  @Test
  public void testConvertMapIntMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProtobuf.MapIntMessage {",
        "  repeated group mapInt = 1 {",
        "    optional int32 key = 1;",
        "    optional int32 value = 2;",
        "  }",
        "}");

    testConversion(TestProtobuf.MapIntMessage.class, expectedSchema, false);
  }

  @Test
  public void testProto3ConvertMapIntMessage() {
    String expectedSchema = JOINER.join(
        "message TestProto3.MapIntMessage {",
        "  optional group mapInt (MAP) = 1 {",
        "    repeated group key_value {",
        "      required int32 key;",
        "      optional int32 value;",
        "    }",
        "  }",
        "}");

    testConversion(TestProto3.MapIntMessage.class, expectedSchema);
  }

  @Test
  public void testProto3ConvertMapIntMessageNonSpecsCompliant() {
    String expectedSchema = JOINER.join(
        "message TestProto3.MapIntMessage {",
        "  repeated group mapInt = 1 {",
        "    optional int32 key = 1;",
        "    optional int32 value = 2;",
        "  }",
        "}");

    testConversion(TestProto3.MapIntMessage.class, expectedSchema, false);
  }

  @Test
  public void testBinaryTreeRecursion() throws Exception {
    String expectedSchema = JOINER.join(
        "message Trees.BinaryTree {",
        "  optional group value = 1 {",
        "    optional binary type_url (STRING) = 1;",
        "    optional binary value = 2;",
        "  }",
        "  optional group left = 2 {",
        "    optional group value = 1 {",
        "      optional binary type_url (STRING) = 1;",
        "      optional binary value = 2;",
        "    }",
        "    optional binary left = 2;",
        "    optional binary right = 3;",
        "  }",
        "  optional group right = 3 {",
        "    optional group value = 1 {",
        "      optional binary type_url (STRING) = 1;",
        "      optional binary value = 2;",
        "    }",
        "    optional binary left = 2;",
        "    optional binary right = 3;",
        "  }",
        "}");
    testConversion(Trees.BinaryTree.class, expectedSchema, new ProtoSchemaConverter(true, 1));
    testConversion(Trees.BinaryTree.class, TestUtils.readResource("BinaryTree.par"), new ProtoSchemaConverter(true, PAR_RECURSION_DEPTH));

  }

  @Test
  public void testWideTreeRecursion() throws Exception {
    String expectedSchema = JOINER.join(
        "message Trees.WideTree {",
        "  optional group value = 1 {",
        "    optional binary type_url (STRING) = 1;",
        "    optional binary value = 2;",
        "  }",
        "  optional group children (LIST) = 2 {",
        "    repeated group list {",
        "      optional group element {",
        "        optional group value = 1 {",
        "          optional binary type_url (STRING) = 1;",
        "          optional binary value = 2;",
        "        }",
        "        optional binary children = 2;",
        "      }",
        "    }",
        "  }",
        "}");
    testConversion(Trees.WideTree.class, expectedSchema, new ProtoSchemaConverter(true, 1));
    testConversion(Trees.WideTree.class, TestUtils.readResource("WideTree.par"), new ProtoSchemaConverter(true, PAR_RECURSION_DEPTH));

  }

  @Test
  public void testValueRecursion() throws Exception {
    String expectedSchema = JOINER.join(
        "message google.protobuf.Value {",
        "  optional binary null_value (ENUM) = 1;",
        "  optional double number_value = 2;",
        "  optional binary string_value (STRING) = 3;",
        "  optional boolean bool_value = 4;",
        "  optional group struct_value = 5 {",
        "    optional group fields (MAP) = 1 {",
        "      repeated group key_value {",
        "        required binary key (STRING);",
        "        optional group value {",
        "          optional binary null_value (ENUM) = 1;",
        "          optional double number_value = 2;",
        "          optional binary string_value (STRING) = 3;",
        "          optional boolean bool_value = 4;",
        "          optional group struct_value = 5 {",
        "            optional binary fields = 1;",
        "          }",
        "          optional group list_value = 6 {",
        "            optional binary values = 1;",
        "          }",
        "        }",
        "      }",
        "    }",
        "  }",
        "  optional group list_value = 6 {",
        "    optional group values (LIST) = 1 {",
        "      repeated group list {",
        "        optional group element {",
        "          optional binary null_value (ENUM) = 1;",
        "          optional double number_value = 2;",
        "          optional binary string_value (STRING) = 3;",
        "          optional boolean bool_value = 4;",
        "          optional group struct_value = 5 {",
        "            optional binary fields = 1;",
        "          }",
        "          optional group list_value = 6 {",
        "            optional binary values = 1;",
        "          }",
        "        }",
        "      }",
        "    }",
        "  }",
        "}");
    testConversion(Value.class, expectedSchema, new ProtoSchemaConverter(true, 1));
    testConversion(Value.class, TestUtils.readResource("Value.par"), new ProtoSchemaConverter(true, PAR_RECURSION_DEPTH));
  }

  @Test
  public void testStructRecursion() throws Exception {
    String expectedSchema = JOINER.join(
        "message google.protobuf.Struct {",
        "  optional group fields (MAP) = 1 {",
        "    repeated group key_value {",
        "      required binary key (STRING);",
        "      optional group value {",
        "        optional binary null_value (ENUM) = 1;",
        "        optional double number_value = 2;",
        "        optional binary string_value (STRING) = 3;",
        "        optional boolean bool_value = 4;",
        "        optional group struct_value = 5 {",
        "          optional group fields (MAP) = 1 {",
        "            repeated group key_value {",
        "              required binary key (STRING);",
        "              optional group value {",
        "                optional binary null_value (ENUM) = 1;",
        "                optional double number_value = 2;",
        "                optional binary string_value (STRING) = 3;",
        "                optional boolean bool_value = 4;",
        "                optional binary struct_value = 5;",
        "                optional group list_value = 6 {",
        "                  optional binary values = 1;",
        "                }",
        "              }",
        "            }",
        "          }",
        "        }",
        "        optional group list_value = 6 {",
        "          optional group values (LIST) = 1 {",
        "            repeated group list {",
        "              optional group element {",
        "                optional binary null_value (ENUM) = 1;",
        "                optional double number_value = 2;",
        "                optional binary string_value (STRING) = 3;",
        "                optional boolean bool_value = 4;",
        "                optional group struct_value = 5 {",
        "                  optional binary fields = 1;",
        "                }",
        "                optional group list_value = 6 {",
        "                  optional binary values = 1;",
        "                }",
        "              }",
        "            }",
        "          }",
        "        }",
        "      }",
        "    }",
        "  }",
        "}");
    testConversion(Struct.class, expectedSchema, new ProtoSchemaConverter(true, 1));
    testConversion(Struct.class, TestUtils.readResource("Struct.par"), new ProtoSchemaConverter(true, PAR_RECURSION_DEPTH));
  }

  @Test
  public void testDeepRecursion() {
    // The general idea is to test the fanout of the schema.
    // TODO: figured out closed forms of the binary tree and struct series.
    long expectedBinaryTreeSize = 4;
    long expectedStructSize = 7;
    for (int i = 0; i < 10; ++i) {
      MessageType deepSchema = new ProtoSchemaConverter(true, i).convert(Trees.WideTree.class);
      // 3, 5, 7, 9, 11, 13, 15, 17, 19, 21
      assertEquals(2 * i + 3, deepSchema.getPaths().size());

      deepSchema = new ProtoSchemaConverter(true, i).convert(Trees.BinaryTree.class);
      // 4, 10, 22, 46, 94, 190, 382, 766, 1534, 3070
      assertEquals(expectedBinaryTreeSize, deepSchema.getPaths().size());
      expectedBinaryTreeSize = 2 * expectedBinaryTreeSize + 2;

      deepSchema = new ProtoSchemaConverter(true, i).convert(Struct.class);
      // 7, 18, 40, 84, 172, 348, 700, 1404, 2812, 5628
      assertEquals(expectedStructSize, deepSchema.getPaths().size());
      expectedStructSize = 2 * expectedStructSize + 4;
    }
  }
}
