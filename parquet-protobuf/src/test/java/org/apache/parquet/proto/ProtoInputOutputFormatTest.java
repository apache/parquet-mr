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

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.apache.parquet.proto.test.TestProtobuf.FirstCustomClassMessage;
import org.apache.parquet.proto.test.TestProtobuf.SecondCustomClassMessage;
import org.apache.parquet.proto.utils.ReadUsingMR;
import org.apache.parquet.proto.utils.WriteUsingMR;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ProtoInputOutputFormatTest {

  /**
   * Writes Protocol Buffer using first MR job, reads written file using
   * second job and compares input and output.
   */
  @Test
  public void testInputOutput() throws Exception {
    TestProtobuf.IOFormatMessage input;
    {
      TestProtobuf.IOFormatMessage.Builder msg = TestProtobuf.IOFormatMessage.newBuilder();
      msg.setOptionalDouble(666);
      msg.addRepeatedString("Msg1");
      msg.addRepeatedString("Msg2");
      msg.getMsgBuilder().setSomeId(323);
      input = msg.build();
    }

    List<Message> result = runMRJobs(input);

    assertEquals(1, result.size());
    TestProtobuf.IOFormatMessage output = (TestProtobuf.IOFormatMessage) result.get(0);

    assertEquals(666, output.getOptionalDouble(), 0.00001);
    assertEquals(323, output.getMsg().getSomeId());
    assertEquals("Msg1", output.getRepeatedString(0));
    assertEquals("Msg2", output.getRepeatedString(1));

    assertEquals(input, output);
  }

  @Test
  public void testProto3InputOutput() throws Exception {
    TestProto3.IOFormatMessage input;
    {
      TestProto3.IOFormatMessage.Builder msg = TestProto3.IOFormatMessage.newBuilder();
      msg.setOptionalDouble(666);
      msg.addRepeatedString("Msg1");
      msg.addRepeatedString("Msg2");
      msg.getMsgBuilder().setSomeId(323);
      input = msg.build();
    }

    List<Message> result = runMRJobs(input);

    assertEquals(1, result.size());
    TestProto3.IOFormatMessage output = (TestProto3.IOFormatMessage) result.get(0);

    assertEquals(666, output.getOptionalDouble(), 0.00001);
    assertEquals(323, output.getMsg().getSomeId());
    assertEquals("Msg1", output.getRepeatedString(0));
    assertEquals("Msg2", output.getRepeatedString(1));

    assertEquals(input, output);
  }


  /**
   * Writes data to file then reads them again with projection.
   * Only requested data should be read.
   * */
  @Test
  public void testProjection() throws Exception {

    TestProtobuf.Document.Builder writtenDocument = TestProtobuf.Document.newBuilder();
    writtenDocument.setDocId(12345);
    writtenDocument.addNameBuilder().setUrl("http://goout.cz/");

    Path outputPath = new WriteUsingMR().write(writtenDocument.build());

    //lets prepare reading with schema
    ReadUsingMR reader = new ReadUsingMR();

    String projection = "message Document {required int64 DocId; }";
    reader.setRequestedProjection(projection);
    List<Message> output = reader.read(outputPath);
    TestProtobuf.Document readDocument = (TestProtobuf.Document) output.get(0);


    //test that only requested fields were deserialized
    assertTrue(readDocument.hasDocId());
    assertTrue("Found data outside projection.", readDocument.getNameCount() == 0);
  }

  @Test
  public void testProto3Projection() throws Exception {

    TestProto3.Document.Builder writtenDocument = TestProto3.Document.newBuilder();
    writtenDocument.setDocId(12345);
    writtenDocument.addNameBuilder().setUrl("http://goout.cz/");

    Path outputPath = new WriteUsingMR().write(writtenDocument.build());

    //lets prepare reading with schema
    ReadUsingMR reader = new ReadUsingMR();

    String projection = "message Document {optional int64 DocId; }";
    reader.setRequestedProjection(projection);
    List<Message> output = reader.read(outputPath);
    TestProto3.Document readDocument = (TestProto3.Document) output.get(0);


    //test that only requested fields were deserialized
    assertTrue(readDocument.getDocId() == 12345);
    assertTrue(readDocument.getNameCount() == 0);
    assertTrue("Found data outside projection.", readDocument.getNameCount() == 0);
  }

  /**
   * When user specified protobuffer class in configuration,
   * It should replace class specified in header.
   * */
  @Test
  public void testCustomProtoClass() throws Exception {
    FirstCustomClassMessage.Builder inputMessage;
    inputMessage = FirstCustomClassMessage.newBuilder();
    inputMessage.setString("writtenString");

    Path outputPath = new WriteUsingMR().write(new Message[]{inputMessage.build()});
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = SecondCustomClassMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    Message msg = result.get(0);
    assertFalse("Class from header returned.",
            msg instanceof FirstCustomClassMessage);
    assertTrue("Custom class was not used",
            msg instanceof SecondCustomClassMessage);

    String stringValue;
    stringValue = ((SecondCustomClassMessage) msg).getString();
    assertEquals("writtenString", stringValue);
  }

  @Test
  public void testProto3CustomProtoClass() throws Exception {
    TestProto3.FirstCustomClassMessage.Builder inputMessage;
    inputMessage = TestProto3.FirstCustomClassMessage.newBuilder();
    inputMessage.setString("writtenString");

    Path outputPath = new WriteUsingMR().write(new Message[]{inputMessage.build()});
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.SecondCustomClassMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    Message msg = result.get(0);
    assertFalse("Class from header returned.",
      msg instanceof TestProto3.FirstCustomClassMessage);
    assertTrue("Custom class was not used",
      msg instanceof TestProto3.SecondCustomClassMessage);

    String stringValue;
    stringValue = ((TestProto3.SecondCustomClassMessage) msg).getString();
    assertEquals("writtenString", stringValue);
  }

  @Test
  public void testRepeatedIntMessageClass() throws Exception {
    TestProtobuf.RepeatedIntMessage msgEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().build();
    TestProtobuf.RepeatedIntMessage msgNonEmpty = TestProtobuf.RepeatedIntMessage.newBuilder()
      .addRepeatedInt(1).addRepeatedInt(2)
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3RepeatedIntMessageClass() throws Exception {
    TestProto3.RepeatedIntMessage msgEmpty = TestProto3.RepeatedIntMessage.newBuilder().build();
    TestProto3.RepeatedIntMessage msgNonEmpty = TestProto3.RepeatedIntMessage.newBuilder()
      .addRepeatedInt(1).addRepeatedInt(2)
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedIntMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.RepeatedIntMessage msgEmpty = TestProtobuf.RepeatedIntMessage.newBuilder().build();
    TestProtobuf.RepeatedIntMessage msgNonEmpty = TestProtobuf.RepeatedIntMessage.newBuilder()
      .addRepeatedInt(1).addRepeatedInt(2)
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3RepeatedIntMessageClassSchemaCompliant() throws Exception {
    TestProto3.RepeatedIntMessage msgEmpty = TestProto3.RepeatedIntMessage.newBuilder().build();
    TestProto3.RepeatedIntMessage msgNonEmpty = TestProto3.RepeatedIntMessage.newBuilder()
      .addRepeatedInt(1).addRepeatedInt(2)
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.RepeatedIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testMapIntMessageClass() throws Exception {
    TestProtobuf.MapIntMessage msgEmpty = TestProtobuf.MapIntMessage.newBuilder().build();
    TestProtobuf.MapIntMessage msgNonEmpty = TestProtobuf.MapIntMessage.newBuilder()
      .putMapInt(1, 123).putMapInt(2, 234)
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3MapIntMessageClass() throws Exception {
    TestProto3.MapIntMessage msgEmpty = TestProto3.MapIntMessage.newBuilder().build();
    TestProto3.MapIntMessage msgNonEmpty = TestProto3.MapIntMessage.newBuilder()
      .putMapInt(1, 123).putMapInt(2, 234)
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testMapIntMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.MapIntMessage msgEmpty = TestProtobuf.MapIntMessage.newBuilder().build();
    TestProtobuf.MapIntMessage msgNonEmpty = TestProtobuf.MapIntMessage.newBuilder()
      .putMapInt(1, 123).putMapInt(2, 234)
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProtobuf.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3MapIntMessageClassSchemaCompliant() throws Exception {
    TestProto3.MapIntMessage msgEmpty = TestProto3.MapIntMessage.newBuilder().build();
    TestProto3.MapIntMessage msgNonEmpty = TestProto3.MapIntMessage.newBuilder()
      .putMapInt(1, 123).putMapInt(2, 234)
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.MapIntMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedInnerMessageClass() throws Exception {
    TestProtobuf.RepeatedInnerMessage msgEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder().build();
    TestProtobuf.RepeatedInnerMessage msgNonEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder()
      .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setOne("one").build())
      .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setTwo("two").build())
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProtobuf.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3RepeatedInnerMessageClass() throws Exception {
    TestProto3.RepeatedInnerMessage msgEmpty = TestProto3.RepeatedInnerMessage.newBuilder().build();
    TestProto3.RepeatedInnerMessage msgNonEmpty = TestProto3.RepeatedInnerMessage.newBuilder()
      .addRepeatedInnerMessage(TestProto3.InnerMessage.newBuilder().setOne("one").build())
      .addRepeatedInnerMessage(TestProto3.InnerMessage.newBuilder().setTwo("two").build())
      .build();

    Path outputPath = new WriteUsingMR().write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testRepeatedInnerMessageClassSchemaCompliant() throws Exception {
    TestProtobuf.RepeatedInnerMessage msgEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder().build();
    TestProtobuf.RepeatedInnerMessage msgNonEmpty = TestProtobuf.RepeatedInnerMessage.newBuilder()
      .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setOne("one").build())
      .addRepeatedInnerMessage(TestProtobuf.InnerMessage.newBuilder().setTwo("two").build())
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProtobuf.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3RepeatedInnerMessageClassSchemaCompliant() throws Exception {
    TestProto3.RepeatedInnerMessage msgEmpty = TestProto3.RepeatedInnerMessage.newBuilder().build();
    TestProto3.RepeatedInnerMessage msgNonEmpty = TestProto3.RepeatedInnerMessage.newBuilder()
      .addRepeatedInnerMessage(TestProto3.InnerMessage.newBuilder().setOne("one").build())
      .addRepeatedInnerMessage(TestProto3.InnerMessage.newBuilder().setTwo("two").build())
      .build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.RepeatedInnerMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3Defaults() throws Exception {
    TestProto3.SchemaConverterAllDatatypes msgEmpty = TestProto3.SchemaConverterAllDatatypes.newBuilder().build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(msgEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.SchemaConverterAllDatatypes.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    //assertEquals(msgEmpty, result.get(0));
    // proto3 will return default values for absent fields which is what is returned in output
    // this is why we can ignore absent fields here as optionalMessage and optionalMap will get default value
    com.google.common.truth.extensions.proto.ProtoTruth.assertThat(result.get(0))
      .ignoringRepeatedFieldOrder()
      .ignoringFieldAbsence()
      .reportingMismatchesOnly()
      .isEqualTo(msgEmpty);
  }

  @Test
  public void testProto3AllTypes() throws Exception {
    TestProto3.SchemaConverterAllDatatypes.Builder data;
    data = TestProto3.SchemaConverterAllDatatypes.newBuilder();

    data.setOptionalBool(true);
    data.setOptionalBytes(ByteString.copyFrom("someText", "UTF-8"));
    data.setOptionalDouble(0.577);
    data.setOptionalFloat(3.1415f);
    data.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
    data.setOptionalFixed32(1000 * 1000 * 1);
    data.setOptionalFixed64(1000 * 1000 * 1000 * 2);
    data.setOptionalInt32(1000 * 1000 * 3);
    data.setOptionalInt64(1000L * 1000 * 1000 * 4);
    data.setOptionalSFixed32(1000 * 1000 * 5);
    data.setOptionalSFixed64(1000L * 1000 * 1000 * 6);
    data.setOptionalSInt32(1000 * 1000 * 56);
    data.setOptionalSInt64(1000L * 1000 * 1000 * 7);
    data.setOptionalString("Good Will Hunting");
    data.setOptionalUInt32(1000 * 1000 * 8);
    data.setOptionalUInt64(1000L * 1000 * 1000 * 9);
    data.getOptionalMessageBuilder().setSomeId(1984);

    TestProto3.SchemaConverterAllDatatypes dataBuilt = data.build();

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(dataBuilt);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.SchemaConverterAllDatatypes.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(1, result.size());
    // proto3 will return default values for absent fields which is what is returned in output
    // this is why we can ignore absent fields here as optionalMap will get default value
    com.google.common.truth.extensions.proto.ProtoTruth.assertThat(result.get(0))
      .ignoringRepeatedFieldOrder()
      .ignoringFieldAbsence()
      .reportingMismatchesOnly()
      .isEqualTo(dataBuilt);

    TestProto3.SchemaConverterAllDatatypes o = (TestProto3.SchemaConverterAllDatatypes) result.get(0);
    assertEquals("Good Will Hunting", o.getOptionalString());
    assertEquals(true, o.getOptionalBool());
    assertEquals(ByteString.copyFrom("someText", "UTF-8"), o.getOptionalBytes());
    assertEquals(0.577, o.getOptionalDouble(), 0.00001);
    assertEquals(3.1415f, o.getOptionalFloat(), 0.00001);
    assertEquals(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST, o.getOptionalEnum());
    assertEquals(1000 * 1000 * 1, o.getOptionalFixed32());
    assertEquals(1000 * 1000 * 1000 * 2, o.getOptionalFixed64());
    assertEquals(1000 * 1000 * 3, o.getOptionalInt32());
    assertEquals(1000L * 1000 * 1000 * 4, o.getOptionalInt64());
    assertEquals(1000 * 1000 * 5, o.getOptionalSFixed32());
    assertEquals(1000L * 1000 * 1000 * 6, o.getOptionalSFixed64());
    assertEquals(1000 * 1000 * 56, o.getOptionalSInt32());
    assertEquals(1000L * 1000 * 1000 * 7, o.getOptionalSInt64());
    assertEquals(1000 * 1000 * 8, o.getOptionalUInt32());
    assertEquals(1000L * 1000 * 1000 * 9, o.getOptionalUInt64());
    assertEquals(1984, o.getOptionalMessage().getSomeId());
  }

  @Test
  public void testProto3AllTypesMultiple() throws Exception {
    int count = 100;
    TestProto3.SchemaConverterAllDatatypes[] input = new TestProto3.SchemaConverterAllDatatypes[count];

    for (int i = 0; i < count; i++) {
      TestProto3.SchemaConverterAllDatatypes.Builder d = TestProto3.SchemaConverterAllDatatypes.newBuilder();

      if (i % 2 != 0) d.setOptionalBool(true);
      if (i % 3 != 0) d.setOptionalBytes(ByteString.copyFrom("someText " + i, "UTF-8"));
      if (i % 4 != 0) d.setOptionalDouble(0.577 * i);
      if (i % 5 != 0) d.setOptionalFloat(3.1415f * i);
      if (i % 6 != 0) d.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
      if (i % 7 != 0) d.setOptionalFixed32(1000 * i * 1);
      if (i % 8 != 0) d.setOptionalFixed64(1000 * i * 1000 * 2);
      if (i % 9 != 0) d.setOptionalInt32(1000 * i * 3);
      if (i % 2 != 1) d.setOptionalSFixed32(1000 * i * 5);
      if (i % 3 != 1) d.setOptionalSFixed64(1000 * i * 1000 * 6);
      if (i % 4 != 1) d.setOptionalSInt32(1000 * i * 56);
      if (i % 5 != 1) d.setOptionalSInt64(1000 * i * 1000 * 7);
      if (i % 6 != 1) d.setOptionalString("Good Will Hunting " + i);
      if (i % 7 != 1) d.setOptionalUInt32(1000 * i * 8);
      if (i % 8 != 1) d.setOptionalUInt64(1000 * i * 1000 * 9);
      if (i % 9 != 1) d.getOptionalMessageBuilder().setSomeId(1984 * i);
      if (i % 3 != 1) d.setOptionalInt64(1000 * i * 1000 * 4);
      input[i] = d.build();
    }

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(input);
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.SchemaConverterAllDatatypes.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(100, result.size());
    for (int i = 0; i < 100 ; i++) {
      // proto3 will return default values for absent fields which is what is returned in output
      // this is why we can ignore absent fields here
      com.google.common.truth.extensions.proto.ProtoTruth.assertThat(result.get(i))
        .ignoringRepeatedFieldOrder()
        .ignoringFieldAbsence()
        .reportingMismatchesOnly()
        .isEqualTo(input[i]);
    }
    assertEquals("Good Will Hunting 0", ((TestProto3.SchemaConverterAllDatatypes) result.get(0)).getOptionalString());
    assertEquals("Good Will Hunting 90", ((TestProto3.SchemaConverterAllDatatypes) result.get(90)).getOptionalString());
  }

  @Test
  public void testProto3RepeatedMessages() throws Exception {
    TestProto3.TopMessage.Builder top = TestProto3.TopMessage.newBuilder();
    top.addInnerBuilder().setOne("First inner");
    top.addInnerBuilder().setTwo("Second inner");
    top.addInnerBuilder().setThree("Third inner");

    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);

    Path outputPath = new WriteUsingMR(conf).write(top.build());
    ReadUsingMR readUsingMR = new ReadUsingMR(conf);
    String customClass = TestProto3.TopMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> messages = readUsingMR.read(outputPath);
    TestProto3.TopMessage result = (TestProto3.TopMessage) messages.get(0);

    assertEquals(3, result.getInnerCount());

    TestProto3.InnerMessage first = result.getInner(0);
    TestProto3.InnerMessage second = result.getInner(1);
    TestProto3.InnerMessage third = result.getInner(2);

    assertEquals("First inner", first.getOne());
    assertTrue(first.getTwo().isEmpty());
    assertTrue(first.getThree().isEmpty());

    assertEquals("Second inner", second.getTwo());
    assertTrue(second.getOne().isEmpty());
    assertTrue(second.getThree().isEmpty());

    assertEquals("Third inner", third.getThree());
    assertTrue(third.getOne().isEmpty());
    assertTrue(third.getTwo().isEmpty());
  }

  @Test
  public void testProto3TimestampMessageClass() throws Exception {
    Timestamp timestamp = Timestamps.parse("2021-05-02T15:04:03.748Z");
    TestProto3.DateTimeMessage msgEmpty = TestProto3.DateTimeMessage.newBuilder().build();
    TestProto3.DateTimeMessage msgNonEmpty = TestProto3.DateTimeMessage.newBuilder()
      .setTimestamp(timestamp)
      .build();

    Configuration conf = new Configuration();
    conf.setBoolean(ProtoWriteSupport.PB_UNWRAP_PROTO_WRAPPERS, true);
    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.DateTimeMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  @Test
  public void testProto3WrappedMessageClass() throws Exception {
    TestProto3.WrappedMessage msgEmpty = TestProto3.WrappedMessage.newBuilder().build();
    TestProto3.WrappedMessage msgNonEmpty = TestProto3.WrappedMessage.newBuilder()
      .setWrappedDouble(DoubleValue.of(0.577))
      .setWrappedBool(BoolValue.of(true))
      .build();


    Configuration conf = new Configuration();
    conf.setBoolean(ProtoWriteSupport.PB_UNWRAP_PROTO_WRAPPERS, true);
    Path outputPath = new WriteUsingMR(conf).write(msgEmpty, msgNonEmpty);
    ReadUsingMR readUsingMR = new ReadUsingMR();
    String customClass = TestProto3.WrappedMessage.class.getName();
    ProtoReadSupport.setProtobufClass(readUsingMR.getConfiguration(), customClass);
    List<Message> result = readUsingMR.read(outputPath);

    assertEquals(2, result.size());
    assertEquals(msgEmpty, result.get(0));
    assertEquals(msgNonEmpty, result.get(1));
  }

  /**
   * Runs job that writes input to file and then job reading data back.
   */
  public static List<Message> runMRJobs(Message... messages) throws Exception {
    Path outputPath = new WriteUsingMR().write(messages);
    List<Message> result = new ReadUsingMR().read(outputPath);
    return result;
  }
}
