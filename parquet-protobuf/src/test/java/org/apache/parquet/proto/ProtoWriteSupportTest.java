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
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.Timestamps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProtobuf;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoWriteSupportTest {

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock) {
    return createReadConsumerInstance(cls, readConsumerMock, new Configuration());
  }

  private <T extends Message> ProtoWriteSupport<T> createReadConsumerInstance(Class<T> cls, RecordConsumer readConsumerMock, Configuration conf) {
    ProtoWriteSupport support = new ProtoWriteSupport(cls);
    support.init(conf);
    support.prepareForWrite(readConsumerMock);
    return support;
  }

  @Test
  public void testSimplestMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.InnerMessage.class, readConsumerMock);

    TestProtobuf.InnerMessage.Builder msg = TestProtobuf.InnerMessage.newBuilder();
    msg.setOne("oneValue");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("oneValue"));
    inOrder.verify(readConsumerMock).endField("one", 0);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3SimplestMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.InnerMessage.class, readConsumerMock);

    TestProto3.InnerMessage.Builder msg = TestProto3.InnerMessage.newBuilder();
    msg.setOne("oneValue");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString("oneValue"));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString(""));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromString(""));
    inOrder.verify(readConsumerMock).endField("three", 2);

    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.RepeatedIntMessage.class, readConsumerMock);

    TestProtobuf.RepeatedIntMessage.Builder msg = TestProtobuf.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();
    msg.addRepeatedInt(1323);
    msg.addRepeatedInt(54469);

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).addInteger(1323);
    inOrder.verify(readConsumerMock).addInteger(54469);
    inOrder.verify(readConsumerMock).endField("repeatedInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock, conf);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.RepeatedIntMessage.class, readConsumerMock);

    TestProto3.RepeatedIntMessage.Builder msg = TestProto3.RepeatedIntMessage.newBuilder();

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock, conf);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key_value", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("key_value", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock, conf);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testMapIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MapIntMessage.class, readConsumerMock);

    TestProtobuf.MapIntMessage.Builder msg = TestProtobuf.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageSpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock, conf);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key_value", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("key_value", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    msg.putMapInt(123, 1);
    msg.putMapInt(234, 2);
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("mapInt", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(123);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(1);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("key", 0);
    inOrder.verify(readConsumerMock).addInteger(234);
    inOrder.verify(readConsumerMock).endField("key", 0);
    inOrder.verify(readConsumerMock).startField("value", 1);
    inOrder.verify(readConsumerMock).addInteger(2);
    inOrder.verify(readConsumerMock).endField("value", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("mapInt", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageEmptySpecsCompliant() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock, conf);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3MapIntMessageEmpty() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MapIntMessage.class, readConsumerMock);

    TestProto3.MapIntMessage.Builder msg = TestProto3.MapIntMessage.newBuilder();
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageSpecsCompliantMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock, conf);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);;
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageSpecsCompliantMessage_message() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock, conf);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one").setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);
    inOrder.verify(readConsumerMock).startGroup();

    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);

    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }


  @Test
  public void testRepeatedInnerMessageSpecsCompliantMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock, conf);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testRepeatedInnerMessageMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.TopMessage.class, readConsumerMock);

    TestProtobuf.TopMessage.Builder msg = TestProtobuf.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3RepeatedInnerMessageSpecsCompliantMessage_scalar() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.TopMessage.class, readConsumerMock, conf);

    TestProto3.TopMessage.Builder msg = TestProto3.TopMessage.newBuilder();
    msg.addInnerBuilder().setOne("one");
    msg.addInnerBuilder().setTwo("two");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("list", 0);

    //first inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    //second inner message
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("element", 0);
    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("two".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("element", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("list", 0);
    inOrder.verify(readConsumerMock).endGroup();
    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testOptionalInnerMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.MessageA.class, readConsumerMock);

    TestProtobuf.MessageA.Builder msg = TestProtobuf.MessageA.newBuilder();
    msg.getInnerBuilder().setOne("one");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3OptionalInnerMessage() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProto3.MessageA.class, readConsumerMock);

    TestProto3.MessageA.Builder msg = TestProto3.MessageA.newBuilder();
    msg.getInnerBuilder().setOne("one");

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("inner", 0);

    inOrder.verify(readConsumerMock).startGroup();
    inOrder.verify(readConsumerMock).startField("one", 0);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("one".getBytes()));
    inOrder.verify(readConsumerMock).endField("one", 0);
    inOrder.verify(readConsumerMock).startField("two", 1);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("two", 1);
    inOrder.verify(readConsumerMock).startField("three", 2);
    inOrder.verify(readConsumerMock).addBinary(Binary.fromConstantByteArray("".getBytes()));
    inOrder.verify(readConsumerMock).endField("three", 2);
    inOrder.verify(readConsumerMock).endGroup();

    inOrder.verify(readConsumerMock).endField("inner", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMessageWithExtensions() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport instance = createReadConsumerInstance(TestProtobuf.Vehicle.class, readConsumerMock);

    TestProtobuf.Vehicle.Builder msg = TestProtobuf.Vehicle.newBuilder();
    msg.setHorsePower(300);
    // Currently there's no support for extension fields. This test tests that the extension field
    // will cause an exception.
    msg.setExtension(TestProtobuf.Airplane.wingSpan, 50);

    instance.write(msg.build());
  }

  @Test
  public void testMessageOneOf() {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    ProtoWriteSupport<TestProto3.OneOfTestMessage> spyWriter = createReadConsumerInstance(TestProto3.OneOfTestMessage.class, readConsumerMock);
    final int theInt = 99;

    TestProto3.OneOfTestMessage.Builder msg = TestProto3.OneOfTestMessage.newBuilder();
    msg.setSecond(theInt);
    spyWriter.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("second", 1);
    inOrder.verify(readConsumerMock).addInteger(theInt);
    inOrder.verify(readConsumerMock).endField("second", 1);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  /**
   * Ensure that a message with a oneOf gets written out correctly and can be
   * read back as expected.
   */
  @Test
  public void testMessageOneOfRoundTrip() throws IOException {

    TestProto3.OneOfTestMessage.Builder msgBuilder = TestProto3.OneOfTestMessage.newBuilder();
    msgBuilder.setSecond(99);
    TestProto3.OneOfTestMessage theMessage = msgBuilder.build();

    TestProto3.OneOfTestMessage.Builder msgBuilder2 = TestProto3.OneOfTestMessage.newBuilder();
    TestProto3.OneOfTestMessage theMessageNothingSet = msgBuilder2.build();

    TestProto3.OneOfTestMessage.Builder msgBuilder3 = TestProto3.OneOfTestMessage.newBuilder();
    msgBuilder3.setFirst(42);
    TestProto3.OneOfTestMessage theMessageFirstSet = msgBuilder3.build();

    //Write them out and read them back
    Path tmpFilePath = TestUtils.writeMessages(theMessage, theMessageNothingSet, theMessageFirstSet);
    List<TestProto3.OneOfTestMessage> gotBack = TestUtils.readMessages(tmpFilePath, TestProto3.OneOfTestMessage.class);

    //First message
    TestProto3.OneOfTestMessage gotBackFirst = gotBack.get(0);
    assertEquals(gotBackFirst.getSecond(), 99);
    assertEquals(gotBackFirst.getTheOneofCase(), TestProto3.OneOfTestMessage.TheOneofCase.SECOND);

    //Second message with nothing set
    TestProto3.OneOfTestMessage gotBackSecond = gotBack.get(1);
    assertEquals(gotBackSecond.getTheOneofCase(), TestProto3.OneOfTestMessage.TheOneofCase.THEONEOF_NOT_SET);

    //Third message with opposite field set
    TestProto3.OneOfTestMessage gotBackThird = gotBack.get(2);
    assertEquals(gotBackThird.getFirst(), 42);
    assertEquals(gotBackThird.getTheOneofCase(), TestProto3.OneOfTestMessage.TheOneofCase.FIRST);

  }

  @Test
  public void testProto3DateTimeMessageUnwrapped() throws Exception {
    Timestamp timestamp = Timestamps.parse("2021-05-02T15:04:03.748Z");
    LocalDate date = LocalDate.of(2021, 5, 2);
    LocalTime time = LocalTime.of(15, 4, 3, 748_000_000);

    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setUnwrapProtoWrappers(conf, true);
    ProtoWriteSupport<TestProto3.DateTimeMessage> instance = createReadConsumerInstance(
      TestProto3.DateTimeMessage.class, readConsumerMock, conf);

    TestProto3.DateTimeMessage.Builder msg = TestProto3.DateTimeMessage.newBuilder();
    msg.setTimestamp(timestamp);
    msg.setDate(com.google.type.Date.newBuilder()
      .setYear(date.getYear())
      .setMonth(date.getMonthValue())
      .setDay(date.getDayOfMonth())
    );
    msg.setTime(com.google.type.TimeOfDay.newBuilder()
      .setHours(time.getHour())
      .setMinutes(time.getMinute())
      .setSeconds(time.getSecond())
      .setNanos(time.getNano())
    );
    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("timestamp", 0);
    inOrder.verify(readConsumerMock).addLong(Timestamps.toNanos(timestamp));
    inOrder.verify(readConsumerMock).endField("timestamp", 0);
    inOrder.verify(readConsumerMock).startField("date", 1);
    inOrder.verify(readConsumerMock).addInteger((int) date.toEpochDay());
    inOrder.verify(readConsumerMock).endField("date", 1);
    inOrder.verify(readConsumerMock).startField("time", 2);
    inOrder.verify(readConsumerMock).addLong(time.toNanoOfDay());
    inOrder.verify(readConsumerMock).endField("time", 2);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3DateTimeMessageRoundTrip() throws Exception {
    Timestamp timestamp = Timestamps.parse("2021-05-02T15:04:03.748Z");
    LocalDate date = LocalDate.of(2021, 5, 2);
    LocalTime time = LocalTime.of(15, 4, 3, 748_000_000);
    com.google.type.Date protoDate = com.google.type.Date.newBuilder()
      .setYear(date.getYear())
      .setMonth(date.getMonthValue())
      .setDay(date.getDayOfMonth())
      .build();
    com.google.type.TimeOfDay protoTime = com.google.type.TimeOfDay.newBuilder()
      .setHours(time.getHour())
      .setMinutes(time.getMinute())
      .setSeconds(time.getSecond())
      .setNanos(time.getNano())
      .build();

    TestProto3.DateTimeMessage msg = TestProto3.DateTimeMessage.newBuilder()
      .setTimestamp(timestamp)
      .setDate(protoDate)
      .setTime(protoTime)
      .build();

    //Write them out and read them back
    Path tmpFilePath = TestUtils.someTemporaryFilePath();
    ParquetWriter<MessageOrBuilder> writer =
      ProtoParquetWriter.<MessageOrBuilder>builder(tmpFilePath)
        .withMessage(TestProto3.DateTimeMessage.class)
        .config(ProtoWriteSupport.PB_UNWRAP_PROTO_WRAPPERS, "true")
        .build();
    writer.write(msg);
    writer.close();
    List<TestProto3.DateTimeMessage> gotBack = TestUtils.readMessages(tmpFilePath, TestProto3.DateTimeMessage.class);

    TestProto3.DateTimeMessage gotBackFirst = gotBack.get(0);
    assertEquals(timestamp, gotBackFirst.getTimestamp());
    assertEquals(protoDate, gotBackFirst.getDate());
    assertEquals(protoTime, gotBackFirst.getTime());
  }

  @Test
  public void testProto3WrappedMessageUnwrapped() throws Exception {
    RecordConsumer readConsumerMock =  Mockito.mock(RecordConsumer.class);
    Configuration conf = new Configuration();
    ProtoWriteSupport.setUnwrapProtoWrappers(conf, true);
    ProtoWriteSupport<TestProto3.WrappedMessage> instance = createReadConsumerInstance(
      TestProto3.WrappedMessage.class, readConsumerMock, conf);

    TestProto3.WrappedMessage.Builder msg = TestProto3.WrappedMessage.newBuilder();
    msg.setWrappedDouble(DoubleValue.of(3.1415));

    instance.write(msg.build());

    InOrder inOrder = Mockito.inOrder(readConsumerMock);

    inOrder.verify(readConsumerMock).startMessage();
    inOrder.verify(readConsumerMock).startField("wrappedDouble", 0);
    inOrder.verify(readConsumerMock).addDouble(3.1415);
    inOrder.verify(readConsumerMock).endField("wrappedDouble", 0);
    inOrder.verify(readConsumerMock).endMessage();
    Mockito.verifyNoMoreInteractions(readConsumerMock);
  }

  @Test
  public void testProto3WrappedMessageUnwrappedRoundTrip() throws Exception {
    TestProto3.WrappedMessage.Builder msg = TestProto3.WrappedMessage.newBuilder();
    msg.setWrappedDouble(DoubleValue.of(0.577));
    msg.setWrappedFloat(FloatValue.of(3.1415f));
    msg.setWrappedInt64(Int64Value.of(1_000_000_000L * 4));
    msg.setWrappedUInt64(UInt64Value.of(1_000_000_000L * 9));
    msg.setWrappedInt32(Int32Value.of(1_000_000 * 3));
    msg.setWrappedUInt32(UInt32Value.of(1_000_000 * 8));
    msg.setWrappedBool(BoolValue.of(true));
    msg.setWrappedString(StringValue.of("Good Will Hunting"));
    msg.setWrappedBytes(BytesValue.of(ByteString.copyFrom("someText", "UTF-8")));

    //Write them out and read them back
    Path tmpFilePath = TestUtils.someTemporaryFilePath();
    ParquetWriter<MessageOrBuilder> writer =
      ProtoParquetWriter.<MessageOrBuilder>builder(tmpFilePath)
        .withMessage(TestProto3.WrappedMessage.class)
        .config(ProtoWriteSupport.PB_UNWRAP_PROTO_WRAPPERS, "true")
        .build();
    writer.write(msg);
    writer.close();
    List<TestProto3.WrappedMessage> gotBack = TestUtils.readMessages(tmpFilePath, TestProto3.WrappedMessage.class);

    TestProto3.WrappedMessage gotBackFirst = gotBack.get(0);
    assertEquals(0.577, gotBackFirst.getWrappedDouble().getValue(), 1e-5);
    assertEquals(3.1415f, gotBackFirst.getWrappedFloat().getValue(), 1e-5f);
    assertEquals(1_000_000_000L * 4, gotBackFirst.getWrappedInt64().getValue());
    assertEquals(1_000_000_000L * 9, gotBackFirst.getWrappedUInt64().getValue());
    assertEquals(1_000_000 * 3, gotBackFirst.getWrappedInt32().getValue());
    assertEquals(1_000_000 * 8, gotBackFirst.getWrappedUInt32().getValue());
    assertEquals(BoolValue.of(true), gotBackFirst.getWrappedBool());
    assertEquals("Good Will Hunting", gotBackFirst.getWrappedString().getValue());
    assertEquals(ByteString.copyFrom("someText", "UTF-8"), gotBackFirst.getWrappedBytes().getValue());
  }

  @Test
  public void testProto3WrappedMessageWithNullsRoundTrip() throws Exception {
    TestProto3.WrappedMessage.Builder msg = TestProto3.WrappedMessage.newBuilder();
    msg.setWrappedFloat(FloatValue.of(3.1415f));
    msg.setWrappedString(StringValue.of("Good Will Hunting"));
    msg.setWrappedInt32(Int32Value.of(0));

    //Write them out and read them back
    Path tmpFilePath = TestUtils.someTemporaryFilePath();
    ParquetWriter<MessageOrBuilder> writer =
      ProtoParquetWriter.<MessageOrBuilder>builder(tmpFilePath)
        .withMessage(TestProto3.WrappedMessage.class)
        .config(ProtoWriteSupport.PB_UNWRAP_PROTO_WRAPPERS, "true")
        .build();
    writer.write(msg);
    writer.close();
    List<TestProto3.WrappedMessage> gotBack = TestUtils.readMessages(tmpFilePath, TestProto3.WrappedMessage.class);

    TestProto3.WrappedMessage gotBackFirst = gotBack.get(0);
    assertFalse(gotBackFirst.hasWrappedDouble());
    assertEquals(3.1415f, gotBackFirst.getWrappedFloat().getValue(), 1e-5f);

    // double-check that nulls are honored
    assertTrue(gotBackFirst.hasWrappedFloat());
    assertFalse(gotBackFirst.hasWrappedInt64());
    assertFalse(gotBackFirst.hasWrappedUInt64());
    assertTrue(gotBackFirst.hasWrappedInt32());
    assertFalse(gotBackFirst.hasWrappedUInt32());
    assertEquals(0, gotBackFirst.getWrappedUInt32().getValue());
    assertFalse(gotBackFirst.hasWrappedBool());
    assertEquals("Good Will Hunting", gotBackFirst.getWrappedString().getValue());
    assertFalse(gotBackFirst.hasWrappedBytes());
  }
}
