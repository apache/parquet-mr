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
package org.apache.parquet.hadoop.api;

import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestReadSupport {

  @Test
  public void testSuccessfulProjectionOptionalExtraField() {
    MessageType fileType = MessageTypeParser.parseMessageType(
      "message SampleClass {\n"
        + "required int32 x\n;"
        + "}");
    MessageType requestedProjection = MessageTypeParser.parseMessageType(
      "message SampleProjection {\n"
        + "required int32 x;\n"
        + "optional int32 extra;\n"
        + "}");

    MessageType schema = ReadSupport.getSchemaForRead(fileType, requestedProjection);
    assertEquals(schema, requestedProjection);
  }

  @Test(expected = InvalidRecordException.class)
  public void testProjectionFailureRequiredExtraField() {
    MessageType fileType = MessageTypeParser.parseMessageType(
      "message SampleClass {\n"
        + "required int32 x\n;"
        + "}");
    MessageType requestedProjection = MessageTypeParser.parseMessageType(
      "message SampleProjection {\n"
        + "required int32 x;\n"
        + "required int32 extra;\n"
        + "}");

    ReadSupport.getSchemaForRead(fileType, requestedProjection);
  }

  @Test
  public void testSuccessfulProjectionOfRequiredUsingOptional() {
    MessageType fileType = MessageTypeParser.parseMessageType(
      "message SampleClass {\n"
        + "required int32 x\n;"
        + "}");
    MessageType requestedProjection = MessageTypeParser.parseMessageType(
      "message SampleProjection {\n"
        + "optional int32 x;\n"
        + "}");

    MessageType schema = ReadSupport.getSchemaForRead(fileType, requestedProjection);
    assertEquals(schema, requestedProjection);
  }

  @Test(expected = InvalidRecordException.class)
  public void testProjectionFailureOfOptionalUsingRequired() {
    MessageType fileType = MessageTypeParser.parseMessageType(
      "message SampleClass {\n"
        + "optional int32 x\n;"
        + "}");
    MessageType requestedProjection = MessageTypeParser.parseMessageType(
      "message SampleProjection {\n"
        + "required int32 x;\n"
        + "}");

    ReadSupport.getSchemaForRead(fileType, requestedProjection);
  }
}
