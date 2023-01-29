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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.twitter.elephantbird.util.Protobufs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class TestUtils {

  public static Path someTemporaryFilePath() throws IOException {
    File tmp = File.createTempFile("ParquetProtobuf_unitTest", ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return new Path(tmp.getPath());
  }

  public static <T extends MessageOrBuilder> List<T> writeAndRead(T... records) throws IOException {
    Path file = writeMessages(records);

    return readMessages(file);
  }

  public static Class<? extends Message> inferRecordsClass(MessageOrBuilder[] records) {
    Class<? extends Message> cls = null;

    for (MessageOrBuilder record : records) {
      Class<? extends Message> recordClass;
      if (record instanceof Message.Builder) {
        recordClass = ((Message.Builder) record).build().getClass();
      } else if (record instanceof Message) {
        recordClass = ((Message) record).getClass();
      } else {
        throw new RuntimeException("Illegal class " + record);
      }

      if (cls == null) {
        cls = recordClass;
      } else if (!cls.equals(recordClass)) {
        throw new RuntimeException("Class mismatch :" + cls + " and " + recordClass);
      }
    }
    return cls;
  }

  /**
   * Writes messages to file, reads messages from file and checks if everything is OK.
   */
  public static <T extends  MessageOrBuilder> List<T> testData(T... messages) throws IOException {

    checkSameBuilderInstance(messages);

    List<MessageOrBuilder> output = writeAndRead(messages);
    List<Message> outputAsMessages = asMessages(output);
    Descriptors.Descriptor messageDescriptor = Protobufs.getMessageDescriptor(asMessage(messages[0]).getClass());
    Descriptors.FileDescriptor.Syntax syntax = messageDescriptor.getFile().getSyntax();
    for (int i = 0 ; i < messages.length ; i++) {
      if (Descriptors.FileDescriptor.Syntax.PROTO2.equals(syntax)) {
        com.google.common.truth.extensions.proto.ProtoTruth.assertThat(outputAsMessages.get(i))
          .ignoringRepeatedFieldOrder()
          .reportingMismatchesOnly()
          .isEqualTo(asMessage(messages[i]));
      } else if (Descriptors.FileDescriptor.Syntax.PROTO3.equals(syntax)) {
        // proto3 will return default values for absent fields which is what is returned in output
        // this is why we can ignore absent fields here
        com.google.common.truth.extensions.proto.ProtoTruth.assertThat(outputAsMessages.get(i))
          .ignoringRepeatedFieldOrder()
          .ignoringFieldAbsence()
          .reportingMismatchesOnly()
          .isEqualTo(asMessage(messages[i]));
      }
    }
    return (List<T>) outputAsMessages;
  }

  private static List<MessageOrBuilder> cloneList(MessageOrBuilder[] messages) {
    List<MessageOrBuilder> result = new ArrayList<MessageOrBuilder>();

    for (MessageOrBuilder mob : messages) {
      result.add(asMessage(mob));
    }

    return result;
  }

  public static List<Message> asMessages(List<MessageOrBuilder> mobs) {
    List<Message> result = new ArrayList<Message>();
    for (MessageOrBuilder messageOrBuilder : mobs) {
      result.add(asMessage(messageOrBuilder));
    }
    return result;
  }

  /**
   * Given message or builder returns same data as message
   */
  public static Message asMessage(MessageOrBuilder mob) {
    Message message;
    if (mob instanceof Message.Builder) {
      message = ((Message.Builder) mob).build();
    } else {
      message = (Message) mob;
    }
    return message;
  }

  /**
   * Fails if some instance of builder is two times in list.
   */
  private static void checkSameBuilderInstance(MessageOrBuilder[] messages) {
    for (int i = 0; i < messages.length; i++) {
      MessageOrBuilder firstMessage = messages[i];
      boolean isBuilder = firstMessage instanceof Message.Builder;

      if (isBuilder) {
        for (int j = 0; j < messages.length; j++) {
          MessageOrBuilder secondMessage = messages[j];

          if (i != j) {
            boolean isSame = secondMessage == firstMessage;
            if (isSame) {
              fail("Data contains two references to same instance." + secondMessage);
            }
          }
        }
      }
    }
  }

  /**
   * Reads messages from given file. The file could/should be created by method writeMessages
   */
  public static <T extends MessageOrBuilder> List<T> readMessages(Path file) throws IOException {
    return readMessages(file, null);
  }

  /**
   * Read messages from given file into the expected proto class.
   * @param file
   * @param messageClass
   * @param <T>
   * @return List of protobuf messages for the given type.
   */
  public static <T extends MessageOrBuilder> List<T> readMessages(Path file, Class<T> messageClass) throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    ParquetReader.Builder readerBuilder = ProtoParquetReader.builder(inputFile);
    if (messageClass != null) {
      readerBuilder.set(ProtoReadSupport.PB_CLASS, messageClass.getName()).build();
    }
    try (ParquetReader reader = readerBuilder.build()) {
      List<T> result = new ArrayList<T>();
      boolean hasNext = true;
      while (hasNext) {
        T item = (T) reader.read();
        if (item == null) {
          hasNext = false;
        } else {
          result.add((T) asMessage(item));
        }
      }
      return result;
    }
  }

  /**
   * Writes messages to temporary file and returns its path.
   */
  public static Path writeMessages(MessageOrBuilder... records) throws IOException {
    Path file = someTemporaryFilePath();
    Class<? extends Message> cls = inferRecordsClass(records);

    ParquetWriter<MessageOrBuilder> writer =
      ProtoParquetWriter.<MessageOrBuilder>builder(file).withMessage(cls).build();

    for (MessageOrBuilder record : records) {
      writer.write(record);
    }

    writer.close();

    return file;
  }

  public static String readResource(final String filename) throws IOException {
    return Resources.toString(Resources.getResource(filename), Charsets.UTF_8);
  }
}
