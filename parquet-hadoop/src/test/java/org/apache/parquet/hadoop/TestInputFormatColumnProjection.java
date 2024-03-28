/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.hadoop;

import static java.lang.Thread.sleep;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Parameterized on Vectored IO enabled/disabled so can verify that
 * ranged reads work through the bridge on compatible hadoop versions.
 */
@RunWith(Parameterized.class)
public class TestInputFormatColumnProjection {
  public static final String FILE_CONTENT = "" + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,"
      + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,"
      + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static MessageType PARQUET_TYPE = Types.buildMessage()
      .required(BINARY)
      .as(UTF8)
      .named("uuid")
      .required(BINARY)
      .as(UTF8)
      .named("char")
      .named("FormatTestObject");

  public static class Writer extends Mapper<LongWritable, Text, Void, Group> {
    public static final SimpleGroupFactory GROUP_FACTORY = new SimpleGroupFactory(PARQUET_TYPE);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // writes each character of the line with a UUID
      String line = value.toString();
      for (int i = 0; i < line.length(); i += 1) {
        Group group = GROUP_FACTORY.newGroup();
        group.add(0, Binary.fromString(UUID.randomUUID().toString()));
        group.add(1, Binary.fromString(line.substring(i, i + 1)));
        context.write(null, group);
      }
    }
  }

  public static class Reader extends Mapper<Void, Group, LongWritable, Text> {

    public static Counter bytesReadCounter = null;

    public static void setBytesReadCounter(Counter bytesRead) {
      bytesReadCounter = bytesRead;
    }

    @Override
    protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
      // Do nothing. The test uses Hadoop FS counters for verification.
      setBytesReadCounter(ContextUtil.getCounter(context, "parquet", "bytesread"));
    }
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "vectored : {0}")
  public static List<Boolean> params() {
    return Arrays.asList(true, false);
  }

  /**
   * Read type: true for vectored IO.
   */
  private final boolean readType;

  public TestInputFormatColumnProjection(boolean readType) {
    this.readType = readType;
  }

  @Test
  public void testProjectionSize() throws Exception {
    Assume.assumeTrue( // only run this test for Hadoop 2
        org.apache.hadoop.mapreduce.JobContext.class.isInterface());

    File inputFile = temp.newFile();
    FileOutputStream out = new FileOutputStream(inputFile);
    out.write(FILE_CONTENT.getBytes("UTF-8"));
    out.close();

    File tempFolder = temp.newFolder();
    tempFolder.delete();
    Path tempPath = new Path(tempFolder.toURI());

    File outputFolder = temp.newFile();
    outputFolder.delete();

    Configuration conf = new Configuration();
    // set the vector IO option
    conf.setBoolean(ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, readType);
    // set the projection schema
    conf.set(
        "parquet.read.schema",
        Types.buildMessage()
            .required(BINARY)
            .as(UTF8)
            .named("char")
            .named("FormatTestObject")
            .toString());

    // disable summary metadata, it isn't needed
    conf.set("parquet.enable.summary-metadata", "false");
    conf.set("parquet.example.schema", PARQUET_TYPE.toString());

    {
      Job writeJob = new Job(conf, "write");
      writeJob.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.addInputPath(writeJob, new Path(inputFile.toString()));

      writeJob.setOutputFormatClass(ExampleOutputFormat.class);
      writeJob.setMapperClass(Writer.class);
      writeJob.setNumReduceTasks(0); // write directly to Parquet without reduce
      ParquetOutputFormat.setBlockSize(writeJob, 10240);
      ParquetOutputFormat.setPageSize(writeJob, 512);
      ParquetOutputFormat.setDictionaryPageSize(writeJob, 1024);
      ParquetOutputFormat.setEnableDictionary(writeJob, true);
      ParquetOutputFormat.setMaxPaddingSize(writeJob, 1023); // always pad
      ParquetOutputFormat.setOutputPath(writeJob, tempPath);

      waitForJob(writeJob);
    }

    long bytesWritten = 0;
    FileSystem fs = FileSystem.getLocal(conf);
    for (FileStatus file : fs.listStatus(tempPath)) {
      bytesWritten += file.getLen();
    }

    long bytesRead;
    {
      Job readJob = new Job(conf, "read");
      readJob.setInputFormatClass(ExampleInputFormat.class);
      TextInputFormat.addInputPath(readJob, tempPath);

      readJob.setOutputFormatClass(TextOutputFormat.class);
      readJob.setMapperClass(Reader.class);
      readJob.setNumReduceTasks(0); // no reduce phase
      TextOutputFormat.setOutputPath(readJob, new Path(outputFolder.toString()));

      waitForJob(readJob);

      bytesRead = Reader.bytesReadCounter.getValue();
    }

    Assert.assertTrue(
        "Should read (" + bytesRead + " bytes)" + " less than 10% of the input file size (" + bytesWritten
            + ")",
        bytesRead < (bytesWritten / 10));
  }

  private void waitForJob(Job job) throws Exception {
    job.submit();
    while (!job.isComplete()) {
      sleep(100);
    }
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }
}
