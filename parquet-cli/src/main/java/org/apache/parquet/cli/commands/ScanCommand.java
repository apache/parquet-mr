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
package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Expressions;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Parameters(commandDescription = "Scan all records from a file")
public class ScanCommand extends BaseCommand {

  @Parameter(description = "<file>")
  String sourceFile;

  @Parameter(
    names = {"-c", "--column", "--columns"},
    description = "List of columns")
  List<String> columns;

  public ScanCommand(Logger console) {
    super(console);
  }

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(
      sourceFile != null && !sourceFile.isEmpty(),
      "Missing file name");

    Schema schema = getAvroSchema(sourceFile);
    Schema projection = Expressions.filterSchema(schema, columns);

    long startTime = System.currentTimeMillis();
    Iterable<Object> reader = openDataFile(sourceFile, projection);
    boolean threw = true;
    long count = 0;
    try {
      for (Object record : reader) {
        count += 1;
      }
      threw = false;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed on record " + count, e);
    } finally {
      if (reader instanceof Closeable) {
        Closeables.close((Closeable) reader, threw);
      }
    }
    long endTime = System.currentTimeMillis();

    console.info("Scanned " + count + " records from " + sourceFile);
    console.info("Time: " + (endTime - startTime) / 1000.0 + " s");
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
      "# Scan all the records from file \"data.avro\":",
      "data.avro",
      "# Scan all the records from file \"data.parquet\":",
      "data.parquet"
    );
  }
}
