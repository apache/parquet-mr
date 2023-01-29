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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.hadoop.util.ColumnPruner;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

@Parameters(commandDescription="Prune column(s) in a Parquet file and save it to a new file. " +
  "The columns left are not changed.")
public class PruneColumnsCommand extends BaseCommand {

  public PruneColumnsCommand(Logger console) {
    super(console);
  }

  @Parameter(
    names = {"-i", "--input"},
    description = "<input parquet file path>",
    required = true)
  String input;

  @Parameter(
    names = {"-o", "--output"},
    description = "<output parquet file path>",
    required = true)
  String output;

  @Parameter(
    names = {"-c", "--columns"},
    description = "<columns to be replaced with masked value>",
    required = true)
  List<String> cols;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(input != null && output != null,
      "Both input and output parquet file paths are required.");

    Preconditions.checkArgument(cols != null && cols.size() > 0,
      "columns cannot be null or empty");

    Path inPath = new Path(input);
    Path outPath = new Path(output);
    ColumnPruner columnPruner = new ColumnPruner();
    columnPruner.pruneColumns(getConf(), inPath, outPath, cols);
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
      "# Removes specified columns and write to a new Parquet file",
      "-i input.parquet -o output.parquet -c col1_name"
    );
  }
}
