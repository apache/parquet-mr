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

import org.junit.Before;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class CSVFileTest extends FileTest {

  protected File csvFile() throws IOException {
    return csvFile(getClass().getSimpleName());
  }

  protected File csvFile(String basename) throws IOException {
    File tmpDir = getTempFolder();
    File file = new File(tmpDir, basename + ".csv");
    createTestCSVFile(file);
    return file;
  }

  private void createTestCSVFile(File file) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      writer.write(String.format("%s,%s,%s\n",
        INT32_FIELD, INT64_FIELD, BINARY_FIELD));
      writer.write(String.format("%d,%d,\"%s\"\n",
        Integer.MIN_VALUE, Long.MIN_VALUE, COLORS[0]));
      writer.write(String.format("%d,%d,\"%s\"\n",
        Integer.MAX_VALUE, Long.MAX_VALUE, COLORS[1]));
    }
  }
}
