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

package org.apache.parquet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Use java.nio.file.Files facilities as replacement.
 */
@Deprecated
public final class Files {
  private Files() {}

  public static List<String> readAllLines(File file, Charset charset) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    try {
      List<String> result = new ArrayList<String>();
      for (; ; ) {
        String line = reader.readLine();
        if (line == null) break;
        result.add(line);
      }
      return result;
    } finally {
      reader.close();
    }
  }
}
