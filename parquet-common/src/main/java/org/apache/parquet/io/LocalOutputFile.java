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

package org.apache.parquet.io;

import com.sun.tools.doclets.standard.Standard;

import java.io.IOException;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LocalOutputFile implements OutputFile {
  private final FileSystem fs;
  private final Path path;

  public LocalOutputFile(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  @Override
	public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new LocalOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW));
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new LocalOutputStream(Files.newOutputStream(path));
  }

	@Override
	public boolean supportsBlockSize() {
    return false;
	}

	@Override
	public long defaultBlockSize() {
    return 0;
	}
}
