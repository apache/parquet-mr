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
package org.apache.parquet.column.values.plain;

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ValuesReader for FIXED_LEN_BYTE_ARRAY.
 */
public class FixedLenByteArrayPlainValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(FixedLenByteArrayPlainValuesReader.class);

  private final int length;
  private ByteBufferInputStream in;

  public FixedLenByteArrayPlainValuesReader(int length) {
    this.length = length;
  }

  @Override
  public Binary readBytes() {
    try {
      return Binary.fromConstantByteBuffer(in.slice(length));
    } catch (IOException | RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + in.position(), e);
    }
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    try {
      in.skipFully(n * length);
    } catch (IOException | RuntimeException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + in.position(), e);
    }
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());
    this.in = stream.remainingStream();
  }
}
