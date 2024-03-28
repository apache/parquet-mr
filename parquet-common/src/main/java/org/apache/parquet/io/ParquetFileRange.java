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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Class to define a file range for a parquet file and to
 * hold future data for any ongoing read for that range.
 */
public class ParquetFileRange {

  /**
   * Start position in file.
   */
  private final long offset;

  /**
   * Length of data to be read from position.
   */
  private final int length;

  /**
   * A future object to hold future for ongoing read.
   */
  private CompletableFuture<ByteBuffer> dataReadFuture;

  public ParquetFileRange(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public CompletableFuture<ByteBuffer> getDataReadFuture() {
    return dataReadFuture;
  }

  public void setDataReadFuture(CompletableFuture<ByteBuffer> dataReadFuture) {
    this.dataReadFuture = dataReadFuture;
  }

  @Override
  public String toString() {
    return "range[" + this.offset + " - " + (this.offset + (long) this.length) + "]";
  }
}
