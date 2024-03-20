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
package org.apache.parquet.column.values.bytestreamsplit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ByteStreamSplitValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(ByteStreamSplitValuesReader.class);
  protected final int elementSizeInBytes;
  protected ByteBuffer decodedDataBuffer;
  private int indexInStream;
  private int valuesCount;

  protected ByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.elementSizeInBytes = elementSizeInBytes;
    this.indexInStream = 0;
    this.valuesCount = 0;
  }

  protected int nextElementByteOffset() {
    if (indexInStream >= valuesCount) {
      throw new ParquetDecodingException("Byte-stream data was already exhausted.");
    }
    int offset = indexInStream * elementSizeInBytes;
    ++indexInStream;
    return offset;
  }

  // Decode an entire data page
  private byte[] decodeData(ByteBuffer encoded, int valuesCount) {
    assert encoded.limit() == valuesCount * elementSizeInBytes;
    byte[] decoded = new byte[encoded.limit()];
    int destByteIndex = 0;
    for (int srcValueIndex = 0; srcValueIndex < valuesCount; ++srcValueIndex) {
      for (int stream = 0; stream < elementSizeInBytes; ++stream, ++destByteIndex) {
        decoded[destByteIndex] = encoded.get(srcValueIndex + stream * valuesCount);
      }
    }
    assert destByteIndex == decoded.length;
    return decoded;
  }

  @Override
  public void initFromPage(int valuesCount, ByteBufferInputStream stream)
      throws ParquetDecodingException, IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    // ByteStreamSplitValuesWriter does not write number of encoded values to the stream. As the parquet
    // specs state that no padding is allowed inside a data page, we can infer the number of values solely
    // by dividing the number of bytes in the stream by the size of each element.
    if (stream.available() % elementSizeInBytes != 0) {
      String errorMessage = String.format(
          "Invalid ByteStreamSplit stream, total length: %d bytes, element size: %d bytes.",
          stream.available(), elementSizeInBytes);
      throw new ParquetDecodingException(errorMessage);
    }
    this.valuesCount = stream.available() / elementSizeInBytes;

    // The input valuesCount includes number of nulls. It is used for an upperbound check only.
    if (valuesCount < this.valuesCount) {
      String errorMessage = String.format(
          "Invalid ByteStreamSplit stream, num values upper bound (w/ nulls): %d, num encoded values: %d",
          valuesCount, this.valuesCount);
      throw new ParquetDecodingException(errorMessage);
    }

    // Eagerly read and decode the data. This allows returning stable
    // Binary views into the internal decode buffer for FIXED_LEN_BYTE_ARRAY.
    final int totalSizeInBytes = stream.available();
    final ByteBuffer encodedData = stream.slice(totalSizeInBytes).slice(); // possibly zero-copy
    final byte[] decodedData = decodeData(encodedData, this.valuesCount);
    decodedDataBuffer = ByteBuffer.wrap(decodedData).order(ByteOrder.LITTLE_ENDIAN);
    indexInStream = 0;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (n < 0 || indexInStream + n > valuesCount) {
      String errorMessage = String.format(
          "Cannot skip this many elements. Current index: %d. Skip %d. Total number of elements: %d",
          indexInStream, n, valuesCount);
      throw new ParquetDecodingException(errorMessage);
    }
    indexInStream += n;
  }
}
