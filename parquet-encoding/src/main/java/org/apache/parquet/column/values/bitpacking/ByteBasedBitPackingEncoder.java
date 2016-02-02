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
package org.apache.parquet.column.values.bitpacking;

import static org.apache.parquet.bytes.BytesInput.concat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the generated Byte based bit packing to write ints into a BytesInput
 *
 * @author Julien Le Dem
 *
 */
public class ByteBasedBitPackingEncoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ByteBasedBitPackingEncoder.class);

  private static final int VALUES_WRITTEN_AT_A_TIME = 8;

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] input = new int[VALUES_WRITTEN_AT_A_TIME];
  private final int slabSize;
  private int inputSize;
  private byte[] packed;
  private int packedPosition;
  private final List<BytesInput> slabs = new ArrayList<BytesInput>();
  private int totalValues;

  /**
   * @param bitWidth the number of bits used to encode an int
   */
  public ByteBasedBitPackingEncoder(int bitWidth, Packer packer) {
    this.bitWidth = bitWidth;
    this.inputSize = 0;
    // must be a multiple of bitWidth
    this.slabSize = bitWidth * 64 * 1024;
    initPackedSlab();
    this.packer = packer.newBytePacker(bitWidth);
  }

  /**
   * writes an int using the requested number of bits.
   * accepts only value < 2^bitWidth
   * @param value the value to write
   * @throws IOException
   */
  public void writeInt(int value) throws IOException {
    input[inputSize] = value;
    ++ inputSize;
    if (inputSize == VALUES_WRITTEN_AT_A_TIME) {
      pack();
      if (packedPosition == slabSize) {
        slabs.add(BytesInput.from(packed));
        initPackedSlab();
      }
    }
  }

  private void pack() {
    packer.pack8Values(input, 0, packed, packedPosition);
    packedPosition += bitWidth;
    totalValues += inputSize;
    inputSize = 0;
  }

  private void initPackedSlab() {
    packed = new byte[slabSize];
    packedPosition = 0;
  }

  /**
   * @return the bytes representing the packed values
   * @throws IOException
   */
  public BytesInput toBytes() throws IOException {
    int packedByteLength = packedPosition + BytesUtils.paddedByteCountFromBits(inputSize * bitWidth);

    if (LOGGER.isDebugEnabled())
      LOGGER.debug("writing {} bytes", slabs.size() * slabSize + packedByteLength);

    if (inputSize > 0) {
      for (int i = inputSize; i < input.length; i++) {
        input[i] = 0;
      }
      pack();
    }
    return concat(concat(slabs), BytesInput.from(packed, 0, packedByteLength));
  }

  /**
   * @return size of the data as it would be written
   */
  public long getBufferSize() {
    return BytesUtils.paddedByteCountFromBits(totalValues * bitWidth);
  }

  /**
   * @return total memory allocated
   */
  public long getAllocatedSize() {
    return (slabs.size() * slabSize) + packed.length + input.length * 4;
  }

  public String memUsageString(String prefix) {
    return String.format("%s ByteBitPacking %d slabs, %d bytes", prefix, slabs.size(), getAllocatedSize());
  }

}
