/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.codec;

import parquet.Log;
import parquet.Preconditions;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Utilities for SnappyCompressor and SnappyDecompressor.
 */
public class SnappyUtil {
  public static void validateBuffer(byte[] buffer, int off, int len) {
    Preconditions.checkNotNull(buffer, "buffer");
    Preconditions.checkArgument(off >= 0 && len >= 0 && off <= buffer.length - len,
        "Invalid offset or length. Out of buffer bounds. buffer.length=" + buffer.length
        + " off=" + off + " len=" + len);
  }
}
