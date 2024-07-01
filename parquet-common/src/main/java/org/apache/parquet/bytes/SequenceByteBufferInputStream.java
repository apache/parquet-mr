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

package org.apache.parquet.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *   A bare minimum implementation of a {@link java.io.SequenceInputStream} that wraps an
 *   <i>ordered</i> collection of ByteBufferInputStreams.
 *   <p>
 *   This class, as implemented, is intended only for a specific use in the ParquetFileReader and
 *   throws {@link UnsupportedOperationException} in unimplemented methods to catch any unintended
 *   use in other cases.
 *   <p>
 *   Even though this class is derived from ByteBufferInputStream it explicitly does not support any
 *   byte buffer related methods like slice. It does, however support sliceBuffers which is a
 *   curious case of reading data from underlying streams
 *   <p>
 *   Even though this class changes the state of the underlying streams (by reading from them)
 *   it does not own them and so the close method does not close the streams. To avoid resource
 *   leaks the calling code should close the underlying streams
 */
public class SequenceByteBufferInputStream extends ByteBufferInputStream {

  Collection<ByteBufferInputStream> collection;
  Iterator<ByteBufferInputStream> iterator;
  ByteBufferInputStream current;
  long position = 0;

  @Override
  public String toString() {
    return "SequenceByteBufferInputStream{" + "collection="
        + collection + ", current="
        + current + ", position="
        + position + '}';
  }

  public SequenceByteBufferInputStream(Collection<ByteBufferInputStream> collection) {
    this.collection = collection;
    iterator = collection.iterator();
    current = iterator.hasNext() ? iterator.next() : null;
    if (current == null) {
      throw new UnsupportedOperationException(
          "Initializing SequenceByteBufferInputStream with an empty collection is not supported");
    }
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public int read(ByteBuffer out) {
    int len = out.remaining();
    if (len <= 0) {
      return 0;
    }
    if (current == null) {
      return -1;
    }
    int totalBytesRead = 0;
    while (totalBytesRead < len) {
      int bytesRead = current.read(out);
      if (bytesRead == -1) {
        if (iterator.hasNext()) {
          current = iterator.next();
        } else {
          break;
        }
      } else {
        totalBytesRead += bytesRead;
      }
    }
    position += totalBytesRead;
    return totalBytesRead;
  }

  @Override
  public ByteBuffer slice(int length) throws EOFException {
    throw new UnsupportedOperationException("slice is not supported");
  }

  @Override
  /**
   * This is a blocking call. Use with care when using in asynchronous mode.
   */
  public List<ByteBuffer> sliceBuffers(long len) throws EOFException {
    if (len <= 0) {
      return Collections.emptyList();
    }

    if (current == null) {
      throw new EOFException();
    }

    List<ByteBuffer> buffers = new ArrayList<>();
    long bytesAccumulated = 0;
    while (bytesAccumulated < len) {
      // This is not strictly according to the input stream contract, but once again the
      // underlying implementations of ByteBufferInputStream return the available bytes
      // based on the size of the underlying buffers rather than the bytes currently read
      // into the buffers. This works for us because the underlying implementations will
      // actually fill the buffers with the data before returning the slices we ask for
      // (which is why this is a blocking call)
      if (current.available() > 0) {
        int bufLen = (int) Math.min(len - bytesAccumulated, current.available());
        List<ByteBuffer> currentSlices = current.sliceBuffers(bufLen);
        buffers.addAll(currentSlices);
        bytesAccumulated += bufLen;

        // update state; the bytes are considered read
        this.position += bufLen;
      } else {
        if (iterator.hasNext()) {
          current = iterator.next();
        } else {
          // there are no more streams
          throw new EOFException();
        }
      }
    }
    position += bytesAccumulated;
    return buffers;
  }

  @Override
  public ByteBufferInputStream sliceStream(long length) throws EOFException {
    throw new UnsupportedOperationException("sliceStream is not supported");
  }

  @Override
  public List<ByteBuffer> remainingBuffers() {
    throw new UnsupportedOperationException("remainingBuffers is not supported");
  }

  @Override
  public ByteBufferInputStream remainingStream() {
    throw new UnsupportedOperationException("remainingStream is not supported");
  }

  @Override
  public int read() throws IOException {
    int val;
    while (true) {
      try {
        val = current.read() & 0xFF; // as unsigned
        position += 1;
        break;
      } catch (EOFException e) {
        if (iterator.hasNext()) {
          current = iterator.next();
        } else {
          return -1;
        }
      }
    }
    return val;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len <= 0) {
      if (len < 0) {
        throw new IndexOutOfBoundsException("Read length must be greater than 0: " + len);
      }
      return 0;
    }
    if (current == null) {
      return -1;
    }
    int totalBytes = 0;
    while (totalBytes < len) {
      int bytesRead = current.read(b, off + totalBytes, len - totalBytes);
      if (bytesRead == -1) {
        if (iterator.hasNext()) {
          current = iterator.next();
        } else {
          break;
        }
      } else {
        totalBytes += bytesRead;
      }
    }
    position += totalBytes;
    return totalBytes;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      if (n < 0) {
        throw new IndexOutOfBoundsException("Skip length must be greater than 0: " + n);
      }
      return 0;
    }
    if (current == null) {
      return -1;
    }
    long totalBytesSkipped = 0;
    while (totalBytesSkipped < n) {
      long bytesSkipped = current.skip(n - totalBytesSkipped);
      // the contract for skip specifies that skip may return 0 if EOF is reached in which case
      // we have no good way of knowing the end of stream has been reached. We depend on the
      // fact that all implementations of ByteBufferInputStream return -1 on EOF
      if (bytesSkipped == -1) {
        if (iterator.hasNext()) {
          current = iterator.next();
        } else {
          break;
        }
      } else {
        totalBytesSkipped += bytesSkipped;
      }
    }
    position += totalBytesSkipped;
    return totalBytesSkipped;
  }

  @Override
  public int available() {
    return current.available();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException("mark is not supported");
  }

  @Override
  public void reset() throws IOException {
    throw new UnsupportedOperationException("reset is not supported");
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
