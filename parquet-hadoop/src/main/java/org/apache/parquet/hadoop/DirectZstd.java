/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.codec.ZstdDecompressorStream;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.parquet.hadoop.codec.ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED;
import static org.apache.parquet.hadoop.codec.ZstandardCodec.DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL;
import static org.apache.parquet.hadoop.codec.ZstandardCodec.DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS;
import static org.apache.parquet.hadoop.codec.ZstandardCodec.PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED;
import static org.apache.parquet.hadoop.codec.ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL;
import static org.apache.parquet.hadoop.codec.ZstandardCodec.PARQUET_COMPRESS_ZSTD_WORKERS;

/**
 * Utility class to support creating compressor and decompressor instances for the ZStandard codec. It is implemented in
 * a way to work around the codec pools implemented in both parquet-mr and hadoop. These codec pools may result creating
 * and dereferencing direct byte buffers causing OOM errors in case of many parallel compressor/decompressor instances
 * are required working on direct memory.
 *
 * @see DirectCodecFactory.DirectCodecPool
 * @see org.apache.hadoop.io.compress.CodecPool
 */
class DirectZstd {

  static CodecFactory.BytesCompressor createCompressor(Configuration conf, int pageSize) {
    return new ZstdCompressor(
      getPool(conf),
      conf.getInt(PARQUET_COMPRESS_ZSTD_LEVEL, DEFAULT_PARQUET_COMPRESS_ZSTD_LEVEL),
      conf.getInt(PARQUET_COMPRESS_ZSTD_WORKERS, DEFAULTPARQUET_COMPRESS_ZSTD_WORKERS),
      pageSize);
  }

  static CodecFactory.BytesDecompressor createDecompressor(Configuration conf) {
    return new ZstdDecompressor(getPool(conf));
  }

  private static class ZstdCompressor extends CodecFactory.BytesCompressor {
    private final BufferPool pool;
    private final int level;
    private final int workers;
    private final int pageSize;

    ZstdCompressor(BufferPool pool, int level, int workers, int pageSize) {
      this.pool = pool;
      this.level = level;
      this.workers = workers;
      this.pageSize = pageSize;
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      // Since BytesInput does not support direct memory this implementation is heap based
      BytesInputProviderOutputStream stream = new BytesInputProviderOutputStream(pageSize);
      try (ZstdOutputStream zstdStream = new ZstdOutputStream(stream, pool, level)) {
        zstdStream.setWorkers(workers);
        bytes.writeAllTo(zstdStream);
      }
      return stream.getBytesInput();
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.ZSTD;
    }

    @Override
    public void release() {
      // Nothing to do here since we release resources where we create them
    }
  }

  private static class ZstdDecompressor extends CodecFactory.BytesDecompressor {
    private final BufferPool pool;

    private ZstdDecompressor(BufferPool pool) {
      this.pool = pool;
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      // Since BytesInput does not support direct memory this implementation is heap based
      try (ZstdDecompressorStream decompressorStream = new ZstdDecompressorStream(bytes.toInputStream(), pool)) {
        // We need to copy the bytes from the input stream, so we can close it here (BytesInput does not support closing)
        return BytesInput.copy(BytesInput.from(decompressorStream, uncompressedSize));
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) throws IOException {
      Zstd.decompress(output, input);
    }

    @Override
    public void release() {
      // Nothing to do here since we release resources where we create them
    }
  }

  private static class BytesInputProviderOutputStream extends ByteArrayOutputStream {
    BytesInputProviderOutputStream(int initialCapacity) {
      super(initialCapacity);
    }

    BytesInput getBytesInput() {
      return BytesInput.from(buf, 0, count);
    }
  }

  private static BufferPool getPool(Configuration conf) {
    if (conf.getBoolean(PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED, DEFAULT_PARQUET_COMPRESS_ZSTD_BUFFERPOOL_ENABLED)) {
      return RecyclingBufferPool.INSTANCE;
    } else {
      return NoPool.INSTANCE;
    }
  }
}
