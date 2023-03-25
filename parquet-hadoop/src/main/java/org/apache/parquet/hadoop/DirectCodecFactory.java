/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;



import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.Preconditions;

/**
 * Factory to produce compressors and decompressors that operate on java
 * direct memory, without requiring a copy into heap memory (where possible).
 */
class DirectCodecFactory extends CodecFactory implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DirectCodecFactory.class);

  private final ByteBufferAllocator allocator;

  // Any of these can be null depending on the version of hadoop on the classpath
  private static final Class<?> DIRECT_DECOMPRESSION_CODEC_CLASS;
  private static final Method DECOMPRESS_METHOD;
  private static final Method CREATE_DIRECT_DECOMPRESSOR_METHOD;

  static {
    Class<?> tempClass = null;
    Method tempCreateMethod = null;
    Method tempDecompressMethod = null;
    try {
      tempClass = Class.forName("org.apache.hadoop.io.compress.DirectDecompressionCodec");
      tempCreateMethod = tempClass.getMethod("createDirectDecompressor");
      tempDecompressMethod = tempClass.getMethod("decompress", ByteBuffer.class, ByteBuffer.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // do nothing, the class will just be assigned null
    }
    DIRECT_DECOMPRESSION_CODEC_CLASS = tempClass;
    CREATE_DIRECT_DECOMPRESSOR_METHOD = tempCreateMethod;
    DECOMPRESS_METHOD = tempDecompressMethod;
  }

  /**
   * See docs on CodecFactory#createDirectCodecFactory which is how this class is
   * exposed publicly and is just a pass-through factory method for this constructor
   * to hide the rest of this class from public access.
   *
   * @throws NullPointerException if allocator is {@code null}
   */
  DirectCodecFactory(Configuration config, ByteBufferAllocator allocator, int pageSize) {
    super(config, pageSize);

    this.allocator = Objects.requireNonNull(allocator, "allocator cannot be null");
    Preconditions.checkState(allocator.isDirect(),
        "A %s requires a direct buffer allocator be provided.",
        getClass().getSimpleName());
  }

  private ByteBuffer ensure(ByteBuffer buffer, int size) {
    if (buffer == null) {
      buffer = allocator.allocate(size);
    } else if (buffer.capacity() >= size) {
      buffer.clear();
    } else {
      release(buffer);
      buffer = allocator.allocate(size);
    }
    return buffer;
  }

  ByteBuffer release(ByteBuffer buffer) {
    if (buffer != null) {
      allocator.release(buffer);
    }
    return null;
  }

  @Override
  protected BytesCompressor createCompressor(final CompressionCodecName codecName) {

    CompressionCodec codec = getCodec(codecName);
    if (codec == null) {
      return new NoopCompressor();
    } else if (codecName == CompressionCodecName.SNAPPY) {
      // avoid using the default Snappy codec since it allocates direct buffers at awkward spots.
      return new SnappyCompressor();
    } else if (codecName == CompressionCodecName.ZSTD) {
      return DirectZstd.createCompressor(configuration, pageSize);
    } else {
      // todo: create class similar to the SnappyCompressor for zlib and exclude it as
      // snappy is above since it also generates allocateDirect calls.
      return new HeapBytesCompressor(codecName);
    }
  }

  @Override
  protected BytesDecompressor createDecompressor(final CompressionCodecName codecName) {
    CompressionCodec codec = getCodec(codecName);
    if (codec == null) {
      return new NoopDecompressor();
    } else if (codecName == CompressionCodecName.SNAPPY ) {
      return new SnappyDecompressor();
    } else if (codecName == CompressionCodecName.ZSTD) {
      return DirectZstd.createDecompressor(configuration);
    } else if (DirectCodecPool.INSTANCE.codec(codec).supportsDirectDecompression()) {
      return new FullDirectDecompressor(codecName);
    } else {
      return new IndirectDecompressor(codec);
    }
  }

  public void close() {
    release();
  }

  /**
   * Wrapper around legacy hadoop compressors that do not implement a direct memory
   * based version of the decompression algorithm.
   */
  public class IndirectDecompressor extends BytesDecompressor {
    private final Decompressor decompressor;

    public IndirectDecompressor(CompressionCodec codec) {
      this.decompressor = DirectCodecPool.INSTANCE.codec(codec).borrowDecompressor();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      decompressor.reset();
      byte[] inputBytes = bytes.toByteArray();
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] output = new byte[uncompressedSize];
      decompressor.decompress(output, 0, uncompressedSize);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {

      decompressor.reset();
      byte[] inputBytes = new byte[compressedSize];
      input.position(0);
      input.get(inputBytes);
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] outputBytes = new byte[uncompressedSize];
      decompressor.decompress(outputBytes, 0, uncompressedSize);
      output.clear();
      output.put(outputBytes);
    }

    @Override
    public void release() {
      DirectCodecPool.INSTANCE.returnDecompressor(decompressor);
    }
  }

  /**
   * Wrapper around new Hadoop compressors that implement a direct memory
   * based version of a particular decompression algorithm. To maintain
   * compatibility with Hadoop 1.x these classes that implement
   * {@link org.apache.hadoop.io.compress.DirectDecompressionCodec}
   * are currently retrieved and have their decompression method invoked
   * with reflection.
   */
  public class FullDirectDecompressor extends BytesDecompressor {
    private final Object decompressor;
    private HeapBytesDecompressor extraDecompressor;
    public FullDirectDecompressor(CompressionCodecName codecName){
      CompressionCodec codec = getCodec(codecName);
      this.decompressor = DirectCodecPool.INSTANCE.codec(codec).borrowDirectDecompressor();
      this.extraDecompressor = new HeapBytesDecompressor(codecName);
    }

    @Override
    public BytesInput decompress(BytesInput compressedBytes, int uncompressedSize) throws IOException {
    	return extraDecompressor.decompress(compressedBytes, uncompressedSize);
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {
      output.clear();
      try {
        DECOMPRESS_METHOD.invoke(decompressor, (ByteBuffer) input.limit(compressedSize), (ByteBuffer) output.limit(uncompressedSize));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new DirectCodecPool.ParquetCompressionCodecException(e);
      }
      output.position(uncompressedSize);
    }

    @Override
    public void release() {
      DirectCodecPool.INSTANCE.returnDirectDecompressor(decompressor);
      extraDecompressor.release();
    }

  }

  public class NoopDecompressor extends BytesDecompressor {

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
        throws IOException {
      Preconditions.checkArgument(compressedSize == uncompressedSize,
          "Non-compressed data did not have matching compressed and uncompressed sizes.");
      output.clear();
      output.put((ByteBuffer) input.duplicate().position(0).limit(compressedSize));
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      return bytes;
    }

    @Override
    public void release() {}

  }

  public class SnappyDecompressor extends BytesDecompressor {

    private HeapBytesDecompressor extraDecompressor;
    public SnappyDecompressor() {
      this.extraDecompressor = new HeapBytesDecompressor(CompressionCodecName.SNAPPY);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      return extraDecompressor.decompress(bytes, uncompressedSize);
    }

    @Override
    public void decompress(ByteBuffer src, int compressedSize, ByteBuffer dst, int uncompressedSize) throws IOException {
      dst.clear();
      int size = Snappy.uncompress(src, dst);
      dst.limit(size);
    }

    @Override
    public void release() {}
  }

  public class SnappyCompressor extends BytesCompressor {

    // TODO - this outgoing buffer might be better off not being shared, this seems to
    // only work because of an extra copy currently happening where this interface is
    // be consumed
    private ByteBuffer incoming;
    private ByteBuffer outgoing;

    /**
     * Compress a given buffer of bytes
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      int maxOutputSize = Snappy.maxCompressedLength((int) bytes.size());
      ByteBuffer bufferIn = bytes.toByteBuffer();
      outgoing = ensure(outgoing, maxOutputSize);
      final int size;
      if (bufferIn.isDirect()) {
        size = Snappy.compress(bufferIn, outgoing);
      } else {
        // Snappy library requires buffers be direct
        this.incoming = ensure(this.incoming, (int) bytes.size());
        this.incoming.put(bufferIn);
        this.incoming.flip();
        size = Snappy.compress(this.incoming, outgoing);
      }

      outgoing.limit(size);

      return BytesInput.from(outgoing);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    public void release() {
      outgoing = DirectCodecFactory.this.release(outgoing);
      incoming = DirectCodecFactory.this.release(incoming);
    }

  }

  public static class NoopCompressor extends BytesCompressor {

    public NoopCompressor() {}

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      return bytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.UNCOMPRESSED;
    }

    @Override
    public void release() {}
  }

  static class DirectCodecPool {

    public static final DirectCodecPool INSTANCE = new DirectCodecPool();

    private final Map<CompressionCodec, CodecPool> codecs =
        Collections.synchronizedMap(new HashMap<CompressionCodec, CodecPool>());
    private final Map<Class<?>, GenericObjectPool> directDePools = Collections
        .synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());
    private final Map<Class<?>, GenericObjectPool> dePools = Collections
        .synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());
    private final Map<Class<?>, GenericObjectPool> cPools = Collections
        .synchronizedMap(new HashMap<Class<?>, GenericObjectPool>());

    private DirectCodecPool() {}

    public class CodecPool {
      private final GenericObjectPool compressorPool;
      private final GenericObjectPool decompressorPool;
      private final GenericObjectPool directDecompressorPool;
      private final boolean supportDirectDecompressor;
      private static final String BYTE_BUF_IMPL_NOT_FOUND_MSG =
          "Unable to find ByteBuffer based %s for codec %s, will use a byte array based implementation instead.";

      private CodecPool(final CompressionCodec codec){
        try {
          boolean supportDirectDecompressor = codec.getClass() == DIRECT_DECOMPRESSION_CODEC_CLASS;
          compressorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
            public Object makeObject() throws Exception {
              return codec.createCompressor();
            }
          }, Integer.MAX_VALUE);

          Object com = compressorPool.borrowObject();
          if (com != null) {
            cPools.put(com.getClass(), compressorPool);
            compressorPool.returnObject(com);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(BYTE_BUF_IMPL_NOT_FOUND_MSG, "compressor", codec.getClass().getName()));
            }
          }

          decompressorPool = new GenericObjectPool(new BasePoolableObjectFactory() {
            public Object makeObject() throws Exception {
              return codec.createDecompressor();
            }
          }, Integer.MAX_VALUE);

          Object decom = decompressorPool.borrowObject();
          if (decom != null) {
            dePools.put(decom.getClass(), decompressorPool);
            decompressorPool.returnObject(decom);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug(String.format(BYTE_BUF_IMPL_NOT_FOUND_MSG, "decompressor", codec.getClass().getName()));
            }
          }

          if (supportDirectDecompressor) {
            directDecompressorPool = new GenericObjectPool(
                new BasePoolableObjectFactory() {
                  public Object makeObject() throws Exception {
                    return CREATE_DIRECT_DECOMPRESSOR_METHOD.invoke(DIRECT_DECOMPRESSION_CODEC_CLASS);
                  }
                }, Integer.MAX_VALUE);

            Object ddecom = directDecompressorPool.borrowObject();
            if (ddecom != null) {
              directDePools.put(ddecom.getClass(), directDecompressorPool);
              directDecompressorPool.returnObject(ddecom);

            } else {
              supportDirectDecompressor = false;
              if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(BYTE_BUF_IMPL_NOT_FOUND_MSG, "compressor", codec.getClass().getName()));
              }
            }

          } else {
            directDecompressorPool = null;
          }

          this.supportDirectDecompressor = supportDirectDecompressor;
        } catch (Exception e) {
          throw new ParquetCompressionCodecException("Error creating compression codec pool.", e);
        }
      }

      public Object borrowDirectDecompressor(){
        Preconditions.checkArgument(supportDirectDecompressor, "Tried to get a direct Decompressor from a non-direct codec.");
        try {
          return directDecompressorPool.borrowObject();
        } catch (Exception e) {
          throw new ParquetCompressionCodecException(e);
        }
      }

      public boolean supportsDirectDecompression() {
        return supportDirectDecompressor;
      }

      public Decompressor borrowDecompressor(){
        return borrow(decompressorPool);
      }

      public Compressor borrowCompressor(){
        return borrow(compressorPool);
      }
    }

    public CodecPool codec(CompressionCodec codec){
      CodecPool pools = codecs.get(codec);
      if(pools == null){
        synchronized(this){
          pools = codecs.get(codec);
          if(pools == null){
            pools = new CodecPool(codec);
            codecs.put(codec, pools);
          }
        }
      }
      return pools;
    }

    private void returnToPool(Object obj, Map<Class<?>, GenericObjectPool> pools) {
      try {
        GenericObjectPool pool = pools.get(obj.getClass());
        if (pool == null) {
          throw new IllegalStateException("Received unexpected compressor or decompressor, " +
              "cannot be returned to any available pool: " + obj.getClass().getSimpleName());
        }
        pool.returnObject(obj);
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }
    }

    /**
     * Borrow an object from a pool.
     *
     * @param pool - the pull to borrow from, must not be null
     * @return - an object from the pool
     */
    @SuppressWarnings("unchecked")
    public <T> T borrow(GenericObjectPool pool) {
      try {
        return (T) pool.borrowObject();
      } catch (Exception e) {
        throw new ParquetCompressionCodecException(e);
      }

    }

    public void returnCompressor(Compressor compressor) {
      returnToPool(compressor, cPools);
    }

    public void returnDecompressor(Decompressor decompressor) {
      returnToPool(decompressor, dePools);
    }

    public void returnDirectDecompressor(Object decompressor) {
      returnToPool(decompressor, directDePools);
    }

    public static class ParquetCompressionCodecException extends ParquetRuntimeException {

      public ParquetCompressionCodecException() {
        super();
      }

      public ParquetCompressionCodecException(String message, Throwable cause) {
        super(message, cause);
      }

      public ParquetCompressionCodecException(String message) {
        super(message);
      }

      public ParquetCompressionCodecException(Throwable cause) {
        super(cause);
      }
    }
  }
}
