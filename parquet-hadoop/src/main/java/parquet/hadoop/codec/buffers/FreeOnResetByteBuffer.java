package parquet.hadoop.codec.buffers;
/**
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

import java.nio.ByteBuffer;

/**
 * ByteBuffer wrapper the ensures the directBuffer is freed if it
 * is not currently being used, and then re-allocated when it is needed
 */
public class FreeOnResetByteBuffer implements CodecByteBuffer {
  private ByteBuffer buf = null;
  private final int buffsize;

  public FreeOnResetByteBuffer(int buffsize) {
    this.buffsize = buffsize;
  }

  private void allocateBuffer()
  {
    buf = ByteBuffer.allocateDirect(buffsize);
  }

  public ByteBuffer get() {
    if (buf == null) {
      allocateBuffer();
    }
    return buf;
  }

  /**
   * If the buffer is no longer needed then immediately free it so the memory
   * can be used elsewhere
   */
  public void resetBuffer() {
    freeBuffer();
  }

  /**
   * Explicitly free the off-heap buffer
   */
  public void freeBuffer() {
    if (buf != null) {
      CodecByteBufferUtil.freeOffHeapBuffer(buf);
      // The rest will be cleaned up when the buffer object is finalized
      buf = null;
    }
  }

}
