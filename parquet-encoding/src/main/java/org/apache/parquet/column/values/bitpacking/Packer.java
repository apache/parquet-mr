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

/**
 * Factory for packing implementations
 */
public enum Packer {

  /**
   * packers who fill the Least Significant Bit First
   * int and byte packer have the same result on Big Endian architectures
   */
  BIG_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return beIntPackerFactory.newIntPacker(width);
    }
    @Override
    public BytePacker newBytePacker(int width) {
      return beBytePackerFactory.newBytePacker(width);
    }

    @Override
    public BytePacker newBytePackerVector(int width) {
      throw new RuntimeException("Not currently supported!");
    }

    @Override
    public BytePackerForLong newBytePackerForLong(int width) {
      return beBytePackerForLongFactory.newBytePackerForLong(width);
    }
  },

  /**
   * packers who fill the Most Significant Bit first
   * int and byte packer have the same result on Little Endian architectures
   */
  LITTLE_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return leIntPackerFactory.newIntPacker(width);
    }
    @Override
    public BytePacker newBytePacker(int width) {
      return leBytePackerFactory.newBytePacker(width);
    }

    @Override
    public BytePacker newBytePackerVector(int width) {
      if (leBytePacker512VectorFactory == null) {
        synchronized (Packer.class) {
          if (leBytePacker512VectorFactory == null) {
            leBytePacker512VectorFactory = getBytePackerFactory("ByteBitPacking512VectorLE");
          }
        }
      }
      if (leBytePacker512VectorFactory == null) {
        throw new RuntimeException("No enable java vector plugin on little endian architectures");
      }
      return leBytePacker512VectorFactory.newBytePacker(width);
    }

    @Override
    public BytePackerForLong newBytePackerForLong(int width) {
      return leBytePackerForLongFactory.newBytePackerForLong(width);
    }
  };

  private static IntPackerFactory getIntPackerFactory(String name) {
    return (IntPackerFactory)getStaticField("org.apache.parquet.column.values.bitpacking." + name, "factory");
  }

  private static BytePackerFactory getBytePackerFactory(String name) {
    return (BytePackerFactory)getStaticField("org.apache.parquet.column.values.bitpacking." + name, "factory");
  }

  private static BytePackerForLongFactory getBytePackerForLongFactory(String name) {
    return (BytePackerForLongFactory)getStaticField("org.apache.parquet.column.values.bitpacking." + name, "factory");
  }

  private static Object getStaticField(String className, String fieldName) {
    try {
      return Class.forName(className).getField(fieldName).get(null);
    } catch (IllegalArgumentException | IllegalAccessException
        | NoSuchFieldException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static IntPackerFactory beIntPackerFactory = getIntPackerFactory("LemireBitPackingBE");
  static IntPackerFactory leIntPackerFactory = getIntPackerFactory("LemireBitPackingLE");
  static BytePackerFactory beBytePackerFactory = getBytePackerFactory("ByteBitPackingBE");
  static BytePackerFactory leBytePackerFactory = getBytePackerFactory("ByteBitPackingLE");
  // ByteBitPacking512VectorLE is not enabled default, so leBytePacker512VectorFactory cannot be initialized as a static property
  static BytePackerFactory leBytePacker512VectorFactory = null;
  static BytePackerForLongFactory beBytePackerForLongFactory = getBytePackerForLongFactory("ByteBitPackingForLongBE");
  static BytePackerForLongFactory leBytePackerForLongFactory = getBytePackerForLongFactory("ByteBitPackingForLongLE");

  /**
   * @param width the width in bits of the packed values
   * @return an int based packer
   */
  public abstract IntPacker newIntPacker(int width);

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer
   */
  public abstract BytePacker newBytePacker(int width);

  public BytePacker newBytePackerVector(int width) {
    throw new RuntimeException("newBytePackerVector must be implemented by subclasses!");
  }

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer for INT64
   */
  public abstract BytePackerForLong newBytePackerForLong(int width);
}
