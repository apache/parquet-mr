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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;

import java.nio.ByteBuffer;

/**
 * Wrapped class to pack and unpack with AVX512 on Little Endian architectures
 */
public abstract class ByteBitPacking512VectorLE {
  private static final BytePacker[] packers = new BytePacker[33];

  static {
    packers[0] = new Packer0();
    packers[1] = new Packer1();
    packers[2] = new Packer2();
    packers[3] = new Packer3();
    packers[4] = new Packer4();
    packers[5] = new Packer5();
    packers[6] = new Packer6();
    packers[7] = new Packer7();
    packers[8] = new Packer8();
    packers[9] = new Packer9();
    packers[10] = new Packer10();
    packers[11] = new Packer11();
    packers[12] = new Packer12();
    packers[13] = new Packer13();
    packers[14] = new Packer14();
    packers[15] = new Packer15();
    packers[16] = new Packer16();
    packers[17] = new Packer17();
    packers[18] = new Packer18();
    packers[19] = new Packer19();
    packers[20] = new Packer20();
    packers[21] = new Packer21();
    packers[22] = new Packer22();
    packers[23] = new Packer23();
    packers[24] = new Packer24();
    packers[25] = new Packer25();
    packers[26] = new Packer26();
    packers[27] = new Packer27();
    packers[28] = new Packer28();
    packers[29] = new Packer29();
    packers[30] = new Packer30();
    packers[31] = new Packer31();
    packers[32] = new Packer32();
  }

  public static final BytePackerFactory factory = new BytePackerFactory() {
    public BytePacker newBytePacker(int bitWidth) {
      return packers[bitWidth];
    }
  };

  private static final class Packer0 extends BytePacker {
    private int unpackCount = 0;

    private Packer0() {
      super(0);
    }

    public int getUnpackCount() {
      return unpackCount;
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
    }

    public final void unpackValuesUsingVector(final byte[] input, final int inPos, final int[] output, final int outPos) {
    }

    public final void unpackValuesUsingVector(final ByteBuffer input, final int inPos, final int[] output, final int outPos) {
    }
  }

  private static final class Packer1 extends BytePacker {
    private static final VectorSpecies<Byte> BYTE_SPECIES_64 = ByteVector.SPECIES_64;
    private static final VectorSpecies<Short> SHORT_SPECIES_512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;
    private static final int[] SHUFFLE_ARRAY = {
      0, 63, 0, 63, 0, 63, 0, 63, 0, 63, 0, 63, 0, 63, 0, 63,
      2, 63, 2, 63, 2, 63, 2, 63, 2, 63, 2, 63, 2, 63, 2, 63,
      4, 63, 4, 63, 4, 63, 4, 63, 4, 63, 4, 63, 4, 63, 4, 63,
      6, 63, 6, 63, 6, 63, 6, 63, 6, 63, 6, 63, 6, 63, 6, 63
    };

    private static final short[] LSHR_SHORT_VECTOR_ARRAY = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7
    };
    private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(SHORT_SPECIES_512, LSHR_SHORT_VECTOR_ARRAY, 0);
    private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(ByteVector.SPECIES_512, SHUFFLE_ARRAY, 0);

    private static final int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer1() {
      super(1);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(BYTE_SPECIES_64, in, inPos);
      ShortVector tempRes = byteVector.castShape(SHORT_SPECIES_512, 0)
        .reinterpretAsBytes()
        .rearrange(SHUFFLE)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
        .lanewise(VectorOperators.AND, 1);
      tempRes.castShape(INT_SPECIES_512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(INT_SPECIES_512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(BYTE_SPECIES_64, in, inPos, in.order());
      ShortVector tempRes = byteVector.castShape(SHORT_SPECIES_512, 0)
        .reinterpretAsBytes()
        .rearrange(SHUFFLE)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
        .lanewise(VectorOperators.AND, 1);
      tempRes.castShape(INT_SPECIES_512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(INT_SPECIES_512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer2 extends BytePacker {

    public static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_64;
    public static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_512;
    public static int[] PERM_INDEX = {
      0, 63, 0, 63, 0, 63, 0, 63,
      8, 63, 8, 63, 8, 63, 8, 63,
      16, 63, 16, 63, 16, 63, 16, 63,
      24, 63, 24, 63, 24, 63, 24, 63,
      32, 63, 32, 63, 32, 63, 32, 63,
      40, 63, 40, 63, 40, 63, 40, 63,
      48, 63, 48, 63, 48, 63, 48, 63,
      56, 63, 56, 63, 56, 63, 56, 63
    };


    public static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_512;
    public static VectorShuffle<Byte> PERM_MASK = VectorShuffle.fromArray(ByteVector.SPECIES_512, PERM_INDEX, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer2() {
      super(2);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(BYTE_SPECIES, in, inPos);
      ShortVector tempRes = byteVector.castShape(LONG_SPECIES, 0)
        .reinterpretAsBytes()
        .rearrange(PERM_MASK)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR, LONG_SPECIES.broadcast(0x0006000400020000L).reinterpretAsShorts())
        .lanewise(VectorOperators.AND, 3);

      tempRes.castShape(INT_SPECIES, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(INT_SPECIES, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(BYTE_SPECIES, in, inPos, in.order());
      ShortVector tempRes = byteVector.castShape(LONG_SPECIES, 0)
        .reinterpretAsBytes()
        .rearrange(PERM_MASK)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR, LONG_SPECIES.broadcast(0x0006000400020000L).reinterpretAsShorts())
        .lanewise(VectorOperators.AND, 3);

      tempRes.castShape(INT_SPECIES, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(INT_SPECIES, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer3 extends BytePacker {
    public static final short[] rshift_cnt_arr = {
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5,
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5,
    };

    public static final short[] lshift_cnt_arr = {
      0, 3, 2, 1, 4, 1, 2, 5, 0, 3, 2, 1, 4, 1, 2, 5,
      0, 3, 2, 1, 4, 1, 2, 5, 0, 3, 2, 1, 4, 1, 2, 5,
    };

    public static final short[] and_val_arr = {
      7, 7, 3, 7, 7, 1, 7, 7, 7, 7, 3, 7, 7, 1, 7, 7,
      7, 7, 3, 7, 7, 1, 7, 7, 7, 7, 3, 7, 7, 1, 7, 7,
    };

    public static final int[] perm_pos0_arr = {
      0, 63, 0, 63, 0, 63, 2, 63, 2, 63, 2, 63, 4, 63, 4, 63,
      6, 63, 6, 63, 6, 63, 8, 63, 8, 63, 8, 63, 10, 63, 10, 63,
      12, 63, 12, 63, 12, 63, 14, 63, 14, 63, 14, 63, 16, 63, 16, 63,
      18, 63, 18, 63, 18, 63, 20, 63, 20, 63, 20, 63, 22, 63, 22, 63
    };

    public static final int[] perm_pos1_arr = {
      0, 63, 0, 63, 2, 63, 2, 63, 2, 63, 4, 63, 4, 63, 4, 63,
      6, 63, 6, 63, 8, 63, 8, 63, 8, 63, 10, 63, 10, 63, 10, 63,
      12, 63, 12, 63, 14, 63, 14, 63, 14, 63, 16, 63, 16, 63, 16, 63,
      18, 63, 18, 63, 20, 63, 20, 63, 20, 63, 22, 63, 22, 63, 22, 63
    };

    public static final VectorSpecies<Byte> B128 = ByteVector.SPECIES_128;
    public static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    public static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    public static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    public static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    public static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    public static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B128, 4095);
    public static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x2424242424242424L);
    public static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    public static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    public static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer3() {
      super(3);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B128, in, inPos, inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(7));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B128, in, inPos, in.order())
        .castShape(S512, 0)
        .reinterpretAsBytes();
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(7));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer4 extends BytePacker {
    private static final VectorSpecies<Byte> BSPECIES = ByteVector.SPECIES_128;
    private static final VectorSpecies<Integer> ISPECIES = IntVector.SPECIES_512;
    private static final int[] perm_index = {
      0, 63, 0, 63, 4, 63, 4, 63,
      8, 63, 8, 63, 12, 63, 12, 63,
      16, 63, 16, 63, 20, 63, 20, 63,
      24, 63, 24, 63, 28, 63, 28, 63,
      32, 63, 32, 63, 36, 63, 36, 63,
      40, 63, 40, 63, 44, 63, 44, 63,
      48, 63, 48, 63, 52, 63, 52, 63,
      56, 63, 56, 63, 60, 63, 60, 63
    };
    private static final VectorShuffle<Byte> perm_mask = VectorShuffle.fromArray(ByteVector.SPECIES_512, perm_index, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer4() {
      super(4);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(BSPECIES, in, inPos);
      ShortVector tempRes = byteVector.castShape(ISPECIES, 0).reinterpretAsBytes()
        .rearrange(perm_mask)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR,
          ISPECIES.broadcast(0x00040000L)
            .reinterpretAsShorts())
        .lanewise(VectorOperators.AND, 15);
      tempRes.castShape(ISPECIES, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(ISPECIES, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(BSPECIES, in, inPos, in.order());
      ShortVector tempRes = byteVector.castShape(ISPECIES, 0).reinterpretAsBytes()
        .rearrange(perm_mask)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHR,
          ISPECIES.broadcast(0x00040000L)
            .reinterpretAsShorts())
        .lanewise(VectorOperators.AND, 15);
      tempRes.castShape(ISPECIES, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes.castShape(ISPECIES, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer5 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3,
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3
    };

    private static final short[] lshift_cnt_arr = {
      0, 3, 5, 1, 4, 1, 2, 3, 0, 3, 5, 1, 4, 1, 2, 3,
      0, 3, 5, 1, 4, 1, 2, 3, 0, 3, 5, 1, 4, 1, 2, 3
    };

    private static final short[] and_val_arr = {
      31, 7, 31, 1, 15, 31, 3, 31, 31, 7, 31, 1, 15, 31, 3, 31,
      31, 7, 31, 1, 15, 31, 3, 31, 31, 7, 31, 1, 15, 31, 3, 31,
    };

    private static final int[] perm_pos0_arr = {
      0, 63, 0, 63, 2, 63, 2, 63, 4, 63, 6, 63, 6, 63, 8, 63,
      10, 63, 10, 63, 12, 63, 12, 63, 14, 63, 16, 63, 16, 63, 18, 63,
      20, 63, 20, 63, 22, 63, 22, 63, 24, 63, 26, 63, 26, 63, 28, 63,
      30, 63, 30, 63, 32, 63, 32, 63, 34, 63, 36, 63, 36, 63, 38, 63
    };

    public static final int[] perm_pos1_arr = {
      0, 63, 2, 63, 2, 63, 4, 63, 6, 63, 6, 63, 8, 63, 8, 63,
      10, 63, 12, 63, 12, 63, 14, 63, 16, 63, 16, 63, 18, 63, 18, 63,
      20, 63, 22, 63, 22, 63, 24, 63, 26, 63, 26, 63, 28, 63, 28, 63,
      30, 63, 32, 63, 32, 63, 34, 63, 36, 63, 36, 63, 38, 63, 38, 63
    };
    private static final VectorSpecies<Byte> B256 = ByteVector.SPECIES_256;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B256, 0xfffff);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x5A5A5A5A5A5A5A5AL);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer5() {
      super(5);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B256, in, inPos, inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();

      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(31));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B256, in, inPos, in.order(), inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();

      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(31));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer6 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2,
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2
    };

    private static final short[] lshift_cnt_arr = {
      0, 2, 4, 2, 0, 2, 4, 2, 0, 2, 4, 2, 0, 2, 4, 2,
      0, 2, 4, 2, 0, 2, 4, 2, 0, 2, 4, 2, 0, 2, 4, 2,
    };

    private static final short[] and_val_arr = {
      63, 3, 15, 63, 63, 3, 15, 63, 63, 3, 15, 63, 63, 3, 15, 63,
      63, 3, 15, 63, 63, 3, 15, 63, 63, 3, 15, 63, 63, 3, 15, 63
    };

    private static final int[] perm_pos0_arr = {
      0, 63, 0, 63, 2, 63, 4, 63, 6, 63, 6, 63, 8, 63, 10, 63,
      12, 63, 12, 63, 14, 63, 16, 63, 18, 63, 18, 63, 20, 63, 22, 63,
      24, 63, 24, 63, 26, 63, 28, 63, 30, 63, 30, 63, 32, 63, 34, 63,
      36, 63, 36, 63, 38, 63, 40, 63, 42, 63, 42, 63, 44, 63, 46, 63
    };

    private static final int[] perm_pos1_arr = {
      0, 63, 2, 63, 4, 63, 4, 63, 6, 63, 8, 63, 10, 63, 10, 63,
      12, 63, 14, 63, 16, 63, 16, 63, 18, 63, 20, 63, 22, 63, 22, 63,
      24, 63, 26, 63, 28, 63, 28, 63, 30, 63, 32, 63, 34, 63, 34, 63,
      36, 63, 38, 63, 40, 63, 40, 63, 42, 63, 44, 63, 46, 63, 46, 63
    };
    private static final VectorSpecies<Byte> B256 = ByteVector.SPECIES_256;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B256, 0xffffff);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x6666666666666666L);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer6() {
      super(6);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B256, in, inPos, inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();

      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(63));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B256, in, inPos, in.order(), inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();

      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(63));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer7 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1,
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1,
    };

    private static final short[] lshift_cnt_arr = {
      0, 1, 2, 3, 4, 5, 6, 1, 0, 1, 2, 3, 4, 5, 6, 1,
      0, 1, 2, 3, 4, 5, 6, 1, 0, 1, 2, 3, 4, 5, 6, 1,
    };

    private static final short[] and_val_arr = {
      127, 1, 3, 7, 15, 31, 63, 127, 127, 1, 3, 7, 15, 31, 63, 127,
      127, 1, 3, 7, 15, 31, 63, 127, 127, 1, 3, 7, 15, 31, 63, 127
    };

    private static final int[] perm_pos0_arr = {
      0, 63, 0, 63, 2, 63, 4, 63, 6, 63, 8, 63, 10, 63, 12, 63,
      14, 63, 14, 63, 16, 63, 18, 63, 20, 63, 22, 63, 24, 63, 26, 63,
      28, 63, 28, 63, 30, 63, 32, 63, 34, 63, 36, 63, 38, 63, 40, 63,
      42, 63, 42, 63, 44, 63, 46, 63, 48, 63, 50, 63, 52, 63, 54, 63
    };

    private static final int[] perm_pos1_arr = {
      0, 63, 2, 63, 4, 63, 6, 63, 8, 63, 10, 63, 12, 63, 12, 63,
      14, 63, 16, 63, 18, 63, 20, 63, 22, 63, 24, 63, 26, 63, 26, 63,
      28, 63, 30, 63, 32, 63, 34, 63, 36, 63, 38, 63, 40, 63, 40, 63,
      42, 63, 44, 63, 46, 63, 48, 63, 50, 63, 52, 63, 54, 63, 54, 63
    };
    private static final VectorSpecies<Byte> B256 = ByteVector.SPECIES_256;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B256, 0xfffffff);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x7e7e7e7e7e7e7e7eL);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer7() {
      super(7);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B256, in, inPos, inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(127));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B256, in, inPos, in.order(), inp_mask)
        .castShape(S512, 0)
        .reinterpretAsBytes();
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, S512.broadcast(127));

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer8 extends BytePacker {
    private static final VectorSpecies<Integer> ISPECIES = IntVector.SPECIES_512;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;

    private int unpackCount = 64;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer8() {
      super(8);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos);

      byteVector.castShape(ISPECIES, 0).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos);
      byteVector.castShape(ISPECIES, 1).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 16);
      byteVector.castShape(ISPECIES, 2).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 32);
      byteVector.castShape(ISPECIES, 3).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 48);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order());
      byteVector.castShape(ISPECIES, 0).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos);
      byteVector.castShape(ISPECIES, 1).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 16);
      byteVector.castShape(ISPECIES, 2).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 32);
      byteVector.castShape(ISPECIES, 3).lanewise(VectorOperators.AND, 255).reinterpretAsInts().intoArray(out, outPos + 48);
    }
  }

  private static final class Packer9 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7
    };
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8,
      9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 17,
      18, 19, 19, 20, 20, 21, 21, 22, 22, 23, 23, 24, 24, 25, 25, 26,
      27, 28, 28, 29, 29, 30, 30, 31, 31, 32, 32, 33, 33, 34, 34, 35
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xfffffffffL);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer9() {
      super(9);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 511);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 511);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer10 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6,
      0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6,
    };
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 2, 3, 3, 4,
      5, 6, 6, 7, 7, 8, 8, 9,
      10, 11, 11, 12, 12, 13, 13, 14,
      15, 16, 16, 17, 17, 18, 18, 19,
      20, 21, 21, 22, 22, 23, 23, 24,
      25, 26, 26, 27, 27, 28, 28, 29,
      30, 31, 31, 32, 32, 33, 33, 34,
      35, 36, 36, 37, 37, 38, 38, 39
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer10() {
      super(10);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 1023);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order());
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 1023);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer11 extends BytePacker {
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 2, 3, 4, 5,
      5, 6, 6, 7, 8, 9, 9, 10,
      11, 12, 12, 13, 13, 14, 15, 16,
      16, 17, 17, 18, 19, 20, 20, 21,
      22, 23, 23, 24, 24, 25, 26, 27,
      27, 28, 28, 29, 30, 31, 31, 32,
      33, 34, 34, 35, 35, 36, 37, 38,
      38, 39, 39, 40, 41, 42, 42, 43
    };
    private static final short[] rshift_cnt_arr = {
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5,
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5
    };
    private static final int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 4, 63, 63,
      63, 63, 63, 8, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 15, 63, 63,
      63, 63, 63, 19, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 26, 63, 63,
      63, 63, 63, 30, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 37, 63, 63,
      63, 63, 63, 41, 63, 63, 63, 63
    };
    private static final short[] lshift_cnt_arr = {
      0, 0, 2, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 1, 0, 0,
      0, 0, 2, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 1, 0, 0
    };
    private static final short[] and_val_arr = {
      2047, 2047, 1023, 2047, 2047, 511, 2047, 2047, 2047, 2047, 1023, 2047, 2047, 511, 2047, 2047,
      2047, 2047, 1023, 2047, 2047, 511, 2047, 2047, 2047, 2047, 1023, 2047, 2047, 511, 2047, 2047
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xfffffffffffL);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x2424242424242424L);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    ;
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer11() {
      super(11);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 2047);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 2047);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer12 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4,
      0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4,
    };
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 3, 4, 4, 5,
      6, 7, 7, 8, 9, 10, 10, 11,
      12, 13, 13, 14, 15, 16, 16, 17,
      18, 19, 19, 20, 21, 22, 22, 23,
      24, 25, 25, 26, 27, 28, 28, 29,
      30, 31, 31, 32, 33, 34, 34, 35,
      36, 37, 37, 38, 39, 40, 40, 41,
      42, 43, 43, 44, 45, 46, 46, 47
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer12() {
      super(12);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 4095);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order());
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 4095);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer13 extends BytePacker {
    private static final short[] rshift_cnt_arr = {
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3,
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3
    };

    private static final short[] lshift_cnt_arr = {
      0, 3, 0, 1, 4, 0, 2, 0, 0, 3, 0, 1, 4, 0, 2, 0,
      0, 3, 0, 1, 4, 0, 2, 0, 0, 3, 0, 1, 4, 0, 2, 0
    };
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 3, 4, 4, 5,
      6, 7, 8, 9, 9, 10, 11, 12,
      13, 14, 14, 15, 16, 17, 17, 18,
      19, 20, 21, 22, 22, 23, 24, 25,
      26, 27, 27, 28, 29, 30, 30, 31,
      32, 33, 34, 35, 35, 36, 37, 38,
      39, 40, 40, 41, 42, 43, 43, 44,
      45, 46, 47, 48, 48, 49, 50, 51
    };
    private static final int[] perm_pos1_arr = {
      63, 63, 63, 3, 63, 63, 63, 6, 63, 8, 63, 63, 63, 11, 63, 63,
      63, 63, 63, 16, 63, 63, 63, 19, 63, 21, 63, 63, 63, 24, 63, 63,
      63, 63, 63, 29, 63, 63, 63, 32, 63, 34, 63, 63, 63, 37, 63, 63,
      63, 63, 63, 42, 63, 63, 63, 45, 63, 47, 63, 63, 63, 50, 63, 63
    };

    private static final short[] and_val_arr = {
      8191, 2047, 8191, 511, 4095, 8191, 1023, 8191, 8191, 2047, 8191, 511, 4095, 8191, 1023, 8191,
      8191, 2047, 8191, 511, 4095, 8191, 1023, 8191, 8191, 2047, 8191, 511, 4095, 8191, 1023, 8191
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xfffffffffffffL);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x5a5a5a5a5a5a5a5aL);
    ;
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer13() {
      super(13);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 8191);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 8191);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer14 extends BytePacker {
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 3, 4, 5, 6,
      7, 8, 8, 9, 10, 11, 12, 13,
      14, 15, 15, 16, 17, 18, 19, 20,
      21, 22, 22, 23, 24, 25, 26, 27,
      28, 29, 29, 30, 31, 32, 33, 34,
      35, 36, 36, 37, 38, 39, 40, 41,
      42, 43, 43, 44, 45, 46, 47, 48,
      49, 50, 50, 51, 52, 53, 54, 55
    };
    private static final short[] rshift_cnt_arr = {
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2,
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2
    };
    private static final short[] and_val_arr = {
      16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383,
      16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383, 16383, 1023, 4095, 16383
    };
    private static final int[] perm_pos1_arr = {
      63, 63, 63, 3, 63, 5, 63, 63,
      63, 63, 63, 10, 63, 12, 63, 63,
      63, 63, 63, 17, 63, 19, 63, 63,
      63, 63, 63, 24, 63, 26, 63, 63,
      63, 63, 63, 31, 63, 33, 63, 63,
      63, 63, 63, 38, 63, 40, 63, 63,
      63, 63, 63, 45, 63, 47, 63, 63,
      63, 63, 63, 52, 63, 54, 63, 63
    };
    private static final short[] lshift_cnt_arr = {
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0,
      0, 2, 4, 0
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xffffffffffffffL);
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final VectorMask<Short> or_mask = VectorMask.fromLong(S512, 0x6666666666666666L);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer14() {
      super(14);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 16383);

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);

      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 16383);

      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2, or_mask);

      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer15 extends BytePacker {
    private static final int[] perm_pos0_arr = {
      0, 1, 1, 2, 3, 4, 5, 6,
      7, 8, 9, 10, 11, 12, 13, 14,
      15, 16, 16, 17, 18, 19, 20, 21,
      22, 23, 24, 25, 26, 27, 28, 29,
      30, 31, 31, 32, 33, 34, 35, 36,
      37, 38, 39, 40, 41, 42, 43, 44,
      45, 46, 46, 47, 48, 49, 50, 51,
      52, 53, 54, 55, 56, 57, 58, 59
    };

    private static final short[] rshift_cnt_arr = {
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1,
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1
    };

    private static final int[] perm_pos1_arr = {
      63, 63, 63, 3, 63, 5, 63, 7,
      63, 9, 63, 11, 63, 13, 63, 63,
      63, 63, 63, 18, 63, 20, 63, 22,
      63, 24, 63, 26, 63, 28, 63, 63,
      63, 63, 63, 33, 63, 35, 63, 37,
      63, 39, 63, 41, 63, 43, 63, 63,
      63, 63, 63, 48, 63, 50, 63, 52,
      63, 54, 63, 56, 63, 58, 63, 63
    };
    private static final short[] lshift_cnt_arr = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7
    };
    private static final short[] and_val_arr = {
      32767, 511, 1023, 2047, 4095, 8191, 16383, 32767, 32767, 511, 1023, 2047, 4095, 8191, 16383, 32767,
      32767, 511, 1023, 2047, 4095, 8191, 16383, 32767, 32767, 511, 1023, 2047, 4095, 8191, 16383, 32767
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Short> S512 = ShortVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorShuffle<Byte> perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xfffffffffffffffL);
    private static final Vector<Short> rshift_cnt = ShortVector.fromArray(S512, rshift_cnt_arr, 0);
    private static final Vector<Short> lshift_cnt = ShortVector.fromArray(S512, lshift_cnt_arr, 0);
    private static final Vector<Short> and_vec = ShortVector.fromArray(S512, and_val_arr, 0);

    private int unpackCount = 32;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer15() {
      super(15);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 32767);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, and_vec);
      ShortVector tempRes2 = byteVector.rearrange(perm_mask1)
        .reinterpretAsShorts()
        .lanewise(VectorOperators.LSHL, lshift_cnt)
        .lanewise(VectorOperators.AND, 32767);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);
      tempRes1.castShape(I512, 0).reinterpretAsInts().intoArray(out, outPos);
      tempRes1.castShape(I512, 1).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer16 extends BytePacker {
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xffffffffffffffffL);

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer16() {
      super(16);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      ShortVector shortVector = byteVector.reinterpretAsShorts();
      shortVector.castShape(I512, 0).lanewise(VectorOperators.AND, 65535).reinterpretAsInts().intoArray(out, outPos);
      shortVector.castShape(I512, 1).lanewise(VectorOperators.AND, 65535).reinterpretAsInts().intoArray(out, outPos + 16);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      ShortVector shortVector = byteVector.reinterpretAsShorts();
      shortVector.castShape(I512, 0).lanewise(VectorOperators.AND, 65535).reinterpretAsInts().intoArray(out, outPos);
      shortVector.castShape(I512, 1).lanewise(VectorOperators.AND, 65535).reinterpretAsInts().intoArray(out, outPos + 16);
    }
  }

  private static final class Packer17 extends BytePacker {
    private static final int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 63,
      4, 5, 6, 63, 6, 7, 8, 63,
      8, 9, 10, 63, 10, 11, 12, 63,
      12, 13, 14, 63, 14, 15, 16, 63,
      17, 18, 19, 63, 19, 20, 21, 63,
      21, 22, 23, 63, 23, 24, 25, 63,
      25, 26, 27, 63, 27, 28, 29, 63,
      29, 30, 31, 63, 31, 32, 33, 63
    };
    private static final int[] rshift_cnt_arr = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0x3ffffffffL);
    private static final VectorMask<Integer> out_mask = VectorMask.fromLong(I512, 0xffffL);
    private static final Vector<Integer> rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer17() {
      super(17);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 131071);
      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 131071);
      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer18 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 63,
      4, 5, 6, 63, 6, 7, 8, 63,
      9, 10, 11, 63, 11, 12, 13, 63,
      13, 14, 15, 63, 15, 16, 17, 63,
      18, 19, 20, 63, 20, 21, 22, 63,
      22, 23, 24, 63, 24, 25, 26, 63,
      27, 28, 29, 63, 29, 30, 31, 63,
      31, 32, 33, 63, 33, 34, 35, 63
    };
    private static int[] rshift_cnt_arr = {
      0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6
    };

    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xfffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer18() {
      super(18);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 262143);
      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 262143);
      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer19 extends BytePacker {
    private static int[] perm_pos0_arr = {0, 1, 2, 63, 2, 3, 4, 63, 4, 5, 6, 7, 7, 8, 9, 63, 9, 10, 11, 63, 11, 12, 13, 14, 14, 15, 16, 63, 16, 17, 18, 63, 19, 20, 21, 63, 21, 22, 23, 63, 23, 24, 25, 26, 26, 27, 28, 63, 28, 29, 30, 63, 30, 31, 32, 33, 33, 34, 35, 63, 35, 36, 37, 63};
    private static int[] rshift_cnt_arr = {
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5
    };

    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    static {
      inp_mask = VectorMask.fromLong(B512, 0x3fffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer19() {
      super(19);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 524287);
      tempRes1.intoArray(out, outPos, out_mask);

    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 524287);
      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer20 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 63, 5, 6, 7, 63, 7, 8, 9, 63, 10, 11, 12, 63, 12, 13, 14, 63, 15, 16, 17, 63, 17, 18, 19, 63, 20, 21, 22, 63, 22, 23, 24, 63, 25, 26, 27, 63, 27, 28, 29, 63, 30, 31, 32, 63, 32, 33, 34, 63, 35, 36, 37, 63, 37, 38, 39, 63
    };
    private static int[] rshift_cnt_arr = {
      0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer20() {
      super(20);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 1048575);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 1048575);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer21 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 5, 5, 6, 7, 63, 7, 8, 9, 10, 10, 11, 12, 13, 13, 14, 15, 63, 15, 16, 17, 18, 18, 19, 20, 63, 21, 22, 23, 63, 23, 24, 25, 26, 26, 27, 28, 63, 28, 29, 30, 31, 31, 32, 33, 34, 34, 35, 36, 63, 36, 37, 38, 39, 39, 40, 41, 63
    };

    private static int[] rshift_cnt_arr = {
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3ffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer21() {
      super(21);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 2097151);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 2097151);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer22 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 63, 11, 12, 13, 63, 13, 14, 15, 16, 16, 17, 18, 19, 19, 20, 21, 63, 22, 23, 24, 63, 24, 25, 26, 27, 27, 28, 29, 30, 30, 31, 32, 63, 33, 34, 35, 63, 35, 36, 37, 38, 38, 39, 40, 41, 41, 42, 43, 63};

    private static int[] rshift_cnt_arr = {
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xfffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer22() {
      super(22);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 4194303);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 4194303);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer23 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 2, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 11, 11, 12, 13, 14, 14, 15, 16, 17, 17, 18, 19, 20, 20, 21, 22, 63, 23, 24, 25, 63, 25, 26, 27, 28, 28, 29, 30, 31, 31, 32, 33, 34, 34, 35, 36, 37, 37, 38, 39, 40, 40, 41, 42, 43, 43, 44, 45, 63
    };
    private static int[] rshift_cnt_arr = {
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3fffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer23() {
      super(23);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 8388607);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 8388607);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer24 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 63, 3, 4, 5, 63,
      6, 7, 8, 63, 9, 10, 11, 63,
      12, 13, 14, 63, 15, 16, 17, 63,
      18, 19, 20, 63, 21, 22, 23, 63,
      24, 25, 26, 27, 27, 28, 29, 63,
      30, 31, 32, 63, 33, 34, 35, 63,
      36, 37, 38, 63, 39, 40, 41, 63,
      42, 43, 44, 63, 45, 46, 47, 63
    };
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;
    private static final VectorShuffle<Byte> perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
    private static final VectorMask<Byte> inp_mask = VectorMask.fromLong(B512, 0xffffffffffffL);
    private static final VectorMask<Integer> out_mask = VectorMask.fromLong(I512, 0xffffL);

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer24() {
      super(24);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.AND, 16777215);
      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.AND, 16777215);
      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer25 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12, 12, 13, 14, 15, 15, 16, 17, 18, 18, 19, 20, 21, 21, 22, 23, 24, 25, 26, 27, 28, 28, 29, 30, 31, 31, 32, 33, 34, 34, 35, 36, 37, 37, 38, 39, 40, 40, 41, 42, 43, 43, 44, 45, 46, 46, 47, 48, 49
    };
    private static int[] rshift_cnt_arr = {
      0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3ffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);

    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer25() {
      super(25);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();

    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 33554431);

      tempRes1.intoArray(out, outPos, out_mask);

    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 33554431);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer26 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12, 13, 14, 15, 16, 16, 17, 18, 19, 19, 20, 21, 22, 22, 23, 24, 25, 26, 27, 28, 29, 29, 30, 31, 32, 32, 33, 34, 35, 35, 36, 37, 38, 39, 40, 41, 42, 42, 43, 44, 45, 45, 46, 47, 48, 48, 49, 50, 51
    };
    private static int[] rshift_cnt_arr = {
      0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6, 0, 2, 4, 6
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xfffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      rshift_cnt = IntVector.fromArray(I512, rshift_cnt_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer26() {
      super(26);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();

    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 67108863);

      tempRes1.intoArray(out, outPos, out_mask);

    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt)
        .lanewise(VectorOperators.AND, 67108863);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer27 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6,
      6, 7, 8, 9, 10, 11, 12, 13,
      13, 14, 15, 16, 16, 17, 18, 19,
      20, 21, 22, 23, 23, 24, 25, 26,
      27, 28, 29, 30, 30, 31, 32, 33,
      33, 34, 35, 36, 37, 38, 39, 40,
      40, 41, 42, 43, 43, 44, 45, 46,
      47, 48, 49, 50, 50, 51, 52, 53
    };

    private static int[] and_val_arr = {
      134217727, 134217727, 67108863, 134217727, 134217727, 33554431, 134217727, 134217727, 134217727, 134217727, 67108863, 134217727, 134217727, 33554431, 134217727, 134217727
    };

    private static int[] rshift_cnt0_arr = {
      0, 3, 6, 1, 4, 7, 2, 5, 0, 3, 6, 1, 4, 7, 2, 5
    };

    private static int[] lshift_cnt0_arr = {
      0, 0, 2, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 1, 0, 0
    };

    private static int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 63, 63, 63,
      63, 63, 63, 10, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 63, 63, 20,
      63, 63, 63, 63, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 63, 63, 63,
      63, 63, 63, 37, 63, 63, 63, 63,
      63, 63, 63, 63, 63, 63, 63, 47,
      63, 63, 63, 63, 63, 63, 63, 63
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorShuffle<Byte> perm_mask1;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt0;
    private static Vector<Integer> lshift_cnt0;
    private static Vector<Integer> and_vec;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3fffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
      and_vec = IntVector.fromArray(I512, and_val_arr, 0);

      rshift_cnt0 = IntVector.fromArray(I512, rshift_cnt0_arr, 0);
      lshift_cnt0 = IntVector.fromArray(I512, lshift_cnt0_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer27() {
      super(27);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 134217727);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 134217727);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer28 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 12, 13, 14, 15, 16, 17, 17, 18, 19, 20, 21, 22, 23, 24, 24, 25, 26, 27, 28, 29, 30, 31, 31, 32, 33, 34, 35, 36, 37, 38, 38, 39, 40, 41, 42, 43, 44, 45, 45, 46, 47, 48, 49, 50, 51, 52, 52, 53, 54, 55};


    private static int[] and_val_arr = {
      268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455, 268435455};


    private static int[] rshift_cnt0_arr = {
      0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4, 0, 4
    };

    private static int[] lshift_cnt0_arr = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

    private static int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorShuffle<Byte> perm_mask1;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt0;
    private static Vector<Integer> lshift_cnt0;
    private static Vector<Integer> and_vec;

    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xffffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
      and_vec = IntVector.fromArray(I512, and_val_arr, 0);

      rshift_cnt0 = IntVector.fromArray(I512, rshift_cnt0_arr, 0);
      lshift_cnt0 = IntVector.fromArray(I512, lshift_cnt0_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer28() {
      super(28);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 268435455);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 268435455);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer29 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 32, 33, 34, 35, 36, 37, 38, 39, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 50, 51, 52, 53, 54, 55, 56, 57
    };

    private static int[] and_val_arr = {
      536870911, 134217727, 536870911, 33554431, 268435455, 536870911, 67108863, 536870911, 536870911, 134217727, 536870911, 33554431, 268435455, 536870911, 67108863, 536870911
    };

    private static int[] rshift_cnt0_arr = {
      0, 5, 2, 7, 4, 1, 6, 3, 0, 5, 2, 7, 4, 1, 6, 3
    };

    private static int[] lshift_cnt0_arr = {
      0, 3, 0, 1, 4, 0, 2, 0, 0, 3, 0, 1, 4, 0, 2, 0
    };

    private static int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 63, 63, 7, 63, 63, 63, 63, 63, 63, 63, 14, 63, 63, 63, 18, 63, 63, 63, 63, 63, 63, 63, 25, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 36, 63, 63, 63, 63, 63, 63, 63, 43, 63, 63, 63, 47, 63, 63, 63, 63, 63, 63, 63, 54, 63, 63, 63, 63
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorShuffle<Byte> perm_mask1;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt0;
    private static Vector<Integer> lshift_cnt0;
    private static Vector<Integer> and_vec;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3ffffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
      and_vec = IntVector.fromArray(I512, and_val_arr, 0);

      rshift_cnt0 = IntVector.fromArray(I512, rshift_cnt0_arr, 0);
      lshift_cnt0 = IntVector.fromArray(I512, lshift_cnt0_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer29() {
      super(29);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 536870911);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);

    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 536870911);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer30 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59
    };

    private static int[] and_val_arr = {
      1073741823, 67108863, 268435455, 1073741823, 1073741823, 67108863, 268435455, 1073741823, 1073741823, 67108863, 268435455, 1073741823, 1073741823, 67108863, 268435455, 1073741823
    };

    private static int[] rshift_cnt0_arr = {
      0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2, 0, 6, 4, 2
    };

    private static int[] lshift_cnt0_arr = {
      0, 2, 4, 0, 0, 2, 4, 0, 0, 2, 4, 0, 0, 2, 4, 0
    };

    private static int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 63, 63, 7, 63, 63, 63, 11, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 22, 63, 63, 63, 26, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 37, 63, 63, 63, 41, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 52, 63, 63, 63, 56, 63, 63, 63, 63
    };
    private static VectorShuffle<Byte> perm_mask0;
    private static VectorShuffle<Byte> perm_mask1;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt0;
    private static Vector<Integer> lshift_cnt0;
    private static Vector<Integer> and_vec;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xfffffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
      and_vec = IntVector.fromArray(I512, and_val_arr, 0);

      rshift_cnt0 = IntVector.fromArray(I512, rshift_cnt0_arr, 0);
      lshift_cnt0 = IntVector.fromArray(I512, lshift_cnt0_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer30() {
      super(30);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 1073741823);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 1073741823);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer31 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61
    };

    private static int[] and_val_arr = {
      2147483647, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647, 2147483647, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647
    };

    private static int[] rshift_cnt0_arr = {
      0, 7, 6, 5, 4, 3, 2, 1, 0, 7, 6, 5, 4, 3, 2, 1
    };

    private static int[] lshift_cnt0_arr = {
      0, 1, 2, 3, 4, 5, 6, 0, 0, 1, 2, 3, 4, 5, 6, 0
    };

    private static int[] perm_pos1_arr = {
      63, 63, 63, 63, 63, 63, 63, 7, 63, 63, 63, 11, 63, 63, 63, 15, 63, 63, 63, 19, 63, 63, 63, 23, 63, 63, 63, 27, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 38, 63, 63, 63, 42, 63, 63, 63, 46, 63, 63, 63, 50, 63, 63, 63, 54, 63, 63, 63, 58, 63, 63, 63, 63};


    private static VectorShuffle<Byte> perm_mask0;
    private static VectorShuffle<Byte> perm_mask1;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static Vector<Integer> rshift_cnt0;
    private static Vector<Integer> lshift_cnt0;
    private static Vector<Integer> and_vec;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0x3fffffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);
      perm_mask1 = VectorShuffle.fromArray(B512, perm_pos1_arr, 0);
      and_vec = IntVector.fromArray(I512, and_val_arr, 0);

      rshift_cnt0 = IntVector.fromArray(I512, rshift_cnt0_arr, 0);
      lshift_cnt0 = IntVector.fromArray(I512, lshift_cnt0_arr, 0);
      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer31() {
      super(31);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();

    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 2147483647);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);

    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts()
        .lanewise(VectorOperators.ASHR, rshift_cnt0)
        .lanewise(VectorOperators.AND, and_vec);

      IntVector tempRes2 = byteVector.rearrange(perm_mask1).
        reinterpretAsInts()
        .lanewise(VectorOperators.LSHL, lshift_cnt0)
        .lanewise(VectorOperators.AND, 2147483647);
      tempRes1 = tempRes1.lanewise(VectorOperators.OR, tempRes2);

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static final class Packer32 extends BytePacker {
    private static int[] perm_pos0_arr = {
      0, 1, 2, 3, 4, 5, 6, 7,
      8, 9, 10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23,
      24, 25, 26, 27, 28, 29, 30, 31,
      32, 33, 34, 35, 36, 37, 38, 39,
      40, 41, 42, 43, 44, 45, 46, 47,
      48, 49, 50, 51, 52, 53, 54, 55,
      56, 57, 58, 59, 60, 61, 62, 63
    };

    private static VectorShuffle<Byte> perm_mask0;
    private static VectorMask<Byte> inp_mask;
    private static VectorMask<Integer> out_mask;
    private static final VectorSpecies<Byte> B512 = ByteVector.SPECIES_512;
    private static final VectorSpecies<Integer> I512 = IntVector.SPECIES_512;

    static {
      inp_mask = VectorMask.fromLong(B512, 0xffffffffffffffffL);
      perm_mask0 = VectorShuffle.fromArray(B512, perm_pos0_arr, 0);


      out_mask = VectorMask.fromLong(I512, 0xffffL);
    }

    private int unpackCount = 16;

    public int getUnpackCount() {
      return unpackCount;
    }

    private Packer32() {
      super(32);
    }

    public final void pack8Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void pack32Values(final int[] in, final int inPos, final byte[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack8Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final byte[] in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpack32Values(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      notSupport();
    }

    public final void unpackValuesUsingVector(final byte[] in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromArray(B512, in, inPos, inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts();

      tempRes1.intoArray(out, outPos, out_mask);
    }

    public final void unpackValuesUsingVector(final ByteBuffer in, final int inPos, final int[] out, final int outPos) {
      ByteVector byteVector = ByteVector.fromByteBuffer(B512, in, inPos, in.order(), inp_mask);
      IntVector tempRes1 = byteVector.rearrange(perm_mask0)
        .reinterpretAsInts();

      tempRes1.intoArray(out, outPos, out_mask);
    }
  }

  private static void notSupport() {
    throw new RuntimeException("ByteBitPacking512VectorLE doesn't support the function, please use ByteBitPackingLE!");
  }
}
