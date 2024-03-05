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
package org.apache.parquet.column;

import static org.apache.parquet.column.values.bitpacking.Packer.BIG_ENDIAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.IOException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFLBA;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFloat;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForLong;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainDoubleDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainFloatDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainIntegerDictionary;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainLongDictionary;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.ZeroIntegerValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * encoding of the data
 */
public enum Encoding {
  PLAIN {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      switch (descriptor.getType()) {
        case BOOLEAN:
          return new BooleanPlainValuesReader();
        case BINARY:
          return new BinaryPlainValuesReader();
        case FLOAT:
          return new FloatPlainValuesReader();
        case DOUBLE:
          return new DoublePlainValuesReader();
        case INT32:
          return new IntegerPlainValuesReader();
        case INT64:
          return new LongPlainValuesReader();
        case INT96:
          return new FixedLenByteArrayPlainValuesReader(12);
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedLenByteArrayPlainValuesReader(descriptor.getTypeLength());
        default:
          throw new ParquetDecodingException("no plain reader for type " + descriptor.getType());
      }
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
        throws IOException {
      switch (descriptor.getType()) {
        case BINARY:
          return new PlainBinaryDictionary(dictionaryPage);
        case FIXED_LEN_BYTE_ARRAY:
          return new PlainBinaryDictionary(dictionaryPage, descriptor.getTypeLength());
        case INT96:
          return new PlainBinaryDictionary(dictionaryPage, 12);
        case INT64:
          return new PlainLongDictionary(dictionaryPage);
        case DOUBLE:
          return new PlainDoubleDictionary(dictionaryPage);
        case INT32:
          return new PlainIntegerDictionary(dictionaryPage);
        case FLOAT:
          return new PlainFloatDictionary(dictionaryPage);
        default:
          throw new ParquetDecodingException(
              "Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }
  },

  /**
   * Actually a combination of bit packing and run length encoding.
   * TODO: Should we rename this to be more clear?
   */
  RLE {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
      if (bitWidth == 0) {
        return new ZeroIntegerValuesReader();
      }
      return new RunLengthBitPackingHybridValuesReader(bitWidth);
    }
  },

  BYTE_STREAM_SPLIT {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      switch (descriptor.getType()) {
        case FLOAT:
          return new ByteStreamSplitValuesReaderForFloat();
        case DOUBLE:
          return new ByteStreamSplitValuesReaderForDouble();
        case INT32:
          return new ByteStreamSplitValuesReaderForInteger();
        case INT64:
          return new ByteStreamSplitValuesReaderForLong();
        case FIXED_LEN_BYTE_ARRAY:
          return new ByteStreamSplitValuesReaderForFLBA(descriptor.getTypeLength());
        default:
          throw new ParquetDecodingException("no byte stream split reader for type " + descriptor.getType());
      }
    }
  },

  /**
   * @deprecated This is no longer used, and has been replaced by {@link #RLE}
   * which is combination of bit packing and rle
   */
  @Deprecated
  BIT_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
    }
  },

  /**
   * @deprecated now replaced by RLE_DICTIONARY for the data page encoding and PLAIN for the dictionary page encoding
   */
  @Deprecated
  PLAIN_DICTIONARY {
    @Override
    public ValuesReader getDictionaryBasedValuesReader(
        ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
      return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary);
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
        throws IOException {
      return PLAIN.initDictionary(descriptor, dictionaryPage);
    }

    @Override
    public boolean usesDictionary() {
      return true;
    }
  },

  /**
   * Delta encoding for integers. This can be used for int columns and works best
   * on sorted data
   */
  DELTA_BINARY_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      if (descriptor.getType() != INT32 && descriptor.getType() != INT64) {
        throw new ParquetDecodingException(
            "Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64");
      }
      return new DeltaBinaryPackingValuesReader();
    }
  },

  /**
   * Encoding for byte arrays to separate the length values and the data. The lengths
   * are encoded using DELTA_BINARY_PACKED
   */
  DELTA_LENGTH_BYTE_ARRAY {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      if (descriptor.getType() != BINARY) {
        throw new ParquetDecodingException(
            "Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
      }
      return new DeltaLengthByteArrayValuesReader();
    }
  },

  /**
   * Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
   * Suffixes are stored as delta length byte arrays.
   */
  DELTA_BYTE_ARRAY {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      if (descriptor.getType() != BINARY && descriptor.getType() != FIXED_LEN_BYTE_ARRAY) {
        throw new ParquetDecodingException(
            "Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
      }
      return new DeltaByteArrayReader();
    }
  },

  /**
   * Dictionary encoding: the ids are encoded using the RLE encoding
   */
  RLE_DICTIONARY {
    @Override
    public ValuesReader getDictionaryBasedValuesReader(
        ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
      switch (descriptor.getType()) {
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
        case INT64:
        case DOUBLE:
        case INT32:
        case FLOAT:
          return new DictionaryValuesReader(dictionary);
        default:
          throw new ParquetDecodingException(
              "Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }

    @Override
    public boolean usesDictionary() {
      return true;
    }
  };

  int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType) {
    int maxLevel;
    switch (valuesType) {
      case REPETITION_LEVEL:
        maxLevel = descriptor.getMaxRepetitionLevel();
        break;
      case DEFINITION_LEVEL:
        maxLevel = descriptor.getMaxDefinitionLevel();
        break;
      case VALUES:
        if (descriptor.getType() == BOOLEAN) {
          maxLevel = 1;
          break;
        }
      default:
        throw new ParquetDecodingException("Unsupported encoding for values: " + this);
    }
    return maxLevel;
  }

  /**
   * @return whether this encoding requires a dictionary
   */
  public boolean usesDictionary() {
    return false;
  }

  /**
   * initializes a dictionary from a page
   *
   * @param descriptor     the column descriptor for the dictionary-encoded column
   * @param dictionaryPage a dictionary page
   * @return the corresponding dictionary
   * @throws IOException                   if there is an exception while reading the dictionary page
   * @throws UnsupportedOperationException if the encoding is not dictionary based
   */
  public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
    throw new UnsupportedOperationException(this.name() + " does not support dictionary");
  }

  /**
   * To read decoded values that don't require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @return the proper values reader for the given column
   * @throws UnsupportedOperationException if the encoding is dictionary based
   */
  public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
    throw new UnsupportedOperationException(
        "Error decoding " + descriptor + ". " + this.name() + " is dictionary based");
  }

  /**
   * To read decoded values that require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @param dictionary the dictionary
   * @return the proper values reader for the given column
   * @throws UnsupportedOperationException if the encoding is not dictionary based
   */
  public ValuesReader getDictionaryBasedValuesReader(
      ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
    throw new UnsupportedOperationException(this.name() + " is not dictionary based");
  }
}
