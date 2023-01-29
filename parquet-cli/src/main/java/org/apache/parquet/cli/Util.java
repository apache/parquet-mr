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

package org.apache.parquet.cli;

import com.google.common.base.Ascii;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashCode;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import java.util.Set;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.DELTA_BINARY_PACKED;
import static org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY;


public class Util {

  private static final long KB = 1024;
  private static final long MB = 1024 * KB;
  private static final long GB = 1024 * MB;
  private static final long TB = 1024 * GB;

  public static String humanReadable(float bytes) {
    if (bytes > TB) {
      return String.format("%.03f TB", bytes / TB);
    } else if (bytes > GB) {
      return String.format("%.03f GB", bytes / GB);
    } else if (bytes > MB) {
      return String.format("%.03f MB", bytes / MB);
    } else if (bytes > KB) {
      return String.format("%.03f kB", bytes / KB);
    } else {
      return String.format("%.02f B", bytes);
    }
  }

  public static String humanReadable(long bytes) {
    if (bytes > TB) {
      return String.format("%.03f TB", ((float) bytes) / TB);
    } else if (bytes > GB) {
      return String.format("%.03f GB", ((float) bytes) / GB);
    } else if (bytes > MB) {
      return String.format("%.03f MB", ((float) bytes) / MB);
    } else if (bytes > KB) {
      return String.format("%.03f kB", ((float) bytes) / KB);
    } else {
      return String.format("%d B", bytes);
    }
  }

  @Deprecated
  public static String minMaxAsString(Statistics stats, OriginalType annotation) {
    return minMaxAsString(stats);
  }

  public static String minMaxAsString(Statistics stats) {
    if (stats == null) {
      return "no stats";
    }
    if (!stats.hasNonNullValue()) {
      return "";
    }
    return String.format("%s / %s", humanReadable(stats.minAsString(), 30), humanReadable(stats.maxAsString(), 30));
  }

  @Deprecated
  public static String toString(Statistics stats, long count, OriginalType annotation) {
    return toString(stats, count);
  }

  public static String toString(Statistics stats, long count) {
    if (stats == null) {
      return "no stats";
    }
    return String.format("min: %s max: %s nulls: %d/%d",
        humanReadable(stats.minAsString(), 30), humanReadable(stats.maxAsString(), 30), stats.getNumNulls(), count);
  }

  public static String humanReadable(String str, int len) {
    if (str == null) {
      return "null";
    }

    StringBuilder sb = new StringBuilder();
    sb.append("\"");
    if (str.length() > len - 2) {
      sb.append(str.substring(0, len - 5)).append("...");
    } else {
      sb.append(str);
    }
    sb.append("\"");

    return sb.toString();
  }

  public static String humanReadable(byte[] bytes, int len) {
    Preconditions.checkArgument(len >= 5, "Display length must be minimum 5");
    if (bytes == null || bytes.length == 0) {
      return "null";
    }

    final String asString = HashCode.fromBytes(bytes).toString();
    return "0x" + Ascii.truncate(asString, len - 2, "...");
  }

  public static String shortCodec(CompressionCodecName codec) {
    switch (codec) {
      case UNCOMPRESSED:
        return "_";
      case SNAPPY:
        return "S";
      case GZIP:
        return "G";
      case LZO:
        return "L";
      case BROTLI:
        return "B";
      case LZ4:
        return "4";
      case LZ4_RAW:
        return "F";
      case ZSTD:
        return "Z";
      default:
        return "?";
    }
  }

  public static String encodingAsString(Encoding encoding, boolean isDict) {
    switch (encoding) {
      case PLAIN:
        return "_";
      case PLAIN_DICTIONARY:
        // data pages use RLE, dictionary pages use plain
        return isDict ? "_" : "R";
      case RLE_DICTIONARY:
        return "R";
      case DELTA_BINARY_PACKED:
      case DELTA_LENGTH_BYTE_ARRAY:
      case DELTA_BYTE_ARRAY:
        return "D";
      default:
        return "?";
    }
  }

  public static String encodingStatsAsString(EncodingStats encodingStats) {
    StringBuilder sb = new StringBuilder();
    if (encodingStats.hasDictionaryPages()) {
      for (Encoding encoding: encodingStats.getDictionaryEncodings()) {
        sb.append(encodingAsString(encoding, true));
      }
      sb.append(" ");
    } else {
      sb.append("  ");
    }

    Set<Encoding> encodings = encodingStats.getDataEncodings();
    if (encodings.contains(RLE_DICTIONARY) || encodings.contains(PLAIN_DICTIONARY)) {
      sb.append("R");
    }
    if (encodings.contains(PLAIN)) {
      sb.append("_");
    }
    if (encodings.contains(DELTA_BYTE_ARRAY) ||
        encodings.contains(DELTA_BINARY_PACKED) ||
        encodings.contains(DELTA_LENGTH_BYTE_ARRAY)) {
      sb.append("D");
    }

    // Check for fallback and add a flag
    if (encodingStats.hasDictionaryEncodedPages() && encodingStats.hasNonDictionaryEncodedPages()) {
      sb.append(" F");
    }

    return sb.toString();
  }

  public static String encodingsAsString(Set<Encoding> encodings, ColumnDescriptor desc) {
    StringBuilder sb = new StringBuilder();
    if (encodings.contains(RLE) || encodings.contains(BIT_PACKED)) {
      sb.append(desc.getMaxDefinitionLevel() == 0 ? "B" : "R");
      sb.append(desc.getMaxRepetitionLevel() == 0 ? "B" : "R");
      if (encodings.contains(PLAIN_DICTIONARY)) {
        sb.append("R");
      }
      if (encodings.contains(PLAIN)) {
        sb.append("_");
      }
    } else {
      sb.append("RR");
      if (encodings.contains(RLE_DICTIONARY)) {
        sb.append("R");
      }
      if (encodings.contains(PLAIN)) {
        sb.append("_");
      }
      if (encodings.contains(DELTA_BYTE_ARRAY) ||
          encodings.contains(DELTA_BINARY_PACKED) ||
          encodings.contains(DELTA_LENGTH_BYTE_ARRAY)) {
        sb.append("D");
      }
    }
    return sb.toString();
  }

  private static final Splitter DOT = Splitter.on('.');

  public static ColumnDescriptor descriptor(String column, MessageType schema) {
    String[] path = Iterables.toArray(DOT.split(column), String.class);
    Preconditions.checkArgument(schema.containsPath(path),
        "Schema doesn't have column: " + column);
    return schema.getColumnDescription(path);
  }

  public static String columnName(ColumnDescriptor desc) {
    return Joiner.on('.').join(desc.getPath());
  }

  public static PrimitiveType primitive(MessageType schema, String[] path) {
    Type current = schema;
    for (String part : path) {
      current = current.asGroupType().getType(part);
      if (current.isPrimitive()) {
        return current.asPrimitiveType();
      }
    }
    return null;
  }

  public static PrimitiveType primitive(String column, MessageType schema) {
    String[] path = Iterables.toArray(DOT.split(column), String.class);
    Preconditions.checkArgument(schema.containsPath(path),
        "Schema doesn't have column: " + column);
    return primitive(schema, path);
  }

}
