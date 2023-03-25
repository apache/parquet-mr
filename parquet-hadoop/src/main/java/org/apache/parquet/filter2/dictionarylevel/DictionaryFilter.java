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
package org.apache.parquet.filter2.dictionarylevel;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Applies filters based on the contents of column dictionaries.
 */
public class DictionaryFilter implements FilterPredicate.Visitor<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(DictionaryFilter.class);
  private static final boolean BLOCK_MIGHT_MATCH = false;
  private static final boolean BLOCK_CANNOT_MATCH = true;

  public static boolean canDrop(FilterPredicate pred, List<ColumnChunkMetaData> columns, DictionaryPageReadStore dictionaries) {
    Objects.requireNonNull(pred, "pred cannnot be null");
    Objects.requireNonNull(columns, "columns cannnot be null");
    return pred.accept(new DictionaryFilter(columns, dictionaries));
  }

  private final Map<ColumnPath, ColumnChunkMetaData> columns = new HashMap<ColumnPath, ColumnChunkMetaData>();
  private final DictionaryPageReadStore dictionaries;

  private DictionaryFilter(List<ColumnChunkMetaData> columnsList, DictionaryPageReadStore dictionaries) {
    for (ColumnChunkMetaData chunk : columnsList) {
      columns.put(chunk.getPath(), chunk);
    }

    this.dictionaries = dictionaries;
  }

  private ColumnChunkMetaData getColumnChunk(ColumnPath columnPath) {
    return columns.get(columnPath);
  }

  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> Set<T> expandDictionary(ColumnChunkMetaData meta) throws IOException {
    ColumnDescriptor col = new ColumnDescriptor(meta.getPath().toArray(), meta.getPrimitiveType(), -1, -1);
    DictionaryPage page = dictionaries.readDictionaryPage(col);

    // the chunk may not be dictionary-encoded
    if (page == null) {
      return null;
    }

    Dictionary dict = page.getEncoding().initDictionary(col, page);

    IntFunction<Object> dictValueProvider;
    PrimitiveTypeName type = meta.getPrimitiveType().getPrimitiveTypeName();
    switch (type) {
    case FIXED_LEN_BYTE_ARRAY: // Same as BINARY
    case BINARY:
      dictValueProvider = dict::decodeToBinary;
      break;
    case INT32:
      dictValueProvider = dict::decodeToInt;
      break;
    case INT64:
      dictValueProvider = dict::decodeToLong;
      break;
    case FLOAT:
      dictValueProvider = dict::decodeToFloat;
      break;
    case DOUBLE:
      dictValueProvider = dict::decodeToDouble;
      break;
    default:
      LOG.warn("Unsupported dictionary type: {}", type);
      return null;
    }

    Set<T> dictSet = new HashSet<>();
    for (int i = 0; i <= dict.getMaxId(); i++) {
      dictSet.add((T) dictValueProvider.apply(i));
    }
    
    return dictSet;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
    T value = eq.getValue();

    if (value == null) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    Column<T> filterColumn = eq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet != null && !dictSet.contains(value)) {
        return BLOCK_CANNOT_MATCH;
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH; // cannot drop the row group based on this dictionary
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
    Column<T> filterColumn = notEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    T value = notEq.getValue();

    if (value == null && meta == null) {
      // the predicate value is null and all rows have a null value, so the
      // predicate is always false (null != null)
      return BLOCK_CANNOT_MATCH;
    }

    if (value == null) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    if (meta == null) {
      // column is missing from this file and is always null and not equal to
      // the non-null test value, so the predicate is true for all rows
      return BLOCK_MIGHT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      boolean mayContainNull = (meta.getStatistics() == null
          || !meta.getStatistics().isNumNullsSet()
          || meta.getStatistics().getNumNulls() > 0);
      if (dictSet != null && dictSet.size() == 1 && dictSet.contains(value) && !mayContainNull) {
        return BLOCK_CANNOT_MATCH;
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
    Column<T> filterColumn = lt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than a
      // value. for all x, null is never < x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = lt.getValue();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      Comparator<T> comparator = meta.getPrimitiveType().comparator();
      for (T entry : dictSet) {
        if (comparator.compare(value, entry) > 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
    Column<T> filterColumn = ltEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never less than or
      // equal to a value. for all x, null is never <= x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = ltEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      Comparator<T> comparator = meta.getPrimitiveType().comparator();
      for (T entry : dictSet) {
        if (comparator.compare(value, entry) >= 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
    Column<T> filterColumn = gt.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than a
      // value. for all x, null is never > x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gt.getValue();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      Comparator<T> comparator = meta.getPrimitiveType().comparator();
      for (T entry : dictSet) {
        if (comparator.compare(value, entry) < 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
    Column<T> filterColumn = gtEq.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column is missing and always null, which is never greater than or
      // equal to a value. for all x, null is never >= x.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    T value = gtEq.getValue();

    filterColumn.getColumnPath();

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      Comparator<T> comparator = meta.getPrimitiveType().comparator();
      for (T entry : dictSet) {
        if (comparator.compare(value, entry) <= 0) {
          return BLOCK_MIGHT_MATCH;
        }
      }

      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(In<T> in) {
    Set<T> values = in.getValues();

    if (values.contains(null)) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    Column<T> filterColumn = in.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_CANNOT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet != null) {
        return drop(dictSet, values);
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }
    return BLOCK_MIGHT_MATCH; // cannot drop the row group based on this dictionary
  }

  private <T extends Comparable<T>> Boolean drop(Set<T> dictSet, Set<T> values) {
    // we need to find out the smaller set to iterate through
    Set<T> smallerSet;
    Set<T> biggerSet;

    if (values.size() < dictSet.size()) {
      smallerSet = values;
      biggerSet = dictSet;
    } else {
      smallerSet = dictSet;
      biggerSet = values;
    }

    for (T e : smallerSet) {
      if (biggerSet.contains(e)) {
        // value sets intersect so rows match
        return BLOCK_MIGHT_MATCH;
      }
    }
    return BLOCK_CANNOT_MATCH;
  }

  @Override
  public <T extends Comparable<T>> Boolean visit(NotIn<T> notIn) {
    Set<T> values = notIn.getValues();

    Column<T> filterColumn = notIn.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());

    if (values.size() == 1 && values.contains(null) && meta == null) {
      // the predicate value is null and all rows have a null value, so the
      // predicate is always false (null != null)
      return BLOCK_CANNOT_MATCH;
    }

    if (values.contains(null)) {
      // the dictionary contains only non-null values so isn't helpful. this
      // could check the column stats, but the StatisticsFilter is responsible
      return BLOCK_MIGHT_MATCH;
    }

    if (meta == null) {
      // the column isn't in this file so all values are null, but the value
      // must be non-null because of the above check.
      return BLOCK_MIGHT_MATCH;
    }

    boolean mayContainNull = (meta.getStatistics() == null
      || !meta.getStatistics().isNumNullsSet()
      || meta.getStatistics().getNumNulls() > 0);
    // The column may contain nulls and the values set contains no null, so the row group cannot be eliminated.
    if (mayContainNull) {
      return BLOCK_MIGHT_MATCH;
    }

    // if the chunk has non-dictionary pages, don't bother decoding the
    // dictionary because the row group can't be eliminated.
    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet != null) {
        if (dictSet.size() > values.size()) return BLOCK_MIGHT_MATCH;
        // ROWS_CANNOT_MATCH if no values in the dictionary that are not also in the set
        return values.containsAll(dictSet) ? BLOCK_CANNOT_MATCH : BLOCK_MIGHT_MATCH;
      }
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }
    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public Boolean visit(And and) {
    return and.getLeft().accept(this) || and.getRight().accept(this);
  }

  @Override
  public Boolean visit(Or or) {
    return or.getLeft().accept(this) && or.getRight().accept(this);
  }

  @Override
  public Boolean visit(Not not) {
    throw new IllegalArgumentException(
        "This predicate contains a not! Did you forget to run this predicate through LogicalInverseRewriter? " + not);
  }

  private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> ud, boolean inverted) {
    Column<T> filterColumn = ud.getColumn();
    ColumnChunkMetaData meta = getColumnChunk(filterColumn.getColumnPath());
    U udp = ud.getUserDefinedPredicate();

    // The column is missing, thus all null. Check if the predicate keeps null.
    if (meta == null) {
      if (inverted) {
        return udp.acceptsNullValue();
      } else {
        return !udp.acceptsNullValue();
      }
    }

    if (hasNonDictionaryPages(meta)) {
      return BLOCK_MIGHT_MATCH;
    }

    try {
      Set<T> dictSet = expandDictionary(meta);
      if (dictSet == null) {
        return BLOCK_MIGHT_MATCH;
      }

      for (T entry : dictSet) {
        boolean keep = udp.keep(entry);
        if ((keep && !inverted) || (!keep && inverted)) return BLOCK_MIGHT_MATCH;
      }
      return BLOCK_CANNOT_MATCH;
    } catch (IOException e) {
      LOG.warn("Failed to process dictionary for filter evaluation.", e);
    }

    return BLOCK_MIGHT_MATCH;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(UserDefined<T, U> udp) {
    return visit(udp, false);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Boolean visit(LogicalNotUserDefined<T, U> udp) {
    return visit(udp.getUserDefined(), true);
  }

  @SuppressWarnings("deprecation")
  private static boolean hasNonDictionaryPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonDictionaryEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = new HashSet<Encoding>(meta.getEncodings());
    if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      if (encodings.isEmpty()) {
        return false; // no encodings other than dictionary or rep/def levels
      }

      return true;

    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }
}
