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
package org.apache.parquet.internal.column.columnindex;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder.ColumnIndexBase;

/**
 * Iterator implementation for page indexes.
 */
public class IndexIterator implements PrimitiveIterator.OfInt {
  public static final PrimitiveIterator.OfInt EMPTY = IntStream.empty().iterator();
  private int index;
  private final int endIndex;
  private final IntPredicate filter;
  private final IntUnaryOperator translator;

  public static PrimitiveIterator.OfInt all(int pageCount) {
    return new IndexIterator(0, pageCount, i -> true, i -> i);
  }

  static PrimitiveIterator.OfInt all(ColumnIndexBase<?>.ValueComparator comparator) {
    return new IndexIterator(0, comparator.arrayLength(), i -> true, comparator::translate);
  }

  public static PrimitiveIterator.OfInt filter(int pageCount, IntPredicate filter) {
    return new IndexIterator(0, pageCount, filter, i -> i);
  }

  static PrimitiveIterator.OfInt filterTranslate(int arrayLength, IntPredicate filter, IntUnaryOperator translator) {
    return new IndexIterator(0, arrayLength, filter, translator);
  }

  static PrimitiveIterator.OfInt rangeTranslate(int from, int to, IntUnaryOperator translator) {
    return new IndexIterator(from, to + 1, i -> true, translator);
  }

  private IndexIterator(int startIndex, int endIndex, IntPredicate filter, IntUnaryOperator translator) {
    this.endIndex = endIndex;
    this.filter = filter;
    this.translator = translator;
    index = nextPageIndex(startIndex);
  }

  private int nextPageIndex(int startIndex) {
    for (int i = startIndex; i < endIndex; ++i) {
      if (filter.test(i)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public boolean hasNext() {
    return index >= 0;
  }

  @Override
  public int nextInt() {
    if (hasNext()) {
      int ret = index;
      index = nextPageIndex(index + 1);
      return translator.applyAsInt(ret);
    }
    throw new NoSuchElementException();
  }
}
