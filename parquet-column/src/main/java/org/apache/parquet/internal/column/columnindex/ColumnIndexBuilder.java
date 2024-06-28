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

import static java.util.Objects.requireNonNull;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.booleans.BooleanLists;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.IntPredicate;
import org.apache.parquet.column.MinMax;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geometry.GeometryStatistics;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.Contains;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.In;
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.NotIn;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Builder implementation to create {@link ColumnIndex} objects.
 */
public abstract class ColumnIndexBuilder {

  abstract static class ColumnIndexBase<C> implements ColumnIndex {
    /*
     * A class containing the value to be compared to the min/max values. This way we only need to do the deboxing once
     * per predicate execution instead for every comparison.
     */
    abstract class ValueComparator {
      abstract int compareValueToMin(int arrayIndex);

      abstract int compareValueToMax(int arrayIndex);

      int arrayLength() {
        return pageIndexes.length;
      }

      int translate(int arrayIndex) {
        return pageIndexes[arrayIndex];
      }
    }

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final int MAX_VALUE_LENGTH_FOR_TOSTRING = 40;
    private static final String TOSTRING_TRUNCATION_MARKER = "(...)";
    private static final int TOSTRING_TRUNCATION_START_POS =
        (MAX_VALUE_LENGTH_FOR_TOSTRING - TOSTRING_TRUNCATION_MARKER.length()) / 2;
    private static final int TOSTRING_TRUNCATION_END_POS =
        MAX_VALUE_LENGTH_FOR_TOSTRING - TOSTRING_TRUNCATION_MARKER.length() - TOSTRING_TRUNCATION_START_POS;
    private static final String TOSTRING_MISSING_VALUE_MARKER = "<none>";

    final PrimitiveStringifier stringifier;
    final PrimitiveComparator<C> comparator;
    private boolean[] nullPages;
    private BoundaryOrder boundaryOrder;
    // Storing the page index for each array index (min/max values are not stored for null-pages)
    private int[] pageIndexes;
    // might be null
    private long[] nullCounts;
    // might be null
    private long[] repLevelHistogram;
    // might be null
    private long[] defLevelHistogram;
    // might be null
    private GeometryStatistics[] geometryStatistics;

    static String truncate(String str) {
      if (str.length() <= MAX_VALUE_LENGTH_FOR_TOSTRING) {
        return str;
      }
      return str.substring(0, TOSTRING_TRUNCATION_START_POS)
          + TOSTRING_TRUNCATION_MARKER
          + str.substring(str.length() - TOSTRING_TRUNCATION_END_POS);
    }

    ColumnIndexBase(PrimitiveType type) {
      comparator = type.comparator();
      stringifier = type.stringifier();
    }

    @Override
    public BoundaryOrder getBoundaryOrder() {
      return boundaryOrder;
    }

    @Override
    public List<Long> getNullCounts() {
      if (nullCounts == null) {
        return null;
      }
      return LongLists.unmodifiable(LongArrayList.wrap(nullCounts));
    }

    @Override
    public List<Boolean> getNullPages() {
      return BooleanLists.unmodifiable(BooleanArrayList.wrap(nullPages));
    }

    @Override
    public List<ByteBuffer> getMinValues() {
      List<ByteBuffer> list = new ArrayList<>(getPageCount());
      int arrayIndex = 0;
      for (int i = 0, n = getPageCount(); i < n; ++i) {
        if (isNullPage(i)) {
          list.add(EMPTY_BYTE_BUFFER);
        } else {
          list.add(getMinValueAsBytes(arrayIndex++));
        }
      }
      return list;
    }

    @Override
    public List<ByteBuffer> getMaxValues() {
      List<ByteBuffer> list = new ArrayList<>(getPageCount());
      int arrayIndex = 0;
      for (int i = 0, n = getPageCount(); i < n; ++i) {
        if (isNullPage(i)) {
          list.add(EMPTY_BYTE_BUFFER);
        } else {
          list.add(getMaxValueAsBytes(arrayIndex++));
        }
      }
      return list;
    }

    @Override
    public List<Long> getRepetitionLevelHistogram() {
      if (repLevelHistogram == null) {
        return LongList.of();
      }
      return LongLists.unmodifiable(LongArrayList.wrap(repLevelHistogram));
    }

    @Override
    public List<Long> getDefinitionLevelHistogram() {
      if (defLevelHistogram == null) {
        return LongList.of();
      }
      return LongLists.unmodifiable(LongArrayList.wrap(defLevelHistogram));
    }

    @Override
    public List<GeometryStatistics> getGeometryStatistics() {
      List<GeometryStatistics> geomStats = new ArrayList<>();
      if (geometryStatistics != null) {
        for (GeometryStatistics stats : geometryStatistics) {
          geomStats.add(stats.copy());
        }
      }
      return geomStats;
    }

    @Override
    public String toString() {
      try (Formatter formatter = new Formatter()) {
        formatter.format("Boundary order: %s\n", boundaryOrder);
        String minMaxPart =
            "  %-" + MAX_VALUE_LENGTH_FOR_TOSTRING + "s  %-" + MAX_VALUE_LENGTH_FOR_TOSTRING + "s\n";
        formatter.format("%-10s  %20s" + minMaxPart, "", "null count", "min", "max");
        String format = "page-%-5d  %20s" + minMaxPart;
        int arrayIndex = 0;
        for (int i = 0, n = nullPages.length; i < n; ++i) {
          String nullCount =
              nullCounts == null ? TOSTRING_MISSING_VALUE_MARKER : Long.toString(nullCounts[i]);
          String min, max;
          if (nullPages[i]) {
            min = max = TOSTRING_MISSING_VALUE_MARKER;
          } else {
            min = truncate(getMinValueAsString(arrayIndex));
            max = truncate(getMaxValueAsString(arrayIndex++));
          }
          formatter.format(format, i, nullCount, min, max);
        }
        return formatter.toString();
      }
    }

    int getPageCount() {
      return nullPages.length;
    }

    boolean isNullPage(int pageIndex) {
      return nullPages[pageIndex];
    }

    /*
     * Returns the min value for arrayIndex as a ByteBuffer. (Min values are not stored for null-pages so arrayIndex
     * might not equal to pageIndex.)
     */
    abstract ByteBuffer getMinValueAsBytes(int arrayIndex);

    /*
     * Returns the max value for arrayIndex as a ByteBuffer. (Max values are not stored for null-pages so arrayIndex
     * might not equal to pageIndex.)
     */
    abstract ByteBuffer getMaxValueAsBytes(int arrayIndex);

    /*
     * Returns the min value for arrayIndex as a String. (Min values are not stored for null-pages so arrayIndex might
     * not equal to pageIndex.)
     */
    abstract String getMinValueAsString(int arrayIndex);

    /*
     * Returns the max value for arrayIndex as a String. (Max values are not stored for null-pages so arrayIndex might
     * not equal to pageIndex.)
     */
    abstract String getMaxValueAsString(int arrayIndex);

    /* Creates a Statistics object for filtering. Used for user defined predicates. */
    abstract <T extends Comparable<T>> org.apache.parquet.filter2.predicate.Statistics<T> createStats(
        int arrayIndex);

    /* Creates a ValueComparator object containing the specified value to be compared for min/max values */
    abstract ValueComparator createValueComparator(Object value);

    @Override
    public PrimitiveIterator.OfInt visit(And and) {
      throw new UnsupportedOperationException("AND shall not be used on column index directly");
    }

    @Override
    public PrimitiveIterator.OfInt visit(Not not) {
      throw new UnsupportedOperationException("NOT shall not be used on column index directly");
    }

    @Override
    public PrimitiveIterator.OfInt visit(Or or) {
      throw new UnsupportedOperationException("OR shall not be used on column index directly");
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(Eq<T> eq) {
      T value = eq.getValue();
      if (value == null) {
        if (nullCounts == null) {
          // Searching for nulls so if we don't have null related statistics we have to return all pages
          return IndexIterator.all(getPageCount());
        } else {
          return IndexIterator.filter(getPageCount(), pageIndex -> nullCounts[pageIndex] > 0);
        }
      }
      return getBoundaryOrder().eq(createValueComparator(value));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(Gt<T> gt) {
      return getBoundaryOrder().gt(createValueComparator(gt.getValue()));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(GtEq<T> gtEq) {
      return getBoundaryOrder().gtEq(createValueComparator(gtEq.getValue()));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(Lt<T> lt) {
      return getBoundaryOrder().lt(createValueComparator(lt.getValue()));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(LtEq<T> ltEq) {
      return getBoundaryOrder().ltEq(createValueComparator(ltEq.getValue()));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(NotEq<T> notEq) {
      T value = notEq.getValue();
      if (value == null) {
        return IndexIterator.filter(getPageCount(), pageIndex -> !nullPages[pageIndex]);
      }

      if (nullCounts == null) {
        // Nulls match so if we don't have null related statistics we have to return all pages
        return IndexIterator.all(getPageCount());
      }

      // Merging value filtering with pages containing nulls
      IntSet matchingIndexes = new IntOpenHashSet();
      getBoundaryOrder()
          .notEq(createValueComparator(value))
          .forEachRemaining((int index) -> matchingIndexes.add(index));
      return IndexIterator.filter(
          getPageCount(), pageIndex -> nullCounts[pageIndex] > 0 || matchingIndexes.contains(pageIndex));
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(In<T> in) {
      Set<T> values = in.getValues();
      IntSet matchingIndexesForNull = new IntOpenHashSet(); // for null
      for (T value : values) {
        if (value == null) {
          if (nullCounts == null) {
            // Searching for nulls so if we don't have null related statistics we have to return all pages
            return IndexIterator.all(getPageCount());
          } else {
            for (int i = 0; i < nullCounts.length; i++) {
              if (nullCounts[i] > 0) {
                matchingIndexesForNull.add(i);
              }
            }
            if (values.size() == 1) {
              return IndexIterator.filter(getPageCount(), matchingIndexesForNull::contains);
            }
          }
        }
      }

      IntSet matchingIndexesLessThanMax = new IntOpenHashSet();
      IntSet matchingIndexesGreaterThanMin = new IntOpenHashSet();

      MinMax<T> minMax = new MinMax(comparator, values);
      T min = minMax.getMin();
      T max = minMax.getMax();

      // We don't want to iterate through each of the values in the IN set to compare,
      // because the size of the IN set might be very large. Instead, we want to only
      // compare the max and min value of the IN set to see if the page might contain the
      // values in the IN set.
      // If there might be values in a page that are <= the max value in the IN set,
      // and >= the min value in the IN set, then the page might contain
      // the values in the IN set.
      getBoundaryOrder()
          .ltEq(createValueComparator(max))
          .forEachRemaining((int index) -> matchingIndexesLessThanMax.add(index));
      getBoundaryOrder()
          .gtEq(createValueComparator(min))
          .forEachRemaining((int index) -> matchingIndexesGreaterThanMin.add(index));
      matchingIndexesLessThanMax.retainAll(matchingIndexesGreaterThanMin);
      IntSet matchingIndex = matchingIndexesLessThanMax;
      matchingIndex.addAll(matchingIndexesForNull); // add the matching null pages
      return IndexIterator.filter(getPageCount(), matchingIndex::contains);
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(NotIn<T> notIn) {
      return IndexIterator.all(getPageCount());
    }

    @Override
    public <T extends Comparable<T>> PrimitiveIterator.OfInt visit(Contains<T> contains) {
      return contains.filter(this, IndexIterator::intersection, IndexIterator::union);
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> PrimitiveIterator.OfInt visit(
        UserDefined<T, U> udp) {
      final UserDefinedPredicate<T> predicate = udp.getUserDefinedPredicate();
      final boolean acceptNulls = predicate.acceptsNullValue();

      if (acceptNulls && nullCounts == null) {
        // Nulls match so if we don't have null related statistics we have to return all pages
        return IndexIterator.all(getPageCount());
      }

      return IndexIterator.filter(getPageCount(), new IntPredicate() {
        private int arrayIndex = -1;

        @Override
        public boolean test(int pageIndex) {
          if (isNullPage(pageIndex)) {
            return acceptNulls;
          } else {
            ++arrayIndex;
            if (acceptNulls && nullCounts[pageIndex] > 0) {
              return true;
            }
            org.apache.parquet.filter2.predicate.Statistics<T> stats = createStats(arrayIndex);
            return !predicate.canDrop(stats);
          }
        }
      });
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> PrimitiveIterator.OfInt visit(
        LogicalNotUserDefined<T, U> udp) {
      final UserDefinedPredicate<T> inversePredicate =
          udp.getUserDefined().getUserDefinedPredicate();
      final boolean acceptNulls = !inversePredicate.acceptsNullValue();

      if (acceptNulls && nullCounts == null) {
        // Nulls match so if we don't have null related statistics we have to return all pages
        return IndexIterator.all(getPageCount());
      }

      return IndexIterator.filter(getPageCount(), new IntPredicate() {
        private int arrayIndex = -1;

        @Override
        public boolean test(int pageIndex) {
          if (isNullPage(pageIndex)) {
            return acceptNulls;
          } else {
            ++arrayIndex;
            if (acceptNulls && nullCounts[pageIndex] > 0) {
              return true;
            }
            org.apache.parquet.filter2.predicate.Statistics<T> stats = createStats(arrayIndex);
            return !inversePredicate.inverseCanDrop(stats);
          }
        }
      });
    }
  }

  private static final ColumnIndexBuilder NO_OP_BUILDER = new ColumnIndexBuilder() {
    @Override
    public ColumnIndex build() {
      return null;
    }

    @Override
    public void add(Statistics<?> stats) {}

    @Override
    public void add(Statistics<?> stats, SizeStatistics sizeStats) {}

    @Override
    void addMinMax(Object min, Object max) {}

    @Override
    ColumnIndexBase<?> createColumnIndex(PrimitiveType type) {
      return null;
    }

    @Override
    void clearMinMax() {}

    @Override
    void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max) {}

    @Override
    int compareMinValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
      return 0;
    }

    @Override
    int compareMaxValues(PrimitiveComparator<Binary> comparator, int index1, int index2) {
      return 0;
    }

    @Override
    int sizeOf(Object value) {
      return 0;
    }

    @Override
    public long getMinMaxSize() {
      return 0;
    }
  };

  private PrimitiveType type;
  private final BooleanList nullPages = new BooleanArrayList();
  private final LongList nullCounts = new LongArrayList();
  private final IntList pageIndexes = new IntArrayList();
  private int nextPageIndex;
  private LongList repLevelHistogram = new LongArrayList();
  private LongList defLevelHistogram = new LongArrayList();
  private List<GeometryStatistics> geometryStatistics = new ArrayList<>();

  /**
   * @return a no-op builder that does not collect statistics objects and therefore returns {@code null} at
   * {@link #build()}.
   */
  public static ColumnIndexBuilder getNoOpBuilder() {
    return NO_OP_BUILDER;
  }

  /**
   * @param type           the type this builder is to be created for
   * @param truncateLength the length to be used for truncating binary values if possible
   * @return a {@link ColumnIndexBuilder} instance to be used for creating {@link ColumnIndex} objects
   */
  public static ColumnIndexBuilder getBuilder(PrimitiveType type, int truncateLength) {
    ColumnIndexBuilder builder = createNewBuilder(type, truncateLength);
    builder.type = type;
    return builder;
  }

  private static ColumnIndexBuilder createNewBuilder(PrimitiveType type, int truncateLength) {
    switch (type.getPrimitiveTypeName()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
      case INT96:
        return new BinaryColumnIndexBuilder(type, truncateLength);
      case BOOLEAN:
        return new BooleanColumnIndexBuilder();
      case DOUBLE:
        return new DoubleColumnIndexBuilder();
      case FLOAT:
        return new FloatColumnIndexBuilder();
      case INT32:
        return new IntColumnIndexBuilder();
      case INT64:
        return new LongColumnIndexBuilder();
      default:
        throw new IllegalArgumentException("Unsupported type for column index: " + type);
    }
  }

  /**
   * @param type          the primitive type
   * @param boundaryOrder the boundary order of the min/max values
   * @param nullPages     the null pages (one boolean value for each page that signifies whether the page consists of nulls
   *                      entirely)
   * @param nullCounts    the number of null values for each page
   * @param minValues     the min values for each page
   * @param maxValues     the max values for each page
   * @return the newly created {@link ColumnIndex} object based on the specified arguments
   */
  public static ColumnIndex build(
      PrimitiveType type,
      BoundaryOrder boundaryOrder,
      List<Boolean> nullPages,
      List<Long> nullCounts,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues) {
    return build(type, boundaryOrder, nullPages, nullCounts, minValues, maxValues, null, null);
  }

  /**
   * @param type
   *          the primitive type
   * @param boundaryOrder
   *          the boundary order of the min/max values
   * @param nullPages
   *          the null pages (one boolean value for each page that signifies whether the page consists of nulls
   *          entirely)
   * @param nullCounts
   *          the number of null values for each page
   * @param minValues
   *          the min values for each page
   * @param maxValues
   *          the max values for each page
   * @param repLevelHistogram
   *          the repetition level histogram for all levels of each page
   * @param defLevelHistogram
   *          the definition level histogram for all levels of each page
   * @return the newly created {@link ColumnIndex} object based on the specified arguments
   */
  public static ColumnIndex build(
      PrimitiveType type,
      BoundaryOrder boundaryOrder,
      List<Boolean> nullPages,
      List<Long> nullCounts,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      List<Long> repLevelHistogram,
      List<Long> defLevelHistogram) {
    return build(type, boundaryOrder, nullPages, nullCounts, minValues, maxValues, null, null, null);
  }

  /**
   * @param type
   *          the primitive type
   * @param boundaryOrder
   *          the boundary order of the min/max values
   * @param nullPages
   *          the null pages (one boolean value for each page that signifies whether the page consists of nulls
   *          entirely)
   * @param nullCounts
   *          the number of null values for each page
   * @param minValues
   *          the min values for each page
   * @param maxValues
   *          the max values for each page
   * @param repLevelHistogram
   *          the repetition level histogram for all levels of each page
   * @param defLevelHistogram
   *          the definition level histogram for all levels of each page
   * @param geometryStatistics
   *          the geometry statistics for each page (apply to GEOMETRY logical type only)
   * @return the newly created {@link ColumnIndex} object based on the specified arguments
   */
  public static ColumnIndex build(
      PrimitiveType type,
      BoundaryOrder boundaryOrder,
      List<Boolean> nullPages,
      List<Long> nullCounts,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      List<Long> repLevelHistogram,
      List<Long> defLevelHistogram,
      List<GeometryStatistics> geometryStatistics) {

    ColumnIndexBuilder builder = createNewBuilder(type, Integer.MAX_VALUE);

    builder.fill(
        nullPages, nullCounts, minValues, maxValues, repLevelHistogram, defLevelHistogram, geometryStatistics);
    ColumnIndexBase<?> columnIndex = builder.build(type);
    columnIndex.boundaryOrder = requireNonNull(boundaryOrder);
    return columnIndex;
  }

  ColumnIndexBuilder() {
    // Shall be able to be created inside this package only
  }

  /**
   * Adds the data from the specified statistics to this builder
   *
   * @param stats the statistics to be added
   */
  public void add(Statistics<?> stats) {
    add(stats, null);
  }

  /**
   * Adds the data from the specified statistics to this builder
   *
   * @param stats
   *          the statistics to be added
   * @param sizeStats
   *          the size statistics to be added
   */
  public void add(Statistics<?> stats, SizeStatistics sizeStats) {
    if (stats.hasNonNullValue()) {
      nullPages.add(false);
      Object min = stats.genericGetMin();
      Object max = stats.genericGetMax();
      addMinMax(min, max);
      pageIndexes.add(nextPageIndex);
    } else {
      nullPages.add(true);
    }
    nullCounts.add(stats.getNumNulls());

    // Collect repetition and definition level histograms only when all pages are valid.
    if (sizeStats != null && sizeStats.isValid() && repLevelHistogram != null && defLevelHistogram != null) {
      repLevelHistogram.addAll(sizeStats.getRepetitionLevelHistogram());
      defLevelHistogram.addAll(sizeStats.getDefinitionLevelHistogram());
    } else {
      repLevelHistogram = null;
      defLevelHistogram = null;
    }

    if (type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) {
      assert stats instanceof BinaryStatistics;
      BinaryStatistics binaryStats = (BinaryStatistics) stats;
      if (geometryStatistics != null && binaryStats.getGeometryStatistics() != null) {
        geometryStatistics.add(binaryStats.getGeometryStatistics());
      } else {
        geometryStatistics = null;
      }
    }

    ++nextPageIndex;
  }

  abstract void addMinMaxFromBytes(ByteBuffer min, ByteBuffer max);

  abstract void addMinMax(Object min, Object max);

  private void fill(
      List<Boolean> nullPages,
      List<Long> nullCounts,
      List<ByteBuffer> minValues,
      List<ByteBuffer> maxValues,
      List<Long> repLevelHistogram,
      List<Long> defLevelHistogram,
      List<GeometryStatistics> geometryStatistics) {
    clear();
    int pageCount = nullPages.size();
    if ((nullCounts != null && nullCounts.size() != pageCount)
        || minValues.size() != pageCount
        || maxValues.size() != pageCount) {
      throw new IllegalArgumentException(String.format(
          "Not all sizes are equal (nullPages:%d, nullCounts:%s, minValues:%d, maxValues:%d",
          nullPages.size(),
          nullCounts == null ? "null" : nullCounts.size(),
          minValues.size(),
          maxValues.size()));
    }
    if (repLevelHistogram != null && repLevelHistogram.size() % pageCount != 0) {
      /// FIXME: it is unfortunate that we don't know the max repetition level here.
      throw new IllegalArgumentException(String.format(
          "Size of repLevelHistogram:%d is not a multiply of pageCount:%d, ",
          repLevelHistogram.size(), pageCount));
    }
    if (defLevelHistogram != null && defLevelHistogram.size() % pageCount != 0) {
      /// FIXME: it is unfortunate that we don't know the max definition level here.
      throw new IllegalArgumentException(String.format(
          "Size of defLevelHistogram:%d is not a multiply of pageCount:%d, ",
          defLevelHistogram.size(), pageCount));
    }
    this.nullPages.addAll(nullPages);
    // Nullcounts is optional in the format
    if (nullCounts != null) {
      this.nullCounts.addAll(nullCounts);
    }

    for (int i = 0; i < pageCount; ++i) {
      if (!nullPages.get(i)) {
        ByteBuffer min = minValues.get(i);
        ByteBuffer max = maxValues.get(i);
        addMinMaxFromBytes(min, max);
        pageIndexes.add(i);
      }
    }

    // Repetition and definition level histograms are optional in the format
    if (repLevelHistogram != null) {
      this.repLevelHistogram.addAll(repLevelHistogram);
    }
    if (defLevelHistogram != null) {
      this.defLevelHistogram.addAll(defLevelHistogram);
    }
    if (geometryStatistics != null) {
      this.geometryStatistics.addAll(geometryStatistics);
    }
  }

  /**
   * @return the newly created column index or {@code null} if the {@link ColumnIndex} would be empty
   */
  public ColumnIndex build() {
    ColumnIndexBase<?> columnIndex = build(type);
    if (columnIndex == null) {
      return null;
    }
    columnIndex.boundaryOrder = calculateBoundaryOrder(type.comparator());
    return columnIndex;
  }

  private ColumnIndexBase<?> build(PrimitiveType type) {
    if (nullPages.isEmpty()) {
      return null;
    }
    ColumnIndexBase<?> columnIndex = createColumnIndex(type);
    if (columnIndex == null) {
      // Might happen if the specialized builder discovers invalid min/max values
      return null;
    }
    columnIndex.nullPages = nullPages.toBooleanArray();
    // Null counts is optional so keep it null if the builder has no values
    if (!nullCounts.isEmpty()) {
      columnIndex.nullCounts = nullCounts.toLongArray();
    }
    columnIndex.pageIndexes = pageIndexes.toIntArray();
    // Repetition and definition level histograms are optional so keep them null if the builder has no values
    if (repLevelHistogram != null && !repLevelHistogram.isEmpty()) {
      columnIndex.repLevelHistogram = repLevelHistogram.toLongArray();
    }
    if (defLevelHistogram != null && !defLevelHistogram.isEmpty()) {
      columnIndex.defLevelHistogram = defLevelHistogram.toLongArray();
    }
    if (geometryStatistics != null && !geometryStatistics.isEmpty()) {
      columnIndex.geometryStatistics = new GeometryStatistics[geometryStatistics.size()];
      geometryStatistics.toArray(columnIndex.geometryStatistics);
    }

    return columnIndex;
  }

  private BoundaryOrder calculateBoundaryOrder(PrimitiveComparator<Binary> comparator) {
    if (isAscending(comparator)) {
      return BoundaryOrder.ASCENDING;
    } else if (isDescending(comparator)) {
      return BoundaryOrder.DESCENDING;
    } else {
      return BoundaryOrder.UNORDERED;
    }
  }

  // min[i] <= min[i+1] && max[i] <= max[i+1]
  private boolean isAscending(PrimitiveComparator<Binary> comparator) {
    for (int i = 1, n = pageIndexes.size(); i < n; ++i) {
      if (compareMinValues(comparator, i - 1, i) > 0 || compareMaxValues(comparator, i - 1, i) > 0) {
        return false;
      }
    }
    return true;
  }

  // min[i] >= min[i+1] && max[i] >= max[i+1]
  private boolean isDescending(PrimitiveComparator<Binary> comparator) {
    for (int i = 1, n = pageIndexes.size(); i < n; ++i) {
      if (compareMinValues(comparator, i - 1, i) < 0 || compareMaxValues(comparator, i - 1, i) < 0) {
        return false;
      }
    }
    return true;
  }

  abstract int compareMinValues(PrimitiveComparator<Binary> comparator, int index1, int index2);

  abstract int compareMaxValues(PrimitiveComparator<Binary> comparator, int index1, int index2);

  private void clear() {
    nullPages.clear();
    nullCounts.clear();
    clearMinMax();
    nextPageIndex = 0;
    pageIndexes.clear();
    repLevelHistogram.clear();
    defLevelHistogram.clear();
    geometryStatistics.clear();
  }

  abstract void clearMinMax();

  abstract ColumnIndexBase<?> createColumnIndex(PrimitiveType type);

  abstract int sizeOf(Object value);

  /**
   * @return the number of pages added so far to this builder
   */
  public int getPageCount() {
    return nullPages.size();
  }

  /**
   * @return the sum of size in bytes of the min/max values added so far to this builder
   */
  public long getMinMaxSize() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
