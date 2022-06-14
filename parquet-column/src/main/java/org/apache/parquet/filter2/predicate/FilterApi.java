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
package org.apache.parquet.filter2.predicate;

import java.io.Serializable;
import java.util.Set;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.Eq;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.Gt;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.In;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.LtEq;
import org.apache.parquet.filter2.predicate.Operators.Not;
import org.apache.parquet.filter2.predicate.Operators.NotEq;
import org.apache.parquet.filter2.predicate.Operators.NotIn;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.filter2.predicate.Operators.SupportsEqNotEq;
import org.apache.parquet.filter2.predicate.Operators.SupportsLtGt;
import org.apache.parquet.filter2.predicate.Operators.UserDefined;
import org.apache.parquet.filter2.predicate.Operators.UserDefinedByClass;
import org.apache.parquet.filter2.predicate.Operators.UserDefinedByInstance;
import org.apache.parquet.schema.Type;

/**
 * The Filter API is expressed through these static methods.
 * <p>
 * Example usage:
 * <pre>
 *   IntColumn foo = intColumn("foo");
 *   DoubleColumn bar = doubleColumn("x.y.bar");
 *
 *   // foo == 10 || bar &lt;= 17.0
 *   FilterPredicate pred = or(eq(foo, 10), ltEq(bar, 17.0));
 * </pre>
 */
// TODO: Support repeated columns (https://issues.apache.org/jira/browse/PARQUET-34)
//
// TODO: Support filtering on groups (eg, filter where this group is / isn't null)
// TODO: (https://issues.apache.org/jira/browse/PARQUET-43)

// TODO: Consider adding support for more column types that aren't coupled with parquet types, eg Column<String>
// TODO: (https://issues.apache.org/jira/browse/PARQUET-35)
public final class FilterApi {
  private FilterApi() { }

  public static IntColumn intColumn(String columnPath) {
    return new IntColumn(ColumnPath.fromDotString(columnPath));
  }

  public static IntColumn intColumn(Type.ID id) {
    return new IntColumn(id);
  }

  public static LongColumn longColumn(String columnPath) {
    return new LongColumn(ColumnPath.fromDotString(columnPath));
  }

  public static LongColumn longColumn(Type.ID id) {
    return new LongColumn(id);
  }

  public static FloatColumn floatColumn(String columnPath) {
    return new FloatColumn(ColumnPath.fromDotString(columnPath));
  }

  public static FloatColumn floatColumn(Type.ID id) {
    return new FloatColumn(id);
  }

  public static DoubleColumn doubleColumn(String columnPath) {
    return new DoubleColumn(ColumnPath.fromDotString(columnPath));
  }

  public static DoubleColumn doubleColumn(Type.ID id) {
    return new DoubleColumn(id);
  }

  public static BooleanColumn booleanColumn(String columnPath) {
    return new BooleanColumn(ColumnPath.fromDotString(columnPath));
  }

  public static BooleanColumn booleanColumn(Type.ID id) {
    return new BooleanColumn(id);
  }

  public static BinaryColumn binaryColumn(String columnPath) {
    return new BinaryColumn(ColumnPath.fromDotString(columnPath));
  }

  public static BinaryColumn binaryColumn(Type.ID id) {
    return new BinaryColumn(id);
  }

  /**
   * Keeps records if their value is equal to the provided value.
   * Nulls are treated the same way the java programming language does.
   * <p>
   * For example:
   *   eq(column, null) will keep all records whose value is null.
   *   eq(column, 7) will keep all records whose value is 7, and will drop records whose value is null
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return an equals predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> Eq<T> eq(C column, T value) {
    return new Eq<>(column, value);
  }

  /**
   * Keeps records if their value is not equal to the provided value.
   * Nulls are treated the same way the java programming language does.
   * <p>
   * For example:
   *   notEq(column, null) will keep all records whose value is not null.
   *   notEq(column, 7) will keep all records whose value is not 7, including records whose value is null.
   *
   *   NOTE: this is different from how some query languages handle null. For example, SQL and pig will drop
   *   nulls when you filter by not equal to 7. To achieve similar behavior in this api, do:
   *   and(notEq(column, 7), notEq(column, null))
   *
   *   NOTE: be sure to read the {@link #lt}, {@link #ltEq}, {@link #gt}, {@link #gtEq} operator's docs
   *         for how they handle nulls
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return a not-equals predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> NotEq<T> notEq(C column, T value) {
    return new NotEq<>(column, value);
  }

  /**
   * Keeps records if their value is less than (but not equal to) the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be dropped.
   * <p>
   * For example:
   *   lt(column, 7) will keep all records whose value is less than (but not equal to) 7, and not null.
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return a less-than predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> Lt<T> lt(C column, T value) {
    return new Lt<>(column, value);
  }

  /**
   * Keeps records if their value is less than or equal to the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be dropped.
   * <p>
   * For example:
   *   ltEq(column, 7) will keep all records whose value is less than or equal to 7, and not null.
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return a less-than-or-equal predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> LtEq<T> ltEq(C column, T value) {
    return new LtEq<>(column, value);
  }

  /**
   * Keeps records if their value is greater than (but not equal to) the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be dropped.
   * <p>
   * For example:
   *   gt(column, 7) will keep all records whose value is greater than (but not equal to) 7, and not null.
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return a greater-than predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> Gt<T> gt(C column, T value) {
    return new Gt<>(column, value);
  }

  /**
   * Keeps records if their value is greater than or equal to the provided value.
   * The provided value cannot be null, as less than null has no meaning.
   * Records with null values will be dropped.
   * <p>
   * For example:
   *   gtEq(column, 7) will keep all records whose value is greater than or equal to 7, and not null.
   *
   * @param column a column reference created by FilterApi
   * @param value a value that matches the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return a greater-than-or-equal predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsLtGt> GtEq<T> gtEq(C column, T value) {
    return new GtEq<>(column, value);
  }

  /**
   * Keeps records if their value is in the provided values.
   * The provided values set could not be null, but could contains a null value.
   * <p>
   * For example:
   * <pre>
   *   {@code
   *   Set<Integer> set = new HashSet<>();
   *   set.add(9);
   *   set.add(null);
   *   set.add(50);
   *   in(column, set);}
   * </pre>
   * will keep all records whose values are 9, null, or 50.
   *
   * @param column a column reference created by FilterApi
   * @param values a set of values that match the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return an in predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> In<T> in(C column, Set<T> values) {
    return new In<>(column, values);
  }

  /**
   * Keeps records if their value is not in the provided values.
   * The provided values set could not be null, but could contains a null value.
   * <p>
   * For example:
   * <pre>
   *   {@code
   *   Set<Integer> set = new HashSet<>();
   *   set.add(9);
   *   set.add(null);
   *   set.add(50);
   *   notIn(column, set);}
   * </pre>
   * will keep all records whose values are not 9, null, and 50.
   *
   * @param column a column reference created by FilterApi
   * @param values a set of values that match the column's type
   * @param <T> the Java type of values in the column
   * @param <C> the column type that corresponds to values of type T
   * @return an notIn predicate for the given column and value
   */
  public static <T extends Comparable<T>, C extends Column<T> & SupportsEqNotEq> NotIn<T> notIn(C column, Set<T> values) {
    return new NotIn<>(column, values);
  }

  /**
   * Keeps records that pass the provided {@link UserDefinedPredicate}
   * <p>
   * The provided class must have a default constructor. To use an instance
   * of a UserDefinedPredicate instead, see userDefined below.
   *
   * @param column a column reference created by FilterApi
   * @param clazz a user-defined predicate class
   * @param <T> the Java type of values in the column
   * @param <U> a user-defined predicate for values of type T
   * @return a user-defined predicate for the given column
   */
  public static <T extends Comparable<T>, U extends UserDefinedPredicate<T>>
    UserDefined<T, U> userDefined(Column<T> column, Class<U> clazz) {
    return new UserDefinedByClass<>(column, clazz);
  }
  
  /**
   * Keeps records that pass the provided {@link UserDefinedPredicate}
   * <p>
   * The provided instance of UserDefinedPredicate must be serializable.
   *
   * @param column a column reference created by FilterApi
   * @param udp a user-defined predicate instance
   * @param <T> the Java type of values in the column
   * @param <U> a user-defined predicate for values of type T
   * @return a user-defined predicate for the given column
   */
  public static <T extends Comparable<T>, U extends UserDefinedPredicate<T> & Serializable>
    UserDefined<T, U> userDefined(Column<T> column, U udp) {
    return new UserDefinedByInstance<>(column, udp);
  }

  /**
   * Constructs the logical and of two predicates. Records will be kept if both the left and right predicate agree
   * that the record should be kept.
   *
   * @param left a predicate
   * @param right a predicate
   * @return an and predicate from the result of the left and right predicates
   */
  public static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
    return new And(left, right);
  }

  /**
   * Constructs the logical or of two predicates. Records will be kept if either the left or right predicate
   * is satisfied (or both).
   *
   * @param left a predicate
   * @param right a predicate
   * @return an or predicate from the result of the left and right predicates
   */
  public static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
    return new Or(left, right);
  }

  /**
   * Constructs the logical not (or inverse) of a predicate.
   * Records will be kept if the provided predicate is not satisfied.
   *
   * @param predicate a predicate
   * @return a not predicate wrapping the result of the given predicate
   */
  public static FilterPredicate not(FilterPredicate predicate) {
    return new Not(predicate);
  }

}
