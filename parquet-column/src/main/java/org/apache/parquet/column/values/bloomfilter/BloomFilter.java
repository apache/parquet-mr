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
package org.apache.parquet.column.values.bloomfilter;

import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A Bloom filter is a compact structure to indicate whether an item is not in a set or probably
 * in a set. The Bloom filter usually consists of a bit set that represents a elements set,
 * a hash strategy and a Bloom filter algorithm.
 */
public interface BloomFilter {
  /* Bloom filter Hash strategy.
   *
   * xxHash is an extremely fast hash algorithm, running at RAM speed limits. It successfully
   * completes the SMHasher test suite which evaluates collision, dispersion and randomness qualities
   * of hash functions. It shows good performance advantage from its benchmark result.
   * (see https://github.com/Cyan4973/xxHash).
   */
  enum HashStrategy {
    XXH64;

    @Override
    public String toString() {
      return "xxhash";
    }
  }

  // Bloom filter algorithm.
  enum Algorithm {
    BLOCK;

    @Override
    public String toString() {
      return "block";
    }
  }

  // Bloom filter compression.
  enum Compression {
    UNCOMPRESSED;

    @Override
    public String toString() {
      return "uncompressed";
    }
  }

  /**
   * Write the Bloom filter to an output stream. It writes the Bloom filter header including the
   * bitset's length in bytes, the hash strategy, the algorithm, and the bitset.
   *
   * @param out the output stream to write
   */
  void writeTo(OutputStream out) throws IOException;

  /**
   * Insert an element to the Bloom filter, the element content is represented by
   * the hash value of its plain encoding result.
   *
   * @param hash the hash result of element.
   */
  void insertHash(long hash);

  /**
   * Determine whether an element is in set or not.
   *
   * @param hash the hash value of element plain encoding result.
   * @return false if element is must not in set, true if element probably in set.
   */
  boolean findHash(long hash);

  /**
   * Get the number of bytes for bitset in this Bloom filter.
   *
   * @return The number of bytes for bitset in this Bloom filter.
   */
  int getBitsetSize();

  /**
   * Compare this Bloom filter to the specified object.
   *
   * @param object
   * @return true if the given object represents a Bloom filter equivalent to this Bloom filter, false otherwise.
   */
  boolean equals(Object object);

  /**
   * Compute hash for int value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(int value);

  /**
   * Compute hash for long value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(long value) ;

  /**
   * Compute hash for double value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(double value);

  /**
   * Compute hash for float value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(float value);

  /**
   * Compute hash for Binary value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(Binary value);

  /**
   * Compute hash for Object value by using its plain encoding result.
   *
   * @param value the value to hash
   * @return hash result
   */
  long hash(Object value);

  // The boolean type is not supported because boolean type has only two values, while Bloom filter is
  // suitable for high cardinality.
  // long hash(Boolean value);

  /**
   * Return the hash strategy that the bloom filter apply.
   *
   * @return hash strategy that the bloom filter apply
   */
  HashStrategy getHashStrategy();

  /**
   * Return the algorithm that the bloom filter apply.
   *
   * @return algorithm that the bloom filter apply
   */
  Algorithm getAlgorithm();

  /**
   * Return the compress algorithm that the bloom filter apply.
   *
   * @return compress algorithm that the bloom filter apply
   */
  Compression getCompression();

  /**
   * Determines whether a given Bloom filter can be merged into this Bloom filter. For two Bloom
   * filters to merge, they must:
   * <ul>
   * <li> have the same bit size </li>
   * <li> have the same algorithm</li>
   * <li> have the same hash strategy</li>
   * </ul>
   *
   * @param otherBloomFilter The Bloom filter to merge this Bloom filter with.
   */
  default boolean canMergeFrom(BloomFilter otherBloomFilter) {
    throw new UnsupportedOperationException("Merge API is not implemented.");
  }

  /**
   * Merges this Bloom filter with another Bloom filter by performing a bitwise OR of the underlying bitsets
   *
   * @param otherBloomFilter The Bloom filter to merge this Bloom filter with.
   */
  default void merge(BloomFilter otherBloomFilter) throws IOException {
    throw new UnsupportedOperationException("Merge API is not implemented.");
  }
}
