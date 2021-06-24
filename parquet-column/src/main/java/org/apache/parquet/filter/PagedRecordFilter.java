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
package org.apache.parquet.filter;

import org.apache.parquet.column.ColumnReader;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Filter which will only materialize a page worth of results.
 */
public final class PagedRecordFilter implements UnboundRecordFilter {

  private final long startPos;
  private final long endPos;
  private AtomicLong currentPos = new AtomicLong(0);

  /**
   * Returns builder for creating a paged query.
   * @param startPos The record to start from, numbering starts at 1.
   * @param pageSize The size of the page.
   * @return a paged record filter
   */
  public static final UnboundRecordFilter page( final long startPos, final long pageSize ) {
    return new PagedRecordFilter( startPos, pageSize );
  }

  /**
   * Private constructor, use column() instead.
   */
  private PagedRecordFilter(long startPos, long pageSize) {
    this.startPos = startPos;
    this.endPos   = startPos + pageSize;
  }

  @Override
  public RecordFilter bind(Iterable<ColumnReader> readers) {
    // Use inner class to access the currentPos for each group.
    return new BoundedPagedRecordFilter();
  }

  private final class BoundedPagedRecordFilter implements RecordFilter {

    /**
     * Keeps track of how many times it is called. Only returns matches when the
     * record number is in the range.
     */
    @Override
    public boolean isMatch() {
      long pos = currentPos.incrementAndGet();
      return (( pos >= startPos ) && ( pos < endPos ));
    }

  }


}
