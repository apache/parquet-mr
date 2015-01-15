/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class IntStatistics extends Statistics<Integer> {

  private int max;
  private int min;

  @Override
  public void updateStats(int value) {
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(value, value);
    } else {
      updateStats(value, value);
    }
  }

  @Override
  public void mergeStatisticsMinMax(Statistics stats) {
    IntStatistics intStats = (IntStatistics)stats;
    if (this.isEmpty() || !this.hasValidValue()) {
      initializeStats(intStats.getMin(), intStats.getMax());
    } else {
      updateStats(intStats.getMin(), intStats.getMax());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToInt(maxBytes);
    min = BytesUtils.bytesToInt(minBytes);
    this.markHasValidValue();
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(min);
  }

  @Override
  public String toString() {
    if(this.hasValidValue())
      return String.format("min: %d, max: %d, num_nulls: %d", min, max, this.getNumNulls());
    else if(!this.isEmpty())
      return String.format("num_nulls: %d, min/max not defined", this.getNumNulls());
    else
      return "no stats for this column";
  }

  public void updateStats(int min_value, int max_value) {
    if (min_value < min) { min = min_value; }
    if (max_value > max) { max = max_value; }
  }

  public void initializeStats(int min_value, int max_value) {
      min = min_value;
      max = max_value;
      this.markHasValidValue();
      this.markAsNotEmpty();
  }

  @Override
  public Integer genericGetMin() {
    return min;
  }

  @Override
  public Integer genericGetMax() {
    return max;
  }
  
  @Override
  public void incrementNumNulls() {
    if (this.isEmpty()) {
      this.markAsNotEmpty();
    }
    
    super.incrementNumNulls();
  }

  @Override
  public void incrementNumNulls(long increment) {
    if (this.isEmpty()) {
      this.markAsNotEmpty();
    }

    super.incrementNumNulls(increment);
  }

  public int getMax() {
    return max;
  }

  public int getMin() {
    return min;
  }

  public void setMinMax(int min, int max) {
    this.max = max;
    this.min = min;
    this.markHasValidValue();
    this.markAsNotEmpty();
  }
}
