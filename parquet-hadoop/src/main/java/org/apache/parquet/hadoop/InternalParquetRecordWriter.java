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
package org.apache.parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory.BytesCompressor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InternalParquetRecordWriter<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InternalParquetRecordWriter.class);

  private final ParquetFileWriter parquetFileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final boolean estimateNextBlockSizeCheck;
  private long rowGroupSizeThreshold;
  private long nextRowGroupSize;
  private final BytesCompressor compressor;
  private final boolean validating;
  private final ParquetProperties props;

  private boolean closed;

  private long recordCount = 0;
  private long recordCountForNextMemCheck;
  private long lastRowGroupEndPos = 0;

  private ColumnWriteStore columnStore;
  private ColumnChunkPageWriteStore pageStore;
  private RecordConsumer recordConsumer;
  private int minRecordCountForBlockSizeCheck;
  private int maxRecordCountForBlockSizeCheck;

  /**
   * @param parquetFileWriter the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param rowGroupSize the size of a block in the file (this will be approximate)
   * @param compressor the codec used to compress
   */
  public InternalParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long rowGroupSize,
      BytesCompressor compressor,
      boolean validating,
      ParquetProperties props) {
    this.parquetFileWriter = parquetFileWriter;
    this.writeSupport = checkNotNull(writeSupport, "writeSupport");
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.rowGroupSizeThreshold = rowGroupSize;
    this.nextRowGroupSize = rowGroupSizeThreshold;
    this.compressor = compressor;
    this.validating = validating;
    this.props = props;
    this.minRecordCountForBlockSizeCheck = props.getMinRowCountForPageSizeCheck();
    this.maxRecordCountForBlockSizeCheck = props.getMaxRowCountForPageSizeCheck();
    this.estimateNextBlockSizeCheck = props.estimateNextBlockSizeCheck();
    initStore();
  }

  private void initStore() {
    pageStore = new ColumnChunkPageWriteStore(compressor, schema, props.getAllocator());
    columnStore = props.newColumnWriteStore(schema, pageStore);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    this.recordConsumer = columnIO.getRecordWriter(columnStore);
    writeSupport.prepareForWrite(recordConsumer);
    System.out.println(String.format("Created ParquetWriter with [%d, %d] for block size checks. Estimation(%s). BlockSize(%d)",
      minRecordCountForBlockSizeCheck, maxRecordCountForBlockSizeCheck, estimateNextBlockSizeCheck, rowGroupSizeThreshold));
  }

  public void close() throws IOException, InterruptedException {
    if (!closed) {
      flushRowGroupToStore();
      FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();
      Map<String, String> finalMetadata = new HashMap<String, String>(extraMetaData);
      String modelName = writeSupport.getName();
      if (modelName != null) {
        finalMetadata.put(ParquetWriter.OBJECT_MODEL_NAME_PROP, modelName);
      }
      finalMetadata.putAll(finalWriteContext.getExtraMetaData());
      parquetFileWriter.end(finalMetadata);
      closed = true;
    }
  }

  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++ recordCount;
    checkBlockSizeReached();
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return lastRowGroupEndPos + columnStore.getBufferedSize();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.info("mem size {} > {}: flushing {} records to disk.", memSize, nextRowGroupSize, recordCount);
        flushRowGroupToStore();
        initStore();
        if (estimateNextBlockSizeCheck) {
          recordCountForNextMemCheck = min(max(minRecordCountForBlockSizeCheck, recordCount / 2),
            maxRecordCountForBlockSizeCheck);
        } else {
          recordCountForNextMemCheck = minRecordCountForBlockSizeCheck;
        }
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
        if (estimateNextBlockSizeCheck) {
          recordCountForNextMemCheck = min(
            max(minRecordCountForBlockSizeCheck, (recordCount + (long) (nextRowGroupSize / ((float) recordSize))) / 2),
            // will check halfway
            recordCount + maxRecordCountForBlockSizeCheck // will not look more than max records ahead
          );
        } else {
          recordCountForNextMemCheck += minRecordCountForBlockSizeCheck;
        }
        LOG.debug("Checked mem at {} will check again at: {}", recordCount, recordCountForNextMemCheck);
      }
    }
  }

  private void flushRowGroupToStore()
      throws IOException {
    recordConsumer.flush();
    LOG.info("Flushing mem columnStore to file. allocated memory: {}", columnStore.getAllocatedSize());
    if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
      LOG.warn("Too much memory used: {}", columnStore.memUsageString());
    }

    if (recordCount > 0) {
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
      this.nextRowGroupSize = Math.min(
          parquetFileWriter.getNextRowGroupSize(),
          rowGroupSizeThreshold);
    }

    columnStore = null;
    pageStore = null;
  }

  long getRowGroupSizeThreshold() {
    return rowGroupSizeThreshold;
  }

  void setRowGroupSizeThreshold(long rowGroupSizeThreshold) {
    this.rowGroupSizeThreshold = rowGroupSizeThreshold;
  }

  MessageType getSchema() {
    return this.schema;
  }
}
