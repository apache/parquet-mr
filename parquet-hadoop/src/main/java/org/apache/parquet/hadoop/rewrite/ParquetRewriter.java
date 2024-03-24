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
package org.apache.parquet.hadoop.rewrite;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
import static org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.IndexCache;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRewriter implements Closeable {

  // Key to store original writer version in the file key-value metadata
  public static final String ORIGINAL_CREATED_BY_KEY = "original.created.by";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetRewriter.class);
  private final int pageBufferSize = ParquetProperties.DEFAULT_PAGE_SIZE * 2;
  private final byte[] pageBuffer = new byte[pageBufferSize];
  // Configurations for the new file
  private final CompressionCodecName newCodecName;
  private Map<ColumnPath, MaskMode> maskColumns = null;
  private Set<ColumnPath> encryptColumns = null;
  private boolean encryptMode = false;
  private final Map<String, String> extraMetaData = new HashMap<>();
  // Writer to rewrite the input files
  private final ParquetFileWriter writer;
  // Number of blocks written which is used to keep track of the actual row group ordinal
  private int numBlocksRewritten = 0;
  // Reader and relevant states of the in-processing input file
  private final Queue<TransParquetFileReader> inputFiles = new LinkedList<>();
  private final List<RightColumnWriter> columnWritersR = new ArrayList<>();
  // Schema of input files (should be the same) and to write to the output file
  private MessageType schema = null;
  private final Map<ColumnPath, ColumnDescriptor> descriptorsMap;
  // The reader for the current input file
  private TransParquetFileReader reader = null;
  // The metadata of current reader being processed
  private ParquetMetadata meta = null;
  // created_by information of current reader being processed
  private String originalCreatedBy = "";
  // The index cache strategy
  private final IndexCache.CacheStrategy indexCacheStrategy;

  public ParquetRewriter(RewriteOptions options) throws IOException {
    ParquetConfiguration conf = options.getParquetConfiguration();
    OutputFile out = options.getParquetOutputFile();
    inputFiles.addAll(getFileReaders(options.getParquetInputFiles(), conf));
    List<Queue<TransParquetFileReader>> inputFilesR = options.getParquetInputFilesR().stream()
        .map(x -> getFileReaders(x, conf))
        .collect(Collectors.toList());
    ensureSameSchema(inputFiles);
    inputFilesR.forEach(this::ensureSameSchema);
    LOG.info("Start rewriting {} input file(s) {} to {}", inputFiles.size(), options.getParquetInputFiles(), out);

    extraMetaData.put(
        ORIGINAL_CREATED_BY_KEY,
        Stream.concat(inputFiles.stream(), inputFilesR.stream().flatMap(Collection::stream))
            .map(x -> x.getFooter().getFileMetaData().getCreatedBy())
            .collect(Collectors.toSet())
            .stream()
            .reduce((a, b) -> a + "\n" + b)
            .orElse(""));
    Stream.concat(inputFiles.stream(), inputFilesR.stream().flatMap(Collection::stream))
        .forEach(x -> extraMetaData.putAll(x.getFileMetaData().getKeyValueMetaData()));

    MessageType schemaL = inputFiles.peek().getFooter().getFileMetaData().getSchema();
    List<MessageType> schemaR = inputFilesR.stream()
        .map(x -> x.peek().getFooter().getFileMetaData().getSchema())
        .collect(Collectors.toList());
    Map<String, Type> fieldNamesL = new LinkedHashMap<>();
    schemaL.getFields().forEach(x -> fieldNamesL.put(x.getName(), x));
    Map<String, Type> fieldNamesR = new LinkedHashMap<>();
    schemaR.stream().flatMap(x -> x.getFields().stream()).forEach(x -> {
      if (fieldNamesR.containsKey(x.getName())) {
        throw new IllegalArgumentException(
            "Found a duplicated field `" + x.getName() + "` in the right side file groups!");
      }
      fieldNamesR.put(x.getName(), x);
    });
    List<Type> fields = Stream.concat(
            fieldNamesL.values().stream()
                .map(x -> fieldNamesR.getOrDefault(
                    x.getName(), x)), // take a field on the right if we can
            fieldNamesR.values().stream()
                .filter(x -> !fieldNamesL.containsKey(
                    x.getName())) // takes fields on the right if it was not present on the left
            )
        .collect(Collectors.toList());
    schema = new MessageType(schemaL.getName(), fields);

    newCodecName = options.getNewCodecName();
    List<String> pruneColumns = options.getPruneColumns();
    // Prune columns if specified
    if (pruneColumns != null && !pruneColumns.isEmpty()) {
      List<String> paths = new ArrayList<>();
      getPaths(schema, paths, null);
      for (String col : pruneColumns) {
        if (!paths.contains(col)) {
          LOG.warn("Input column name {} doesn't show up in the schema", col);
        }
      }

      Set<ColumnPath> prunePaths = convertToColumnPaths(pruneColumns);
      schema = pruneColumnsInSchema(schema, prunePaths);
    }

    if (inputFilesR.isEmpty()) {
      this.descriptorsMap =
          schema.getColumns().stream().collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    } else { // TODO: describe in documentation that only top level column can be overwritten
      this.descriptorsMap = schemaL.getColumns().stream()
          .filter(x -> x.getPath().length == 0 || !fieldNamesR.containsKey(x.getPath()[0]))
          .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    }

    long rowCountL =
        inputFiles.stream().mapToLong(ParquetFileReader::getRecordCount).sum();
    inputFilesR.stream()
        .map(x ->
            x.stream().mapToLong(ParquetFileReader::getRecordCount).sum())
        .forEach(rowCountR -> {
          if (rowCountL != rowCountR) {
            throw new IllegalArgumentException("The number of records on the left(" + rowCountL
                + ") and on the right(" + rowCountR + ") don't match!");
          }
        });

    if (options.getMaskColumns() != null) {
      this.maskColumns = new HashMap<>();
      for (Map.Entry<String, MaskMode> col : options.getMaskColumns().entrySet()) {
        maskColumns.put(ColumnPath.fromDotString(col.getKey()), col.getValue());
      }
    }

    if (options.getEncryptColumns() != null && options.getFileEncryptionProperties() != null) {
      this.encryptColumns = convertToColumnPaths(options.getEncryptColumns());
      this.encryptMode = true;
    }

    this.indexCacheStrategy = options.getIndexCacheStrategy();

    ParquetFileWriter.Mode writerMode = ParquetFileWriter.Mode.CREATE;
    writer = new ParquetFileWriter(
        out,
        schema,
        writerMode,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH,
        DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
        options.getFileEncryptionProperties());
    writer.start();

    for (Queue<TransParquetFileReader> inFiles : inputFilesR) {
      this.columnWritersR.add(new RightColumnWriter(inFiles, this));
    }
  }

  // Ctor for legacy CompressionConverter and ColumnMasker
  public ParquetRewriter(
      TransParquetFileReader reader,
      ParquetFileWriter writer,
      ParquetMetadata meta,
      MessageType schema,
      String originalCreatedBy,
      CompressionCodecName codecName,
      List<String> maskColumns,
      MaskMode maskMode) {
    this.writer = writer;
    this.schema = schema;
    this.descriptorsMap =
        schema.getColumns().stream().collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
    this.newCodecName = codecName;
    originalCreatedBy = originalCreatedBy == null ? meta.getFileMetaData().getCreatedBy() : originalCreatedBy;
    extraMetaData.putAll(meta.getFileMetaData().getKeyValueMetaData());
    extraMetaData.put(ORIGINAL_CREATED_BY_KEY, originalCreatedBy);
    if (maskColumns != null && maskMode != null) {
      this.maskColumns = new HashMap<>();
      for (String col : maskColumns) {
        this.maskColumns.put(ColumnPath.fromDotString(col), maskMode);
      }
    }
    this.inputFiles.add(reader);
    this.indexCacheStrategy = IndexCache.CacheStrategy.NONE;
  }

  private Queue<TransParquetFileReader> getFileReaders(List<InputFile> inputFiles, ParquetConfiguration conf) {
    LinkedList<TransParquetFileReader> inputFileReaders = new LinkedList<>();
    for (InputFile inputFile : inputFiles) {
      try {
        TransParquetFileReader reader = new TransParquetFileReader(
            inputFile, ParquetReadOptions.builder(conf).build());
        inputFileReaders.add(reader);
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to open input file: " + inputFile, e);
      }
    }
    return inputFileReaders;
  }

  private void ensureSameSchema(Queue<TransParquetFileReader> inputFileReaders) {
    MessageType schema = null;
    for (TransParquetFileReader reader : inputFileReaders) {
      MessageType newSchema = reader.getFooter().getFileMetaData().getSchema();
      if (schema == null) {
        schema = newSchema;
      } else {
        // Now we enforce equality of schemas from input files for simplicity.
        if (!schema.equals(newSchema)) {
          String file = reader.getFile();
          LOG.error(
              "Input files have different schemas, expect: {}, input: {}, current file: {}",
              schema,
              newSchema,
              file);
          throw new InvalidSchemaException("Input files have different schemas, current file: " + file);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    writer.end(extraMetaData);
  }

  public void processBlocks() throws IOException {
    while (!inputFiles.isEmpty()) {
      reader = inputFiles.poll();
      meta = reader.getFooter();
      originalCreatedBy = meta.getFileMetaData().getCreatedBy();
      LOG.info("Rewriting input file: {}, remaining files: {}", reader.getFile(), inputFiles.size());
      IndexCache indexCache = IndexCache.create(reader, descriptorsMap.keySet(), indexCacheStrategy, true);
      processBlocksFromReader(indexCache);
      indexCache.clean();
      LOG.info("Finish rewriting input file: {}", reader.getFile());
    }
  }

  private void processBlocksFromReader(IndexCache indexCache) throws IOException {
    for (int blockId = 0; blockId < meta.getBlocks().size(); blockId++) {
      BlockMetaData blockMetaData = meta.getBlocks().get(blockId);
      writer.startBlock(blockMetaData.getRowCount());
      indexCache.setBlockMetadata(blockMetaData);
      List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
      for (int i = 0, columnId = 0; i < columnsInOrder.size(); i++) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());

        // This column has been pruned.
        if (descriptor == null) {
          continue;
        }

        // If a column is encrypted, we simply throw exception.
        // Later we can add a feature to trans-encrypt it with different keys
        if (chunk.isEncrypted()) {
          throw new IOException("Column " + chunk.getPath().toDotString() + " is already encrypted");
        }

        reader.setStreamPosition(chunk.getStartingPos());
        CompressionCodecName newCodecName = this.newCodecName == null ? chunk.getCodec() : this.newCodecName;
        boolean encryptColumn =
            encryptMode && encryptColumns != null && encryptColumns.contains(chunk.getPath());

        if (maskColumns != null && maskColumns.containsKey(chunk.getPath())) {
          // Mask column and compress it again.
          MaskMode maskMode = maskColumns.get(chunk.getPath());
          if (maskMode.equals(MaskMode.NULLIFY)) {
            Type.Repetition repetition =
                descriptor.getPrimitiveType().getRepetition();
            if (repetition.equals(Type.Repetition.REQUIRED)) {
              throw new IOException("Required column ["
                  + descriptor.getPrimitiveType().getName() + "] cannot be nullified");
            }
            nullifyColumn(blockId, descriptor, chunk, writer, schema, newCodecName, encryptColumn);
          } else {
            throw new UnsupportedOperationException("Only nullify is supported for now");
          }
        } else if (encryptMode || this.newCodecName != null) {
          // Prepare encryption context
          ColumnChunkEncryptorRunTime columnChunkEncryptorRunTime = null;
          if (encryptMode) {
            columnChunkEncryptorRunTime = new ColumnChunkEncryptorRunTime(
                writer.getEncryptor(), chunk, numBlocksRewritten, columnId);
          }

          // Translate compression and/or encryption
          writer.startColumn(descriptor, chunk.getValueCount(), newCodecName);
          processChunk(
              blockMetaData.getRowCount(),
              chunk,
              newCodecName,
              columnChunkEncryptorRunTime,
              encryptColumn,
              indexCache.getBloomFilter(chunk),
              indexCache.getColumnIndex(chunk),
              indexCache.getOffsetIndex(chunk));
          writer.endColumn();
        } else {
          // Nothing changed, simply copy the binary data.
          BloomFilter bloomFilter = indexCache.getBloomFilter(chunk);
          ColumnIndex columnIndex = indexCache.getColumnIndex(chunk);
          OffsetIndex offsetIndex = indexCache.getOffsetIndex(chunk);
          writer.appendColumnChunk(
              descriptor, reader.getStream(), chunk, bloomFilter, columnIndex, offsetIndex);
        }

        columnId++;
      }

      // Writing extra columns
      for (RightColumnWriter columnWriter : columnWritersR) {
        columnWriter.writeRows(numBlocksRewritten, blockMetaData.getRowCount());
      }

      writer.endBlock();
      numBlocksRewritten++;
    }
  }

  private void processChunk(
      long blockRowCount,
      ColumnChunkMetaData chunk,
      CompressionCodecName newCodecName,
      ColumnChunkEncryptorRunTime columnChunkEncryptorRunTime,
      boolean encryptColumn,
      BloomFilter bloomFilter,
      ColumnIndex columnIndex,
      OffsetIndex offsetIndex)
      throws IOException {
    CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    CompressionCodecFactory.BytesInputDecompressor decompressor = null;
    CompressionCodecFactory.BytesInputCompressor compressor = null;
    if (!newCodecName.equals(chunk.getCodec())) {
      // Re-compress only if a different codec has been specified
      decompressor = codecFactory.getDecompressor(chunk.getCodec());
      compressor = codecFactory.getCompressor(newCodecName);
    }

    // EncryptorRunTime is only provided when encryption is required
    BlockCipher.Encryptor metaEncryptor = null;
    BlockCipher.Encryptor dataEncryptor = null;
    byte[] dictPageAAD = null;
    byte[] dataPageAAD = null;
    byte[] dictPageHeaderAAD = null;
    byte[] dataPageHeaderAAD = null;
    if (columnChunkEncryptorRunTime != null) {
      metaEncryptor = columnChunkEncryptorRunTime.getMetaDataEncryptor();
      dataEncryptor = columnChunkEncryptorRunTime.getDataEncryptor();
      dictPageAAD = columnChunkEncryptorRunTime.getDictPageAAD();
      dataPageAAD = columnChunkEncryptorRunTime.getDataPageAAD();
      dictPageHeaderAAD = columnChunkEncryptorRunTime.getDictPageHeaderAAD();
      dataPageHeaderAAD = columnChunkEncryptorRunTime.getDataPageHeaderAAD();
    }

    if (bloomFilter != null) {
      writer.addBloomFilter(chunk.getPath().toDotString(), bloomFilter);
    }

    reader.setStreamPosition(chunk.getStartingPos());
    DictionaryPage dictionaryPage = null;
    long readValues = 0L;
    long readRows = 0L;
    Statistics<?> statistics = null;
    boolean isColumnStatisticsMalformed = false;
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    int pageOrdinal = 0;
    long totalChunkValues = chunk.getValueCount();
    while (readValues < totalChunkValues) {
      PageHeader pageHeader = reader.readPageHeader();
      int compressedPageSize = pageHeader.getCompressed_page_size();
      byte[] pageLoad;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException("has more than one dictionary page in column chunk: " + chunk);
          }
          // No quickUpdatePageAAD needed for dictionary page
          DictionaryPageHeader dictPageHeader = pageHeader.dictionary_page_header;
          pageLoad = processPageLoad(
              reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dictPageAAD);
          dictionaryPage = new DictionaryPage(
              BytesInput.from(pageLoad),
              pageHeader.getUncompressed_page_size(),
              dictPageHeader.getNum_values(),
              converter.getEncoding(dictPageHeader.getEncoding()));
          writer.writeDictionaryPage(dictionaryPage, metaEncryptor, dictPageHeaderAAD);
          break;
        case DATA_PAGE:
          if (encryptColumn) {
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
          }
          DataPageHeader headerV1 = pageHeader.data_page_header;
          pageLoad = processPageLoad(
              reader,
              true,
              compressor,
              decompressor,
              pageHeader.getCompressed_page_size(),
              pageHeader.getUncompressed_page_size(),
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy,
              chunk.getPrimitiveType(),
              headerV1.getStatistics(),
              columnIndex,
              pageOrdinal,
              converter);
          if (statistics == null) {
            // Reach here means both the columnIndex and the page header statistics are null
            isColumnStatisticsMalformed = true;
          } else {
            Preconditions.checkState(
                !isColumnStatisticsMalformed,
                "Detected mixed null page statistics and non-null page statistics");
          }
          readValues += headerV1.getNum_values();
          if (offsetIndex != null) {
            long rowCount = 1
                + offsetIndex.getLastRowIndex(pageOrdinal, blockRowCount)
                - offsetIndex.getFirstRowIndex(pageOrdinal);
            readRows += rowCount;
            writer.writeDataPage(
                toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                toIntWithCheck(rowCount),
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()),
                metaEncryptor,
                dataPageHeaderAAD);
          } else {
            writer.writeDataPage(
                toIntWithCheck(headerV1.getNum_values()),
                pageHeader.getUncompressed_page_size(),
                BytesInput.from(pageLoad),
                statistics,
                converter.getEncoding(headerV1.getRepetition_level_encoding()),
                converter.getEncoding(headerV1.getDefinition_level_encoding()),
                converter.getEncoding(headerV1.getEncoding()),
                metaEncryptor,
                dataPageHeaderAAD);
          }
          pageOrdinal++;
          break;
        case DATA_PAGE_V2:
          if (encryptColumn) {
            AesCipher.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
            AesCipher.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
          }
          DataPageHeaderV2 headerV2 = pageHeader.data_page_header_v2;
          int rlLength = headerV2.getRepetition_levels_byte_length();
          BytesInput rlLevels = readBlockAllocate(rlLength, reader);
          int dlLength = headerV2.getDefinition_levels_byte_length();
          BytesInput dlLevels = readBlockAllocate(dlLength, reader);
          int payLoadLength = pageHeader.getCompressed_page_size() - rlLength - dlLength;
          int rawDataLength = pageHeader.getUncompressed_page_size() - rlLength - dlLength;
          pageLoad = processPageLoad(
              reader,
              headerV2.is_compressed,
              compressor,
              decompressor,
              payLoadLength,
              rawDataLength,
              encryptColumn,
              dataEncryptor,
              dataPageAAD);
          statistics = convertStatistics(
              originalCreatedBy,
              chunk.getPrimitiveType(),
              headerV2.getStatistics(),
              columnIndex,
              pageOrdinal,
              converter);
          if (statistics == null) {
            // Reach here means both the columnIndex and the page header statistics are null
            isColumnStatisticsMalformed = true;
          } else {
            Preconditions.checkState(
                !isColumnStatisticsMalformed,
                "Detected mixed null page statistics and non-null page statistics");
          }
          readValues += headerV2.getNum_values();
          readRows += headerV2.getNum_rows();
          writer.writeDataPageV2(
              headerV2.getNum_rows(),
              headerV2.getNum_nulls(),
              headerV2.getNum_values(),
              rlLevels,
              dlLevels,
              converter.getEncoding(headerV2.getEncoding()),
              BytesInput.from(pageLoad),
              rawDataLength,
              statistics,
              metaEncryptor,
              dataPageHeaderAAD);
          pageOrdinal++;
          break;
        default:
          LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
          break;
      }
    }

    Preconditions.checkState(
        readRows == 0 || readRows == blockRowCount,
        "Read row count: %s not match with block total row count: %s",
        readRows,
        blockRowCount);

    if (isColumnStatisticsMalformed) {
      // All the column statistics are invalid, so we need to overwrite the column statistics
      writer.invalidateStatistics(chunk.getStatistics());
    }
  }

  private Statistics<?> convertStatistics(
      String createdBy,
      PrimitiveType type,
      org.apache.parquet.format.Statistics pageStatistics,
      ColumnIndex columnIndex,
      int pageIndex,
      ParquetMetadataConverter converter)
      throws IOException {
    if (columnIndex != null) {
      if (columnIndex.getNullPages() == null) {
        throw new IOException(
            "columnIndex has null variable 'nullPages' which indicates corrupted data for type: "
                + type.getName());
      }
      if (pageIndex > columnIndex.getNullPages().size()) {
        throw new IOException(
            "There are more pages " + pageIndex + " found in the column than in the columnIndex "
                + columnIndex.getNullPages().size());
      }
      org.apache.parquet.column.statistics.Statistics.Builder statsBuilder =
          org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
      statsBuilder.withNumNulls(columnIndex.getNullCounts().get(pageIndex));

      if (!columnIndex.getNullPages().get(pageIndex)) {
        statsBuilder.withMin(
            columnIndex.getMinValues().get(pageIndex).array().clone());
        statsBuilder.withMax(
            columnIndex.getMaxValues().get(pageIndex).array().clone());
      }
      return statsBuilder.build();
    } else if (pageStatistics != null) {
      return converter.fromParquetStatistics(createdBy, pageStatistics, type);
    } else {
      return null;
    }
  }

  private byte[] processPageLoad(
      TransParquetFileReader reader,
      boolean isCompressed,
      CompressionCodecFactory.BytesInputCompressor compressor,
      CompressionCodecFactory.BytesInputDecompressor decompressor,
      int payloadLength,
      int rawDataLength,
      boolean encrypt,
      BlockCipher.Encryptor dataEncryptor,
      byte[] AAD)
      throws IOException {
    BytesInput data = readBlock(payloadLength, reader);

    // recompress page load
    if (compressor != null) {
      if (isCompressed) {
        data = decompressor.decompress(data, rawDataLength);
      }
      data = compressor.compress(data);
    }

    if (!encrypt) {
      return data.toByteArray();
    }

    // encrypt page load
    return dataEncryptor.encrypt(data.toByteArray(), AAD);
  }

  public BytesInput readBlock(int length, TransParquetFileReader reader) throws IOException {
    byte[] data;
    if (length > pageBufferSize) {
      data = new byte[length];
    } else {
      data = pageBuffer;
    }
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  public BytesInput readBlockAllocate(int length, TransParquetFileReader reader) throws IOException {
    byte[] data = new byte[length];
    reader.blockRead(data, 0, length);
    return BytesInput.from(data, 0, length);
  }

  private int toIntWithCheck(long size) {
    if ((int) size != size) {
      throw new ParquetEncodingException("size is bigger than " + Integer.MAX_VALUE + " bytes: " + size);
    }
    return (int) size;
  }

  // We have to rewrite getPaths because MessageType only get level 0 paths
  private void getPaths(GroupType schema, List<String> paths, String parent) {
    List<Type> fields = schema.getFields();
    String prefix = (parent == null) ? "" : parent + ".";
    for (Type field : fields) {
      paths.add(prefix + field.getName());
      if (field instanceof GroupType) {
        getPaths(field.asGroupType(), paths, prefix + field.getName());
      }
    }
  }

  private MessageType pruneColumnsInSchema(MessageType schema, Set<ColumnPath> prunePaths) {
    List<Type> fields = schema.getFields();
    List<String> currentPath = new ArrayList<>();
    List<Type> prunedFields = pruneColumnsInFields(fields, currentPath, prunePaths);
    return new MessageType(schema.getName(), prunedFields);
  }

  private List<Type> pruneColumnsInFields(List<Type> fields, List<String> currentPath, Set<ColumnPath> prunePaths) {
    List<Type> prunedFields = new ArrayList<>();
    for (Type childField : fields) {
      Type prunedChildField = pruneColumnsInField(childField, currentPath, prunePaths);
      if (prunedChildField != null) {
        prunedFields.add(prunedChildField);
      }
    }
    return prunedFields;
  }

  private Type pruneColumnsInField(Type field, List<String> currentPath, Set<ColumnPath> prunePaths) {
    String fieldName = field.getName();
    currentPath.add(fieldName);
    ColumnPath path = ColumnPath.get(currentPath.toArray(new String[0]));
    Type prunedField = null;
    if (!prunePaths.contains(path)) {
      if (field.isPrimitive()) {
        prunedField = field;
      } else {
        List<Type> childFields = ((GroupType) field).getFields();
        List<Type> prunedFields = pruneColumnsInFields(childFields, currentPath, prunePaths);
        if (!prunedFields.isEmpty()) {
          prunedField = ((GroupType) field).withNewFields(prunedFields);
        }
      }
    }

    currentPath.remove(currentPath.size() - 1);
    return prunedField;
  }

  private Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
  }

  private void nullifyColumn(
      int blockIndex,
      ColumnDescriptor descriptor,
      ColumnChunkMetaData chunk,
      ParquetFileWriter writer,
      MessageType schema,
      CompressionCodecName newCodecName,
      boolean encryptColumn)
      throws IOException {
    if (encryptColumn) {
      Preconditions.checkArgument(writer.getEncryptor() != null, "Missing encryptor");
    }

    long totalChunkValues = chunk.getValueCount();
    int dMax = descriptor.getMaxDefinitionLevel();
    PageReadStore pageReadStore = reader.readRowGroup(blockIndex);
    ColumnReadStoreImpl crStore =
        new ColumnReadStoreImpl(pageReadStore, new DummyGroupConverter(), schema, originalCreatedBy);
    ColumnReader cReader = crStore.getColumnReader(descriptor);

    ParquetProperties.WriterVersion writerVersion = chunk.getEncodingStats().usesV2Pages()
        ? ParquetProperties.WriterVersion.PARQUET_2_0
        : ParquetProperties.WriterVersion.PARQUET_1_0;
    ParquetProperties props =
        ParquetProperties.builder().withWriterVersion(writerVersion).build();
    CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(newCodecName);

    // Create new schema that only has the current column
    MessageType newSchema = newSchema(schema, descriptor);
    ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
        compressor,
        newSchema,
        props.getAllocator(),
        props.getColumnIndexTruncateLength(),
        props.getPageWriteChecksumEnabled(),
        writer.getEncryptor(),
        numBlocksRewritten);
    ColumnWriteStore cStore = props.newColumnWriteStore(newSchema, cPageStore);
    ColumnWriter cWriter = cStore.getColumnWriter(descriptor);

    for (int i = 0; i < totalChunkValues; i++) {
      int rlvl = cReader.getCurrentRepetitionLevel();
      int dlvl = cReader.getCurrentDefinitionLevel();
      if (dlvl == dMax) {
        // since we checked ether optional or repeated, dlvl should be > 0
        if (dlvl == 0) {
          throw new IOException("definition level is detected to be 0 for column "
              + chunk.getPath().toDotString() + " to be nullified");
        }
        // we just write one null for the whole list at the top level,
        // instead of nullify the elements in the list one by one
        if (rlvl == 0) {
          cWriter.writeNull(rlvl, dlvl - 1);
        }
      } else {
        cWriter.writeNull(rlvl, dlvl);
      }
      cStore.endRecord();
    }

    pageReadStore.close();
    cStore.flush();
    cPageStore.flushToFileWriter(writer);

    cStore.close();
    cWriter.close();
  }

  private MessageType newSchema(MessageType schema, ColumnDescriptor descriptor) {
    String[] path = descriptor.getPath();
    Type type = schema.getType(path);
    if (path.length == 1) {
      return new MessageType(schema.getName(), type);
    }

    for (Type field : schema.getFields()) {
      if (!field.isPrimitive()) {
        Type newType = extractField(field.asGroupType(), type);
        if (newType != null) {
          return new MessageType(schema.getName(), newType);
        }
      }
    }

    // We should never hit this because 'type' is returned by schema.getType().
    throw new RuntimeException("No field is found");
  }

  private Type extractField(GroupType candidate, Type targetField) {
    if (targetField.equals(candidate)) {
      return targetField;
    }

    // In case 'type' is a descendants of candidate
    for (Type field : candidate.asGroupType().getFields()) {
      if (field.isPrimitive()) {
        if (field.equals(targetField)) {
          return new GroupType(candidate.getRepetition(), candidate.getName(), targetField);
        }
      } else {
        Type tempField = extractField(field.asGroupType(), targetField);
        if (tempField != null) {
          return new GroupType(candidate.getRepetition(), candidate.getName(), tempField);
        }
      }
    }

    return null;
  }

  private static final class DummyGroupConverter extends GroupConverter {
    @Override
    public void start() {}

    @Override
    public void end() {}

    @Override
    public Converter getConverter(int fieldIndex) {
      return new DummyConverter();
    }
  }

  private static final class DummyConverter extends PrimitiveConverter {
    @Override
    public GroupConverter asGroupConverter() {
      return new DummyGroupConverter();
    }
  }

  private static class ColumnChunkEncryptorRunTime {
    private final InternalColumnEncryptionSetup colEncrSetup;
    private final BlockCipher.Encryptor dataEncryptor;
    private final BlockCipher.Encryptor metaDataEncryptor;
    private final byte[] fileAAD;

    private final byte[] dataPageHeaderAAD;
    private final byte[] dataPageAAD;
    private final byte[] dictPageHeaderAAD;
    private final byte[] dictPageAAD;

    public ColumnChunkEncryptorRunTime(
        InternalFileEncryptor fileEncryptor, ColumnChunkMetaData chunk, int blockId, int columnId)
        throws IOException {
      Preconditions.checkArgument(
          fileEncryptor != null, "FileEncryptor is required to create ColumnChunkEncryptorRunTime");

      this.colEncrSetup = fileEncryptor.getColumnSetup(chunk.getPath(), true, columnId);
      this.dataEncryptor = colEncrSetup.getDataEncryptor();
      this.metaDataEncryptor = colEncrSetup.getMetaDataEncryptor();

      this.fileAAD = fileEncryptor.getFileAAD();
      if (colEncrSetup != null && colEncrSetup.isEncrypted()) {
        this.dataPageHeaderAAD = createAAD(ModuleType.DataPageHeader, blockId, columnId);
        this.dataPageAAD = createAAD(ModuleType.DataPage, blockId, columnId);
        this.dictPageHeaderAAD = createAAD(ModuleType.DictionaryPageHeader, blockId, columnId);
        this.dictPageAAD = createAAD(ModuleType.DictionaryPage, blockId, columnId);
      } else {
        this.dataPageHeaderAAD = null;
        this.dataPageAAD = null;
        this.dictPageHeaderAAD = null;
        this.dictPageAAD = null;
      }
    }

    private byte[] createAAD(ModuleType moduleType, int blockId, int columnId) {
      return AesCipher.createModuleAAD(fileAAD, moduleType, blockId, columnId, 0);
    }

    public BlockCipher.Encryptor getDataEncryptor() {
      return this.dataEncryptor;
    }

    public BlockCipher.Encryptor getMetaDataEncryptor() {
      return this.metaDataEncryptor;
    }

    public byte[] getDataPageHeaderAAD() {
      return this.dataPageHeaderAAD;
    }

    public byte[] getDataPageAAD() {
      return this.dataPageAAD;
    }

    public byte[] getDictPageHeaderAAD() {
      return this.dictPageHeaderAAD;
    }

    public byte[] getDictPageAAD() {
      return this.dictPageAAD;
    }
  }

  private static class RightColumnWriter {
    private final Queue<TransParquetFileReader> inputFiles;
    private final ParquetRewriter parquetRewriter;
    private final ParquetFileWriter writer;
    private final MessageType schema;
    private final Map<ColumnPath, ColumnDescriptor> descriptorsMap;
    private final Map<ColumnDescriptor, ColumnReader> colReaders = new HashMap<>();
    private final Map<ColumnDescriptor, ColumnChunkPageWriteStore> cPageStores = new HashMap<>();
    private final Map<ColumnDescriptor, ColumnWriteStore> cStores = new HashMap<>();
    private final Map<ColumnDescriptor, ColumnWriter> cWriters = new HashMap<>();
    private int rowGroupIdxL = 0; // index of the rowGroup of the current file on the left
    private int rowGroupIdxR = 0; // index of the rowGroup of the current file on the right
    private int writtenFromBlock = 0;

    public RightColumnWriter(Queue<TransParquetFileReader> inputFiles, ParquetRewriter parquetRewriter)
        throws IOException {
      this.inputFiles = inputFiles;
      this.parquetRewriter = parquetRewriter;
      this.writer = parquetRewriter.writer;
      this.schema = inputFiles.peek().getFooter().getFileMetaData().getSchema();
      this.descriptorsMap = this.schema.getColumns().stream()
          .collect(Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      initReaders();
      initWriters();
    }

    public void writeRows(int rowGroupIdx, long rowsToWrite) throws IOException {
      if (rowGroupIdxL > rowGroupIdx) {
        throw new IOException("A row group index decrease is determined in RightColumnWriter! Current index: "
            + rowGroupIdxL + ", new index: " + rowGroupIdx);
      }
      if (rowGroupIdxL != rowGroupIdx) {
        rowGroupIdxL = rowGroupIdx;
        flushWriters();
        initWriters();
      }
      while (rowsToWrite > 0) {
        List<BlockMetaData> blocks = inputFiles.peek().getFooter().getBlocks();
        BlockMetaData block = blocks.get(rowGroupIdxR);
        List<ColumnChunkMetaData> chunks = block.getColumns();
        long leftInBlock = block.getRowCount() - writtenFromBlock;
        long writeFromBlock = Math.min(rowsToWrite, leftInBlock);
        for (ColumnChunkMetaData chunk : chunks) {
          if (chunk.isEncrypted()) {
            throw new IOException("Column " + chunk.getPath().toDotString() + " is encrypted");
          }
          ColumnDescriptor descriptor = descriptorsMap.get(chunk.getPath());
          copyValues(descriptor, writeFromBlock);
        }
        rowsToWrite -= writeFromBlock;
        writtenFromBlock += writeFromBlock;
        if (rowsToWrite > 0 || (block.getRowCount() == writtenFromBlock)) {
          rowGroupIdxR++;
          if (rowGroupIdxR == blocks.size()) {
            inputFiles.poll();
            rowGroupIdxR = 0;
          }
          writtenFromBlock = 0;
          // this is called after all rows are processed
          initReaders();
        }
      }
      flushWriters();
    }

    private void flushWriters() throws IOException {
      cStores.values().forEach(cStore -> {
        cStore.flush();
        cStore.close();
      });
      cWriters.values().forEach(ColumnWriter::close);
      for (ColumnDescriptor descriptor : descriptorsMap.values()) {
        if (cPageStores.containsKey(descriptor))
          cPageStores.get(descriptor).flushToFileWriter(writer);
      }
      cStores.clear();
      cWriters.clear();
      cPageStores.clear();
    }

    private void initWriters() {
      if (!inputFiles.isEmpty()) {
        List<BlockMetaData> blocks = inputFiles.peek().getFooter().getBlocks();
        descriptorsMap.forEach((columnPath, descriptor) -> {
          ColumnChunkMetaData chunk = blocks.get(rowGroupIdxR).getColumns().stream()
              .filter(x -> x.getPath() == columnPath)
              .findFirst()
              .orElseThrow(() -> new IllegalStateException(
                  "Could not find column [" + columnPath.toDotString() + "]."));
          int bloomFilterLength = chunk.getBloomFilterLength();
          ParquetProperties.WriterVersion writerVersion =
              chunk.getEncodingStats().usesV2Pages()
                  ? ParquetProperties.WriterVersion.PARQUET_2_0
                  : ParquetProperties.WriterVersion.PARQUET_1_0;
          ParquetProperties props = ParquetProperties.builder()
              .withWriterVersion(writerVersion)
              .withBloomFilterEnabled(bloomFilterLength > 0)
              .build();
          CodecFactory codecFactory = new CodecFactory(new Configuration(), props.getPageSizeThreshold());
          CompressionCodecFactory.BytesInputCompressor compressor =
              codecFactory.getCompressor(chunk.getCodec());

          MessageType columnSchema = parquetRewriter.newSchema(schema, descriptor);
          ColumnChunkPageWriteStore cPageStore = new ColumnChunkPageWriteStore(
              compressor,
              columnSchema,
              props.getAllocator(),
              props.getColumnIndexTruncateLength(),
              props.getPageWriteChecksumEnabled(),
              writer.getEncryptor(),
              rowGroupIdxL);
          ColumnWriteStore cwStore = props.newColumnWriteStore(columnSchema, cPageStore, cPageStore);
          ColumnWriter cWriter = cwStore.getColumnWriter(descriptor);
          cPageStores.put(descriptor, cPageStore);
          cStores.put(descriptor, cwStore);
          cWriters.put(descriptor, cWriter);
        });
      }
    }

    private void initReaders() throws IOException {
      if (!inputFiles.isEmpty()) {
        TransParquetFileReader reader = inputFiles.peek();
        PageReadStore pageReadStore = reader.readRowGroup(rowGroupIdxR);
        String createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
        ColumnReadStoreImpl crStore = new ColumnReadStoreImpl(
            pageReadStore, new ParquetRewriter.DummyGroupConverter(), schema, createdBy);
        for (ColumnDescriptor descriptor : descriptorsMap.values()) {
          ColumnReader cReader = crStore.getColumnReader(descriptor);
          colReaders.put(descriptor, cReader);
        }
      }
    }

    private void copyValues(ColumnDescriptor descriptor, long rowsToWrite) {
      ColumnWriteStore cStore = cStores.get(descriptor);
      ColumnWriter cWriter = cWriters.get(descriptor);
      int dMax = descriptor.getMaxDefinitionLevel();
      Class<?> columnType = descriptor.getPrimitiveType().getPrimitiveTypeName().javaType;
      ColumnReader reader = colReaders.get(descriptor);
      for (int i = 0; i < rowsToWrite; i++) {
        int rlvl = reader.getCurrentRepetitionLevel();
        int dlvl = reader.getCurrentDefinitionLevel();
        do {
          if (dlvl < dMax) {
            cWriter.writeNull(rlvl, dlvl);
          } else if (columnType == Integer.TYPE) {
            cWriter.write(reader.getInteger(), rlvl, dlvl);
          } else if (columnType == Long.TYPE) {
            cWriter.write(reader.getLong(), rlvl, dlvl);
          } else if (columnType == Float.TYPE) {
            cWriter.write(reader.getFloat(), rlvl, dlvl);
          } else if (columnType == Double.TYPE) {
            cWriter.write(reader.getDouble(), rlvl, dlvl);
          } else if (columnType == Binary.class) {
            cWriter.write(reader.getBinary(), rlvl, dlvl);
          } else if (columnType == Boolean.TYPE) {
            cWriter.write(reader.getBoolean(), rlvl, dlvl);
          } else {
            throw new UnsupportedOperationException(
                String.format("Unsupported column java class: %s", columnType.toString()));
          }
          reader.consume();
          rlvl = reader.getCurrentRepetitionLevel();
          dlvl = reader.getCurrentDefinitionLevel();
        } while (rlvl > 0);
        cStore.endRecord();
      }
    }
  }
}
