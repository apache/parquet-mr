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
package org.apache.parquet.format.converter;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.parquet.format.Util.readColumnMetaData;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.apache.parquet.format.Util.writeColumnMetaData;
import static org.apache.parquet.format.Util.writePageHeader;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.AesGcmEncryptor;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TagVerificationException;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterAlgorithm;
import org.apache.parquet.format.BloomFilterCompression;
import org.apache.parquet.format.BloomFilterHash;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.BsonType;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.EnumType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Float16Type;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.JsonType;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.ListType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.NullType;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.SizeStatistics;
import org.apache.parquet.format.SplitBlockAlgorithm;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.UUIDType;
import org.apache.parquet.format.Uncompressed;
import org.apache.parquet.format.XxHash;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.BinaryTruncator;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.InvalidFileOffsetException;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.TypeVisitor;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This file has become too long!
// TODO: Lets split it up: https://issues.apache.org/jira/browse/PARQUET-310
public class ParquetMetadataConverter {

  private static final TypeDefinedOrder TYPE_DEFINED_ORDER = new TypeDefinedOrder();
  public static final MetadataFilter NO_FILTER = new NoFilter();
  public static final MetadataFilter SKIP_ROW_GROUPS = new SkipMetadataFilter();
  public static final long MAX_STATS_SIZE = 4096; // limit stats to 4k

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataConverter.class);
  private static final LogicalTypeConverterVisitor LOGICAL_TYPE_ANNOTATION_VISITOR =
      new LogicalTypeConverterVisitor();
  private static final ConvertedTypeConverterVisitor CONVERTED_TYPE_CONVERTER_VISITOR =
      new ConvertedTypeConverterVisitor();
  private final int statisticsTruncateLength;
  private final boolean useSignedStringMinMax;

  public ParquetMetadataConverter() {
    this(false);
  }

  public ParquetMetadataConverter(int statisticsTruncateLength) {
    this(false, statisticsTruncateLength);
  }

  /**
   * @param conf a configuration
   * @deprecated will be removed in 2.0.0; use {@code ParquetMetadataConverter(ParquetReadOptions)}
   */
  @Deprecated
  public ParquetMetadataConverter(Configuration conf) {
    this(conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
  }

  public ParquetMetadataConverter(ParquetReadOptions options) {
    this(options.useSignedStringMinMax());
  }

  private ParquetMetadataConverter(boolean useSignedStringMinMax) {
    this(useSignedStringMinMax, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
  }

  private ParquetMetadataConverter(boolean useSignedStringMinMax, int statisticsTruncateLength) {
    if (statisticsTruncateLength <= 0) {
      throw new IllegalArgumentException("Truncate length should be greater than 0");
    }
    this.useSignedStringMinMax = useSignedStringMinMax;
    this.statisticsTruncateLength = statisticsTruncateLength;
  }

  // NOTE: this cache is for memory savings, not cpu savings, and is used to de-duplicate
  // sets of encodings. It is important that all collections inserted to this cache be
  // immutable and have thread-safe read-only access. This can be achieved by wrapping
  // an unsynchronized collection in Collections.unmodifiable*(), and making sure to not
  // keep any references to the original collection.
  private static final ConcurrentHashMap<
          Set<org.apache.parquet.column.Encoding>, Set<org.apache.parquet.column.Encoding>>
      cachedEncodingSets = new ConcurrentHashMap<
          Set<org.apache.parquet.column.Encoding>, Set<org.apache.parquet.column.Encoding>>();

  public FileMetaData toParquetMetadata(int currentVersion, ParquetMetadata parquetMetadata) {
    return toParquetMetadata(currentVersion, parquetMetadata, null);
  }

  public FileMetaData toParquetMetadata(
      int currentVersion, ParquetMetadata parquetMetadata, InternalFileEncryptor fileEncryptor) {
    List<BlockMetaData> blocks = parquetMetadata.getBlocks();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    long numRows = 0;
    long preBlockStartPos = 0;
    long preBlockCompressedSize = 0;
    for (BlockMetaData block : blocks) {
      numRows += block.getRowCount();
      long blockStartPos = block.getStartingPos();
      // first block
      if (blockStartPos == 4) {
        preBlockStartPos = 0;
        preBlockCompressedSize = 0;
      }
      if (preBlockStartPos != 0) {
        Preconditions.checkState(
            blockStartPos >= preBlockStartPos + preBlockCompressedSize,
            "Invalid block starting position: %s",
            blockStartPos);
      }
      preBlockStartPos = blockStartPos;
      preBlockCompressedSize = block.getCompressedSize();
      addRowGroup(parquetMetadata, rowGroups, block, fileEncryptor);
    }
    FileMetaData fileMetaData = new FileMetaData(
        currentVersion,
        toParquetSchema(parquetMetadata.getFileMetaData().getSchema()),
        numRows,
        rowGroups);

    Set<Entry<String, String>> keyValues =
        parquetMetadata.getFileMetaData().getKeyValueMetaData().entrySet();
    for (Entry<String, String> keyValue : keyValues) {
      addKeyValue(fileMetaData, keyValue.getKey(), keyValue.getValue());
    }

    fileMetaData.setCreated_by(parquetMetadata.getFileMetaData().getCreatedBy());

    fileMetaData.setColumn_orders(
        getColumnOrders(parquetMetadata.getFileMetaData().getSchema()));

    return fileMetaData;
  }

  private List<ColumnOrder> getColumnOrders(MessageType schema) {
    List<ColumnOrder> columnOrders = new ArrayList<>();
    // Currently, only TypeDefinedOrder is supported, so we create a column order for each columns with
    // TypeDefinedOrder even if some types (e.g. INT96) have undefined column orders.
    for (int i = 0, n = schema.getPaths().size(); i < n; ++i) {
      ColumnOrder columnOrder = new ColumnOrder();
      columnOrder.setTYPE_ORDER(TYPE_DEFINED_ORDER);
      columnOrders.add(columnOrder);
    }
    return columnOrders;
  }

  // Visible for testing
  List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = new ArrayList<SchemaElement>();
    addToList(result, schema);
    return result;
  }

  private void addToList(final List<SchemaElement> result, org.apache.parquet.schema.Type field) {
    field.accept(new TypeVisitor() {
      @Override
      public void visit(PrimitiveType primitiveType) {
        SchemaElement element = new SchemaElement(primitiveType.getName());
        element.setRepetition_type(toParquetRepetition(primitiveType.getRepetition()));
        element.setType(getType(primitiveType.getPrimitiveTypeName()));
        if (primitiveType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(primitiveType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(primitiveType.getLogicalTypeAnnotation()));
        }
        if (primitiveType.getDecimalMetadata() != null) {
          element.setPrecision(primitiveType.getDecimalMetadata().getPrecision());
          element.setScale(primitiveType.getDecimalMetadata().getScale());
        }
        if (primitiveType.getTypeLength() > 0) {
          element.setType_length(primitiveType.getTypeLength());
        }
        if (primitiveType.getId() != null) {
          element.setField_id(primitiveType.getId().intValue());
        }
        result.add(element);
      }

      @Override
      public void visit(MessageType messageType) {
        SchemaElement element = new SchemaElement(messageType.getName());
        if (messageType.getId() != null) {
          element.setField_id(messageType.getId().intValue());
        }
        visitChildren(result, messageType.asGroupType(), element);
      }

      @Override
      public void visit(GroupType groupType) {
        SchemaElement element = new SchemaElement(groupType.getName());
        element.setRepetition_type(toParquetRepetition(groupType.getRepetition()));
        if (groupType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(groupType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(groupType.getLogicalTypeAnnotation()));
        }
        if (groupType.getId() != null) {
          element.setField_id(groupType.getId().intValue());
        }
        visitChildren(result, groupType, element);
      }

      private void visitChildren(final List<SchemaElement> result, GroupType groupType, SchemaElement element) {
        element.setNum_children(groupType.getFieldCount());
        result.add(element);
        for (org.apache.parquet.schema.Type field : groupType.getFields()) {
          addToList(result, field);
        }
      }
    });
  }

  LogicalType convertToLogicalType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(LOGICAL_TYPE_ANNOTATION_VISITOR).orElse(null);
  }

  ConvertedType convertToConvertedType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(CONVERTED_TYPE_CONVERTER_VISITOR).orElse(null);
  }

  static org.apache.parquet.format.TimeUnit convertUnit(LogicalTypeAnnotation.TimeUnit unit) {
    switch (unit) {
      case MICROS:
        return org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
      case MILLIS:
        return org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
      case NANOS:
        return TimeUnit.NANOS(new NanoSeconds());
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  private static class ConvertedTypeConverterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ConvertedType> {
    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(ConvertedType.UTF8);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(ConvertedType.MAP);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(ConvertedType.LIST);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(ConvertedType.ENUM);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(ConvertedType.DECIMAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(ConvertedType.DATE);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      switch (timeLogicalType.getUnit()) {
        case MILLIS:
          return of(ConvertedType.TIME_MILLIS);
        case MICROS:
          return of(ConvertedType.TIME_MICROS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timeLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      switch (timestampLogicalType.getUnit()) {
        case MICROS:
          return of(ConvertedType.TIMESTAMP_MICROS);
        case MILLIS:
          return of(ConvertedType.TIMESTAMP_MILLIS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timestampLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      boolean signed = intLogicalType.isSigned();
      switch (intLogicalType.getBitWidth()) {
        case 8:
          return of(signed ? ConvertedType.INT_8 : ConvertedType.UINT_8);
        case 16:
          return of(signed ? ConvertedType.INT_16 : ConvertedType.UINT_16);
        case 32:
          return of(signed ? ConvertedType.INT_32 : ConvertedType.UINT_32);
        case 64:
          return of(signed ? ConvertedType.INT_64 : ConvertedType.UINT_64);
        default:
          throw new RuntimeException("Unknown original type " + intLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(ConvertedType.JSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(ConvertedType.BSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(ConvertedType.INTERVAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
      return of(ConvertedType.MAP_KEY_VALUE);
    }
  }

  private static class LogicalTypeConverterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<LogicalType> {
    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(LogicalType.STRING(new StringType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(LogicalType.MAP(new MapType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(LogicalType.LIST(new ListType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(LogicalType.ENUM(new EnumType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(LogicalType.DECIMAL(
          new DecimalType(decimalLogicalType.getScale(), decimalLogicalType.getPrecision())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(LogicalType.DATE(new DateType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      return of(LogicalType.TIME(
          new TimeType(timeLogicalType.isAdjustedToUTC(), convertUnit(timeLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      return of(LogicalType.TIMESTAMP(new TimestampType(
          timestampLogicalType.isAdjustedToUTC(), convertUnit(timestampLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      return of(LogicalType.INTEGER(new IntType((byte) intLogicalType.getBitWidth(), intLogicalType.isSigned())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(LogicalType.JSON(new JsonType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(LogicalType.BSON(new BsonType()));
    }

    @Override
    public Optional<LogicalType> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
      return of(LogicalType.UUID(new UUIDType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.Float16LogicalTypeAnnotation float16LogicalType) {
      return of(LogicalType.FLOAT16(new Float16Type()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(LogicalType.UNKNOWN(new NullType()));
    }
  }

  private void addRowGroup(
      ParquetMetadata parquetMetadata,
      List<RowGroup> rowGroups,
      BlockMetaData block,
      InternalFileEncryptor fileEncryptor) {

    // rowGroup.total_byte_size = ;
    List<ColumnChunkMetaData> columns = block.getColumns();
    List<ColumnChunk> parquetColumns = new ArrayList<ColumnChunk>();
    int rowGroupOrdinal = rowGroups.size();
    int columnOrdinal = -1;
    ByteArrayOutputStream tempOutStream = null;
    for (ColumnChunkMetaData columnMetaData : columns) {
      // There is no ColumnMetaData written after the chunk data, so set the ColumnChunk
      // file_offset to 0
      ColumnChunk columnChunk = new ColumnChunk(0);
      columnChunk.file_path = block.getPath(); // they are in the same file for now
      InternalColumnEncryptionSetup columnSetup = null;
      boolean writeCryptoMetadata = false;
      boolean encryptMetaData = false;
      ColumnPath path = columnMetaData.getPath();
      if (null != fileEncryptor) {
        columnOrdinal++;
        columnSetup = fileEncryptor.getColumnSetup(path, false, columnOrdinal);
        writeCryptoMetadata = columnSetup.isEncrypted();
        encryptMetaData = fileEncryptor.encryptColumnMetaData(columnSetup);
      }
      ColumnMetaData metaData = new ColumnMetaData(
          getType(columnMetaData.getType()),
          toFormatEncodings(columnMetaData.getEncodings()),
          Arrays.asList(columnMetaData.getPath().toArray()),
          toFormatCodec(columnMetaData.getCodec()),
          columnMetaData.getValueCount(),
          columnMetaData.getTotalUncompressedSize(),
          columnMetaData.getTotalSize(),
          columnMetaData.getFirstDataPageOffset());
      if (columnMetaData.getEncodingStats() != null
          && columnMetaData.getEncodingStats().hasDictionaryPages()) {
        metaData.setDictionary_page_offset(columnMetaData.getDictionaryPageOffset());
      }
      long bloomFilterOffset = columnMetaData.getBloomFilterOffset();
      if (bloomFilterOffset >= 0) {
        metaData.setBloom_filter_offset(bloomFilterOffset);
      }
      int bloomFilterLength = columnMetaData.getBloomFilterLength();
      if (bloomFilterLength >= 0) {
        metaData.setBloom_filter_length(bloomFilterLength);
      }
      if (columnMetaData.getStatistics() != null
          && !columnMetaData.getStatistics().isEmpty()) {
        metaData.setStatistics(
            toParquetStatistics(columnMetaData.getStatistics(), this.statisticsTruncateLength));
      }
      if (columnMetaData.getEncodingStats() != null) {
        metaData.setEncoding_stats(convertEncodingStats(columnMetaData.getEncodingStats()));
      }
      if (columnMetaData.getSizeStatistics() != null
          && columnMetaData.getSizeStatistics().isValid()) {
        metaData.setSize_statistics(toParquetSizeStatistics(columnMetaData.getSizeStatistics()));
      }

      if (!encryptMetaData) {
        columnChunk.setMeta_data(metaData);
      } else {
        // Serialize and encrypt ColumnMetadata separately
        byte[] columnMetaDataAAD = AesCipher.createModuleAAD(
            fileEncryptor.getFileAAD(),
            ModuleType.ColumnMetaData,
            rowGroupOrdinal,
            columnSetup.getOrdinal(),
            -1);
        if (null == tempOutStream) {
          tempOutStream = new ByteArrayOutputStream();
        } else {
          tempOutStream.reset();
        }
        try {
          writeColumnMetaData(metaData, tempOutStream, columnSetup.getMetaDataEncryptor(), columnMetaDataAAD);
        } catch (IOException e) {
          throw new ParquetCryptoRuntimeException(
              "Failed to serialize and encrypt ColumnMetadata for " + columnMetaData.getPath(), e);
        }
        columnChunk.setEncrypted_column_metadata(tempOutStream.toByteArray());
        // Keep redacted metadata version
        if (!fileEncryptor.isFooterEncrypted()) {
          ColumnMetaData metaDataRedacted = metaData.deepCopy();
          if (metaDataRedacted.isSetStatistics()) metaDataRedacted.unsetStatistics();
          if (metaDataRedacted.isSetEncoding_stats()) metaDataRedacted.unsetEncoding_stats();
          columnChunk.setMeta_data(metaDataRedacted);
        }
      }
      if (writeCryptoMetadata) {
        columnChunk.setCrypto_metadata(columnSetup.getColumnCryptoMetaData());
      }

      //      columnChunk.meta_data.index_page_offset = ;
      //      columnChunk.meta_data.key_value_metadata = ; // nothing yet

      IndexReference columnIndexRef = columnMetaData.getColumnIndexReference();
      if (columnIndexRef != null) {
        columnChunk.setColumn_index_offset(columnIndexRef.getOffset());
        columnChunk.setColumn_index_length(columnIndexRef.getLength());
      }
      IndexReference offsetIndexRef = columnMetaData.getOffsetIndexReference();
      if (offsetIndexRef != null) {
        columnChunk.setOffset_index_offset(offsetIndexRef.getOffset());
        columnChunk.setOffset_index_length(offsetIndexRef.getLength());
      }

      parquetColumns.add(columnChunk);
    }
    RowGroup rowGroup = new RowGroup(parquetColumns, block.getTotalByteSize(), block.getRowCount());
    rowGroup.setFile_offset(block.getStartingPos());
    rowGroup.setTotal_compressed_size(block.getCompressedSize());
    rowGroup.setOrdinal((short) rowGroupOrdinal);
    rowGroups.add(rowGroup);
  }

  private List<Encoding> toFormatEncodings(Set<org.apache.parquet.column.Encoding> encodings) {
    List<Encoding> converted = new ArrayList<Encoding>(encodings.size());
    for (org.apache.parquet.column.Encoding encoding : encodings) {
      converted.add(getEncoding(encoding));
    }
    return converted;
  }

  // Visible for testing
  Set<org.apache.parquet.column.Encoding> fromFormatEncodings(List<Encoding> encodings) {
    Set<org.apache.parquet.column.Encoding> converted = new HashSet<org.apache.parquet.column.Encoding>();

    for (Encoding encoding : encodings) {
      converted.add(getEncoding(encoding));
    }

    // make converted unmodifiable, drop reference to modifiable copy
    converted = Collections.unmodifiableSet(converted);

    // atomically update the cache
    Set<org.apache.parquet.column.Encoding> cached = cachedEncodingSets.putIfAbsent(converted, converted);

    if (cached == null) {
      // cached == null signifies that converted was *not* in the cache previously
      // so we can return converted instead of throwing it away, it has now
      // been cached
      cached = converted;
    }

    return cached;
  }

  private CompressionCodecName fromFormatCodec(CompressionCodec codec) {
    return CompressionCodecName.valueOf(codec.toString());
  }

  private CompressionCodec toFormatCodec(CompressionCodecName codec) {
    return CompressionCodec.valueOf(codec.toString());
  }

  public org.apache.parquet.column.Encoding getEncoding(Encoding encoding) {
    return org.apache.parquet.column.Encoding.valueOf(encoding.name());
  }

  public Encoding getEncoding(org.apache.parquet.column.Encoding encoding) {
    return Encoding.valueOf(encoding.name());
  }

  public EncodingStats convertEncodingStats(List<PageEncodingStats> stats) {
    if (stats == null) {
      return null;
    }

    EncodingStats.Builder builder = new EncodingStats.Builder();
    for (PageEncodingStats stat : stats) {
      switch (stat.getPage_type()) {
        case DATA_PAGE_V2:
          builder.withV2Pages();
          // falls through
        case DATA_PAGE:
          builder.addDataEncoding(getEncoding(stat.getEncoding()), stat.getCount());
          break;
        case DICTIONARY_PAGE:
          builder.addDictEncoding(getEncoding(stat.getEncoding()), stat.getCount());
          break;
      }
    }
    return builder.build();
  }

  public List<PageEncodingStats> convertEncodingStats(EncodingStats stats) {
    if (stats == null) {
      return null;
    }

    List<PageEncodingStats> formatStats = new ArrayList<PageEncodingStats>();
    for (org.apache.parquet.column.Encoding encoding : stats.getDictionaryEncodings()) {
      formatStats.add(new PageEncodingStats(
          PageType.DICTIONARY_PAGE, getEncoding(encoding), stats.getNumDictionaryPagesEncodedAs(encoding)));
    }
    PageType dataPageType = (stats.usesV2Pages() ? PageType.DATA_PAGE_V2 : PageType.DATA_PAGE);
    for (org.apache.parquet.column.Encoding encoding : stats.getDataEncodings()) {
      formatStats.add(new PageEncodingStats(
          dataPageType, getEncoding(encoding), stats.getNumDataPagesEncodedAs(encoding)));
    }
    return formatStats;
  }

  public static Statistics toParquetStatistics(org.apache.parquet.column.statistics.Statistics stats) {
    return toParquetStatistics(stats, ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH);
  }

  public static Statistics toParquetStatistics(
      org.apache.parquet.column.statistics.Statistics stats, int truncateLength) {
    Statistics formatStats = new Statistics();
    // Don't write stats larger than the max size rather than truncating. The
    // rationale is that some engines may use the minimum value in the page as
    // the true minimum for aggregations and there is no way to mark that a
    // value has been truncated and is a lower bound and not in the page.
    if (!stats.isEmpty() && withinLimit(stats, truncateLength)) {
      formatStats.setNull_count(stats.getNumNulls());
      if (stats.hasNonNullValue()) {
        byte[] min;
        byte[] max;

        if (stats instanceof BinaryStatistics && truncateLength != Integer.MAX_VALUE) {
          BinaryTruncator truncator = BinaryTruncator.getTruncator(stats.type());
          min = tuncateMin(truncator, truncateLength, stats.getMinBytes());
          max = tuncateMax(truncator, truncateLength, stats.getMaxBytes());
        } else {
          min = stats.getMinBytes();
          max = stats.getMaxBytes();
        }
        // Fill the former min-max statistics only if the comparison logic is
        // signed so the logic of V1 and V2 stats are the same (which is
        // trivially true for equal min-max values)
        if (sortOrder(stats.type()) == SortOrder.SIGNED || Arrays.equals(min, max)) {
          formatStats.setMin(min);
          formatStats.setMax(max);
        }

        if (isMinMaxStatsSupported(stats.type()) || Arrays.equals(min, max)) {
          formatStats.setMin_value(min);
          formatStats.setMax_value(max);
        }
      }
    }
    return formatStats;
  }

  private static boolean withinLimit(org.apache.parquet.column.statistics.Statistics stats, int truncateLength) {
    if (stats.isSmallerThan(MAX_STATS_SIZE)) {
      return true;
    }

    if (!(stats instanceof BinaryStatistics)) {
      return false;
    }

    BinaryStatistics binaryStatistics = (BinaryStatistics) stats;
    return binaryStatistics.isSmallerThanWithTruncation(MAX_STATS_SIZE, truncateLength);
  }

  private static byte[] tuncateMin(BinaryTruncator truncator, int truncateLength, byte[] input) {
    return truncator
        .truncateMin(Binary.fromConstantByteArray(input), truncateLength)
        .getBytes();
  }

  private static byte[] tuncateMax(BinaryTruncator truncator, int truncateLength, byte[] input) {
    return truncator
        .truncateMax(Binary.fromConstantByteArray(input), truncateLength)
        .getBytes();
  }

  private static boolean isMinMaxStatsSupported(PrimitiveType type) {
    return type.columnOrder().getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER;
  }

  /**
   * @param statistics parquet format statistics
   * @param type       a primitive type name
   * @return the statistics
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      Statistics statistics, PrimitiveTypeName type) {
    return fromParquetStatistics(null, statistics, type);
  }

  /**
   * @param createdBy  the created-by string from the file
   * @param statistics parquet format statistics
   * @param type       a primitive type name
   * @return the statistics
   * @deprecated will be removed in 2.0.0.
   */
  @Deprecated
  public static org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      String createdBy, Statistics statistics, PrimitiveTypeName type) {
    return fromParquetStatisticsInternal(
        createdBy,
        statistics,
        new PrimitiveType(Repetition.OPTIONAL, type, "fake_type"),
        defaultSortOrder(type));
  }

  // Visible for testing
  static org.apache.parquet.column.statistics.Statistics fromParquetStatisticsInternal(
      String createdBy, Statistics formatStats, PrimitiveType type, SortOrder typeSortOrder) {
    // create stats object based on the column type
    org.apache.parquet.column.statistics.Statistics.Builder statsBuilder =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);

    if (formatStats != null) {
      // Use the new V2 min-max statistics over the former one if it is filled
      if (formatStats.isSetMin_value() && formatStats.isSetMax_value()) {
        byte[] min = formatStats.min_value.array();
        byte[] max = formatStats.max_value.array();
        if (isMinMaxStatsSupported(type) || Arrays.equals(min, max)) {
          statsBuilder.withMin(min);
          statsBuilder.withMax(max);
        }
      } else {
        boolean isSet = formatStats.isSetMax() && formatStats.isSetMin();
        boolean maxEqualsMin = isSet ? Arrays.equals(formatStats.getMin(), formatStats.getMax()) : false;
        boolean sortOrdersMatch = SortOrder.SIGNED == typeSortOrder;
        // NOTE: See docs in CorruptStatistics for explanation of why this check is needed
        // The sort order is checked to avoid returning min/max stats that are not
        // valid with the type's sort order. In previous releases, all stats were
        // aggregated using a signed byte-wise ordering, which isn't valid for all the
        // types (e.g. strings, decimals etc.).
        if (!CorruptStatistics.shouldIgnoreStatistics(createdBy, type.getPrimitiveTypeName())
            && (sortOrdersMatch || maxEqualsMin)) {
          if (isSet) {
            statsBuilder.withMin(formatStats.min.array());
            statsBuilder.withMax(formatStats.max.array());
          }
        }
      }

      if (formatStats.isSetNull_count()) {
        statsBuilder.withNumNulls(formatStats.null_count);
      }
    }
    return statsBuilder.build();
  }

  public org.apache.parquet.column.statistics.Statistics fromParquetStatistics(
      String createdBy, Statistics statistics, PrimitiveType type) {
    SortOrder expectedOrder = overrideSortOrderToSigned(type) ? SortOrder.SIGNED : sortOrder(type);
    return fromParquetStatisticsInternal(createdBy, statistics, type, expectedOrder);
  }

  /**
   * Sort order for page and column statistics. Types are associated with sort
   * orders (e.g., UTF8 columns should use UNSIGNED) and column stats are
   * aggregated using a sort order. As of parquet-format version 2.3.1, the
   * order used to aggregate stats is always SIGNED and is not stored in the
   * Parquet file. These stats are discarded for types that need unsigned.
   * <p>
   * See PARQUET-686.
   */
  enum SortOrder {
    SIGNED,
    UNSIGNED,
    UNKNOWN
  }

  private static final Set<Class> STRING_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      LogicalTypeAnnotation.StringLogicalTypeAnnotation.class,
      LogicalTypeAnnotation.EnumLogicalTypeAnnotation.class,
      LogicalTypeAnnotation.JsonLogicalTypeAnnotation.class,
      LogicalTypeAnnotation.Float16LogicalTypeAnnotation.class)));

  /**
   * Returns whether to use signed order min and max with a type. It is safe to
   * use signed min and max when the type is a string type and contains only
   * ASCII characters (where the sign bit was 0). This checks whether the type
   * is a string type and uses {@code useSignedStringMinMax} to determine if
   * only ASCII characters were written.
   *
   * @param type a primitive type with a logical type annotation
   * @return true if signed order min/max can be used with this type
   */
  private boolean overrideSortOrderToSigned(PrimitiveType type) {
    // even if the override is set, only return stats for string-ish types
    // a null type annotation is considered string-ish because some writers
    // failed to use the UTF8 annotation.
    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    return useSignedStringMinMax
        && PrimitiveTypeName.BINARY == type.getPrimitiveTypeName()
        && (annotation == null || STRING_TYPES.contains(annotation.getClass()));
  }

  /**
   * @param primitive a primitive physical type
   * @return the default sort order used when the logical type is not known
   */
  private static SortOrder defaultSortOrder(PrimitiveTypeName primitive) {
    switch (primitive) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return SortOrder.SIGNED;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return SortOrder.UNSIGNED;
    }
    return SortOrder.UNKNOWN;
  }

  /**
   * @param primitive a primitive type with a logical type annotation
   * @return the "correct" sort order of the type that applications assume
   */
  private static SortOrder sortOrder(PrimitiveType primitive) {
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation != null) {
      return annotation
          .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<SortOrder>() {
            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
              return intLogicalType.isSigned() ? of(SortOrder.SIGNED) : of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
              return of(SortOrder.UNKNOWN);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
              return of(SortOrder.SIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
              return of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
              return of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
              return of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
              return of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
              return of(SortOrder.UNSIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.Float16LogicalTypeAnnotation float16LogicalType) {
              return of(SortOrder.SIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
              return of(SortOrder.UNKNOWN);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
              return of(SortOrder.UNKNOWN);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
              return of(SortOrder.UNKNOWN);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
              return of(SortOrder.UNKNOWN);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
              return of(SortOrder.SIGNED);
            }

            @Override
            public Optional<SortOrder> visit(
                LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
              return of(SortOrder.SIGNED);
            }
          })
          .orElse(defaultSortOrder(primitive.getPrimitiveTypeName()));
    }

    return defaultSortOrder(primitive.getPrimitiveTypeName());
  }

  public PrimitiveTypeName getPrimitive(Type type) {
    switch (type) {
      case BYTE_ARRAY: // TODO: rename BINARY and remove this switch
        return PrimitiveTypeName.BINARY;
      case INT64:
        return PrimitiveTypeName.INT64;
      case INT32:
        return PrimitiveTypeName.INT32;
      case BOOLEAN:
        return PrimitiveTypeName.BOOLEAN;
      case FLOAT:
        return PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveTypeName.DOUBLE;
      case INT96:
        return PrimitiveTypeName.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  // Visible for testing
  Type getType(PrimitiveTypeName type) {
    switch (type) {
      case INT64:
        return Type.INT64;
      case INT32:
        return Type.INT32;
      case BOOLEAN:
        return Type.BOOLEAN;
      case BINARY:
        return Type.BYTE_ARRAY;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case INT96:
        return Type.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return Type.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown primitive type " + type);
    }
  }

  // Visible for testing
  LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType type, SchemaElement schemaElement) {
    switch (type) {
      case UTF8:
        return LogicalTypeAnnotation.stringType();
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case MAP_KEY_VALUE:
        return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case DECIMAL:
        int scale = (schemaElement == null ? 0 : schemaElement.scale);
        int precision = (schemaElement == null ? 0 : schemaElement.precision);
        return LogicalTypeAnnotation.decimalType(scale, precision);
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case TIME_MILLIS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIME_MICROS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case TIMESTAMP_MILLIS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case INTERVAL:
        return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();
      case INT_8:
        return LogicalTypeAnnotation.intType(8, true);
      case INT_16:
        return LogicalTypeAnnotation.intType(16, true);
      case INT_32:
        return LogicalTypeAnnotation.intType(32, true);
      case INT_64:
        return LogicalTypeAnnotation.intType(64, true);
      case UINT_8:
        return LogicalTypeAnnotation.intType(8, false);
      case UINT_16:
        return LogicalTypeAnnotation.intType(16, false);
      case UINT_32:
        return LogicalTypeAnnotation.intType(32, false);
      case UINT_64:
        return LogicalTypeAnnotation.intType(64, false);
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      default:
        throw new RuntimeException(
            "Can't convert converted type to logical type, unknown converted type " + type);
    }
  }

  LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type) {
    switch (type.getSetField()) {
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case TIME:
        TimeType time = type.getTIME();
        return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
      case STRING:
        return LogicalTypeAnnotation.stringType();
      case DECIMAL:
        DecimalType decimal = type.getDECIMAL();
        return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
      case INTEGER:
        IntType integer = type.getINTEGER();
        return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
      case UNKNOWN:
        return null;
      case TIMESTAMP:
        TimestampType timestamp = type.getTIMESTAMP();
        return LogicalTypeAnnotation.timestampType(timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
      case UUID:
        return LogicalTypeAnnotation.uuidType();
      case FLOAT16:
        return LogicalTypeAnnotation.float16Type();
      default:
        throw new RuntimeException("Unknown logical type " + type);
    }
  }

  private LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit) {
    switch (unit.getSetField()) {
      case MICROS:
        return LogicalTypeAnnotation.TimeUnit.MICROS;
      case MILLIS:
        return LogicalTypeAnnotation.TimeUnit.MILLIS;
      case NANOS:
        return LogicalTypeAnnotation.TimeUnit.NANOS;
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  private static void addKeyValue(FileMetaData fileMetaData, String key, String value) {
    KeyValue keyValue = new KeyValue(key);
    keyValue.value = value;
    fileMetaData.addToKey_value_metadata(keyValue);
  }

  private static interface MetadataFilterVisitor<T, E extends Throwable> {
    T visit(NoFilter filter) throws E;

    T visit(SkipMetadataFilter filter) throws E;

    T visit(RangeMetadataFilter filter) throws E;

    T visit(OffsetMetadataFilter filter) throws E;
  }

  public abstract static class MetadataFilter {
    private MetadataFilter() {}

    abstract <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E;
  }

  /**
   * [ startOffset, endOffset )
   *
   * @param startOffset a start offset (inclusive)
   * @param endOffset   an end offset (exclusive)
   * @return a range filter from the offsets
   */
  public static MetadataFilter range(long startOffset, long endOffset) {
    return new RangeMetadataFilter(startOffset, endOffset);
  }

  public static MetadataFilter offsets(long... offsets) {
    Set<Long> set = new HashSet<Long>();
    for (long offset : offsets) {
      set.add(offset);
    }
    return new OffsetMetadataFilter(set);
  }

  private static final class NoFilter extends MetadataFilter {
    private NoFilter() {}

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      return "NO_FILTER";
    }
  }

  private static final class SkipMetadataFilter extends MetadataFilter {
    private SkipMetadataFilter() {}

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }

    @Override
    public String toString() {
      return "SKIP_ROW_GROUPS";
    }
  }

  /**
   * [ startOffset, endOffset )
   */
  // Visible for testing
  static final class RangeMetadataFilter extends MetadataFilter {
    final long startOffset;
    final long endOffset;

    RangeMetadataFilter(long startOffset, long endOffset) {
      super();
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }

    public boolean contains(long offset) {
      return offset >= this.startOffset && offset < this.endOffset;
    }

    @Override
    public String toString() {
      return "range(s:" + startOffset + ", e:" + endOffset + ")";
    }
  }

  static final class OffsetMetadataFilter extends MetadataFilter {
    private final Set<Long> offsets;

    public OffsetMetadataFilter(Set<Long> offsets) {
      this.offsets = offsets;
    }

    public boolean contains(long offset) {
      return offsets.contains(offset);
    }

    @Override
    <T, E extends Throwable> T accept(MetadataFilterVisitor<T, E> visitor) throws E {
      return visitor.visit(this);
    }
  }

  @Deprecated
  public ParquetMetadata readParquetMetadata(InputStream from) throws IOException {
    return readParquetMetadata(from, NO_FILTER);
  }

  // Visible for testing
  static FileMetaData filterFileMetaDataByMidpoint(FileMetaData metaData, RangeMetadataFilter filter) {
    List<RowGroup> rowGroups = metaData.getRow_groups();
    List<RowGroup> newRowGroups = new ArrayList<RowGroup>();
    long preStartIndex = 0;
    long preCompressedSize = 0;
    boolean firstColumnWithMetadata = true;
    if (rowGroups != null && !rowGroups.isEmpty()) {
      firstColumnWithMetadata = rowGroups.get(0).getColumns().get(0).isSetMeta_data();
    }
    for (RowGroup rowGroup : rowGroups) {
      long totalSize = 0;
      long startIndex;
      ColumnChunk columnChunk = rowGroup.getColumns().get(0);
      if (firstColumnWithMetadata) {
        startIndex = getOffset(columnChunk);
      } else {
        assert rowGroup.isSetFile_offset();
        assert rowGroup.isSetTotal_compressed_size();

        // the file_offset of first block always holds the truth, while other blocks don't :
        // see PARQUET-2078 for details
        startIndex = rowGroup.getFile_offset();
        if (invalidFileOffset(startIndex, preStartIndex, preCompressedSize)) {
          // first row group's offset is always 4
          if (preStartIndex == 0) {
            startIndex = 4;
          } else {
            // use minStartIndex(imprecise in case of padding, but good enough for filtering)
            startIndex = preStartIndex + preCompressedSize;
          }
        }
        preStartIndex = startIndex;
        preCompressedSize = rowGroup.getTotal_compressed_size();
      }

      if (rowGroup.isSetTotal_compressed_size()) {
        totalSize = rowGroup.getTotal_compressed_size();
      } else {
        for (ColumnChunk col : rowGroup.getColumns()) {
          totalSize += col.getMeta_data().getTotal_compressed_size();
        }
      }

      long midPoint = startIndex + totalSize / 2;
      if (filter.contains(midPoint)) {
        newRowGroups.add(rowGroup);
      }
    }

    metaData.setRow_groups(newRowGroups);
    return metaData;
  }

  private static boolean invalidFileOffset(long startIndex, long preStartIndex, long preCompressedSize) {
    boolean invalid = false;
    assert preStartIndex <= startIndex;
    // checking the first rowGroup
    if (preStartIndex == 0 && startIndex != 4) {
      invalid = true;
      return invalid;
    }

    // calculate start index for other blocks
    long minStartIndex = preStartIndex + preCompressedSize;
    if (startIndex < minStartIndex) {
      // a bad offset detected, try first column's offset
      // can not use minStartIndex in case of padding
      invalid = true;
    }

    return invalid;
  }

  // Visible for testing
  static FileMetaData filterFileMetaDataByStart(FileMetaData metaData, OffsetMetadataFilter filter) {
    List<RowGroup> rowGroups = metaData.getRow_groups();
    List<RowGroup> newRowGroups = new ArrayList<RowGroup>();
    long preStartIndex = 0;
    long preCompressedSize = 0;
    boolean firstColumnWithMetadata = true;
    if (rowGroups != null && !rowGroups.isEmpty()) {
      firstColumnWithMetadata = rowGroups.get(0).getColumns().get(0).isSetMeta_data();
    }
    for (RowGroup rowGroup : rowGroups) {
      long startIndex;
      ColumnChunk columnChunk = rowGroup.getColumns().get(0);
      if (firstColumnWithMetadata) {
        startIndex = getOffset(columnChunk);
      } else {
        assert rowGroup.isSetFile_offset();
        assert rowGroup.isSetTotal_compressed_size();

        // the file_offset of first block always holds the truth, while other blocks don't :
        // see PARQUET-2078 for details
        startIndex = rowGroup.getFile_offset();
        if (invalidFileOffset(startIndex, preStartIndex, preCompressedSize)) {
          // first row group's offset is always 4
          if (preStartIndex == 0) {
            startIndex = 4;
          } else {
            throw new InvalidFileOffsetException("corrupted RowGroup.file_offset found, "
                + "please use file range instead of block offset for split.");
          }
        }
        preStartIndex = startIndex;
        preCompressedSize = rowGroup.getTotal_compressed_size();
      }

      if (filter.contains(startIndex)) {
        newRowGroups.add(rowGroup);
      }
    }
    metaData.setRow_groups(newRowGroups);
    return metaData;
  }

  static long getOffset(RowGroup rowGroup) {
    if (rowGroup.isSetFile_offset()) {
      return rowGroup.getFile_offset();
    }
    return getOffset(rowGroup.getColumns().get(0));
  }

  // Visible for testing
  static long getOffset(ColumnChunk columnChunk) {
    ColumnMetaData md = columnChunk.getMeta_data();
    long offset = md.getData_page_offset();
    if (md.isSetDictionary_page_offset() && offset > md.getDictionary_page_offset()) {
      offset = md.getDictionary_page_offset();
    }
    return offset;
  }

  private static void verifyFooterIntegrity(
      InputStream from, InternalFileDecryptor fileDecryptor, int combinedFooterLength) throws IOException {

    byte[] nonce = new byte[AesCipher.NONCE_LENGTH];
    from.read(nonce);
    byte[] gcmTag = new byte[AesCipher.GCM_TAG_LENGTH];
    from.read(gcmTag);

    AesGcmEncryptor footerSigner = fileDecryptor.createSignedFooterEncryptor();

    int footerSignatureLength = AesCipher.NONCE_LENGTH + AesCipher.GCM_TAG_LENGTH;
    byte[] serializedFooter = new byte[combinedFooterLength - footerSignatureLength];
    // Resetting to the beginning of the footer
    from.reset();
    from.read(serializedFooter);

    byte[] signedFooterAAD = AesCipher.createFooterAAD(fileDecryptor.getFileAAD());
    byte[] encryptedFooterBytes = footerSigner.encrypt(false, serializedFooter, nonce, signedFooterAAD);
    byte[] calculatedTag = new byte[AesCipher.GCM_TAG_LENGTH];
    System.arraycopy(
        encryptedFooterBytes,
        encryptedFooterBytes.length - AesCipher.GCM_TAG_LENGTH,
        calculatedTag,
        0,
        AesCipher.GCM_TAG_LENGTH);
    if (!Arrays.equals(gcmTag, calculatedTag)) {
      throw new TagVerificationException("Signature mismatch in plaintext footer");
    }
  }

  public ParquetMetadata readParquetMetadata(final InputStream from, MetadataFilter filter) throws IOException {
    return readParquetMetadata(from, filter, null, false, 0);
  }

  private Map<RowGroup, Long> generateRowGroupOffsets(FileMetaData metaData) {
    Map<RowGroup, Long> rowGroupOrdinalToRowIdx = new HashMap<>();
    List<RowGroup> rowGroups = metaData.getRow_groups();
    if (rowGroups != null) {
      long rowIdxSum = 0;
      for (int i = 0; i < rowGroups.size(); i++) {
        rowGroupOrdinalToRowIdx.put(rowGroups.get(i), rowIdxSum);
        rowIdxSum += rowGroups.get(i).getNum_rows();
      }
    }
    return rowGroupOrdinalToRowIdx;
  }

  /**
   * A container for [[FileMetaData]] and [[RowGroup]] to ROW_INDEX offset map.
   */
  private class FileMetaDataAndRowGroupOffsetInfo {
    final FileMetaData fileMetadata;
    final Map<RowGroup, Long> rowGroupToRowIndexOffsetMap;

    public FileMetaDataAndRowGroupOffsetInfo(
        FileMetaData fileMetadata, Map<RowGroup, Long> rowGroupToRowIndexOffsetMap) {
      this.fileMetadata = fileMetadata;
      this.rowGroupToRowIndexOffsetMap = rowGroupToRowIndexOffsetMap;
    }
  }

  public ParquetMetadata readParquetMetadata(
      final InputStream fromInputStream,
      MetadataFilter filter,
      final InternalFileDecryptor fileDecryptor,
      final boolean encryptedFooter,
      final int combinedFooterLength)
      throws IOException {

    final BlockCipher.Decryptor footerDecryptor = (encryptedFooter ? fileDecryptor.fetchFooterDecryptor() : null);
    final byte[] encryptedFooterAAD =
        (encryptedFooter ? AesCipher.createFooterAAD(fileDecryptor.getFileAAD()) : null);

    // Mark the beginning of the footer for verifyFooterIntegrity
    final InputStream from;
    if (fileDecryptor != null && fileDecryptor.checkFooterIntegrity()) {
      // fromInputStream should already support marking but let's be on the safe side
      if (!fromInputStream.markSupported()) {
        from = new BufferedInputStream(fromInputStream, combinedFooterLength);
      } else {
        from = fromInputStream;
      }
      from.mark(combinedFooterLength);
    } else {
      from = fromInputStream;
    }

    FileMetaDataAndRowGroupOffsetInfo fileMetaDataAndRowGroupInfo =
        filter.accept(new MetadataFilterVisitor<FileMetaDataAndRowGroupOffsetInfo, IOException>() {
          @Override
          public FileMetaDataAndRowGroupOffsetInfo visit(NoFilter filter) throws IOException {
            FileMetaData fileMetadata = readFileMetaData(from, footerDecryptor, encryptedFooterAAD);
            return new FileMetaDataAndRowGroupOffsetInfo(
                fileMetadata, generateRowGroupOffsets(fileMetadata));
          }

          @Override
          public FileMetaDataAndRowGroupOffsetInfo visit(SkipMetadataFilter filter) throws IOException {
            FileMetaData fileMetadata = readFileMetaData(from, true, footerDecryptor, encryptedFooterAAD);
            return new FileMetaDataAndRowGroupOffsetInfo(
                fileMetadata, generateRowGroupOffsets(fileMetadata));
          }

          @Override
          public FileMetaDataAndRowGroupOffsetInfo visit(OffsetMetadataFilter filter) throws IOException {
            FileMetaData fileMetadata = readFileMetaData(from, footerDecryptor, encryptedFooterAAD);
            // We must generate the map *before* filtering because it modifies `fileMetadata`.
            Map<RowGroup, Long> rowGroupToRowIndexOffsetMap = generateRowGroupOffsets(fileMetadata);
            FileMetaData filteredFileMetadata = filterFileMetaDataByStart(fileMetadata, filter);
            return new FileMetaDataAndRowGroupOffsetInfo(filteredFileMetadata, rowGroupToRowIndexOffsetMap);
          }

          @Override
          public FileMetaDataAndRowGroupOffsetInfo visit(RangeMetadataFilter filter) throws IOException {
            FileMetaData fileMetadata = readFileMetaData(from, footerDecryptor, encryptedFooterAAD);
            // We must generate the map *before* filtering because it modifies `fileMetadata`.
            Map<RowGroup, Long> rowGroupToRowIndexOffsetMap = generateRowGroupOffsets(fileMetadata);
            FileMetaData filteredFileMetadata = filterFileMetaDataByMidpoint(fileMetadata, filter);
            return new FileMetaDataAndRowGroupOffsetInfo(filteredFileMetadata, rowGroupToRowIndexOffsetMap);
          }
        });
    FileMetaData fileMetaData = fileMetaDataAndRowGroupInfo.fileMetadata;
    Map<RowGroup, Long> rowGroupToRowIndexOffsetMap = fileMetaDataAndRowGroupInfo.rowGroupToRowIndexOffsetMap;
    LOG.debug("{}", fileMetaData);

    if (!encryptedFooter && null != fileDecryptor) {
      if (!fileMetaData.isSetEncryption_algorithm()) { // Plaintext file
        fileDecryptor.setPlaintextFile();
        // Done to detect files that were not encrypted by mistake
        if (!fileDecryptor.plaintextFilesAllowed()) {
          throw new ParquetCryptoRuntimeException("Applying decryptor on plaintext file");
        }
      } else { // Encrypted file with plaintext footer
        // if no fileDecryptor, can still read plaintext columns
        fileDecryptor.setFileCryptoMetaData(
            fileMetaData.getEncryption_algorithm(), false, fileMetaData.getFooter_signing_key_metadata());
        if (fileDecryptor.checkFooterIntegrity()) {
          verifyFooterIntegrity(from, fileDecryptor, combinedFooterLength);
        }
      }
    }

    ParquetMetadata parquetMetadata =
        fromParquetMetadata(fileMetaData, fileDecryptor, encryptedFooter, rowGroupToRowIndexOffsetMap);
    if (LOG.isDebugEnabled()) LOG.debug(ParquetMetadata.toPrettyJSON(parquetMetadata));
    return parquetMetadata;
  }

  public ColumnChunkMetaData buildColumnChunkMetaData(
      ColumnMetaData metaData, ColumnPath columnPath, PrimitiveType type, String createdBy) {
    return ColumnChunkMetaData.get(
        columnPath,
        type,
        fromFormatCodec(metaData.codec),
        convertEncodingStats(metaData.getEncoding_stats()),
        fromFormatEncodings(metaData.encodings),
        fromParquetStatistics(createdBy, metaData.statistics, type),
        metaData.data_page_offset,
        metaData.dictionary_page_offset,
        metaData.num_values,
        metaData.total_compressed_size,
        metaData.total_uncompressed_size,
        fromParquetSizeStatistics(metaData.size_statistics, type));
  }

  public ParquetMetadata fromParquetMetadata(FileMetaData parquetMetadata) throws IOException {
    return fromParquetMetadata(parquetMetadata, null, false);
  }

  public ParquetMetadata fromParquetMetadata(
      FileMetaData parquetMetadata, InternalFileDecryptor fileDecryptor, boolean encryptedFooter)
      throws IOException {
    return fromParquetMetadata(parquetMetadata, fileDecryptor, encryptedFooter, new HashMap<RowGroup, Long>());
  }

  public ParquetMetadata fromParquetMetadata(
      FileMetaData parquetMetadata,
      InternalFileDecryptor fileDecryptor,
      boolean encryptedFooter,
      Map<RowGroup, Long> rowGroupToRowIndexOffsetMap)
      throws IOException {
    MessageType messageType = fromParquetSchema(parquetMetadata.getSchema(), parquetMetadata.getColumn_orders());
    List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();
    List<RowGroup> row_groups = parquetMetadata.getRow_groups();

    if (row_groups != null) {
      for (RowGroup rowGroup : row_groups) {
        BlockMetaData blockMetaData = new BlockMetaData();
        blockMetaData.setRowCount(rowGroup.getNum_rows());
        blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
        if (rowGroupToRowIndexOffsetMap.containsKey(rowGroup)) {
          blockMetaData.setRowIndexOffset(rowGroupToRowIndexOffsetMap.get(rowGroup));
        }
        // not set in legacy files
        if (rowGroup.isSetOrdinal()) {
          blockMetaData.setOrdinal(rowGroup.getOrdinal());
        }
        List<ColumnChunk> columns = rowGroup.getColumns();
        String filePath = columns.get(0).getFile_path();
        int columnOrdinal = -1;
        for (ColumnChunk columnChunk : columns) {
          columnOrdinal++;
          if ((filePath == null && columnChunk.getFile_path() != null)
              || (filePath != null && !filePath.equals(columnChunk.getFile_path()))) {
            throw new ParquetDecodingException(
                "all column chunks of the same row group must be in the same file for now");
          }
          ColumnMetaData metaData = columnChunk.meta_data;
          ColumnCryptoMetaData cryptoMetaData = columnChunk.getCrypto_metadata();
          ColumnChunkMetaData column = null;
          ColumnPath columnPath = null;
          boolean lazyMetadataDecryption = false;

          if (null == cryptoMetaData) { // Plaintext column
            columnPath = getPath(metaData);
            if (null != fileDecryptor && !fileDecryptor.plaintextFile()) {
              // mark this column as plaintext in encrypted file decryptor
              fileDecryptor.setColumnCryptoMetadata(
                  columnPath, false, false, (byte[]) null, columnOrdinal);
            }
          } else { // Encrypted column
            boolean encryptedWithFooterKey = cryptoMetaData.isSetENCRYPTION_WITH_FOOTER_KEY();
            if (encryptedWithFooterKey) { // Column encrypted with footer key
              if (null == fileDecryptor) {
                throw new ParquetCryptoRuntimeException(
                    "Column encrypted with footer key: No keys available");
              }
              if (null == metaData) {
                throw new ParquetCryptoRuntimeException(
                    "ColumnMetaData not set in Encryption with Footer key");
              }
              columnPath = getPath(metaData);
              if (!encryptedFooter) { // Unencrypted footer. Decrypt full column metadata, using footer
                // key
                ByteArrayInputStream tempInputStream =
                    new ByteArrayInputStream(columnChunk.getEncrypted_column_metadata());
                byte[] columnMetaDataAAD = AesCipher.createModuleAAD(
                    fileDecryptor.getFileAAD(),
                    ModuleType.ColumnMetaData,
                    rowGroup.getOrdinal(),
                    columnOrdinal,
                    -1);
                try {
                  metaData = readColumnMetaData(
                      tempInputStream, fileDecryptor.fetchFooterDecryptor(), columnMetaDataAAD);
                } catch (IOException e) {
                  throw new ParquetCryptoRuntimeException(
                      columnPath + ". Failed to decrypt column metadata", e);
                }
              }
              fileDecryptor.setColumnCryptoMetadata(columnPath, true, true, (byte[]) null, columnOrdinal);
            } else { // Column encrypted with column key
              // setColumnCryptoMetadata triggers KMS interaction, hence delayed until this column is
              // projected
              lazyMetadataDecryption = true;
            }
          }

          String createdBy = parquetMetadata.getCreated_by();
          if (!lazyMetadataDecryption) { // full column metadata (with stats) is available
            column = buildColumnChunkMetaData(
                metaData,
                columnPath,
                messageType.getType(columnPath.toArray()).asPrimitiveType(),
                createdBy);
            column.setRowGroupOrdinal(rowGroup.getOrdinal());
            if (metaData.isSetBloom_filter_offset()) {
              column.setBloomFilterOffset(metaData.getBloom_filter_offset());
            }
            if (metaData.isSetBloom_filter_length()) {
              column.setBloomFilterLength(metaData.getBloom_filter_length());
            }
          } else { // column encrypted with column key
            // Metadata will be decrypted later, if this column is accessed
            EncryptionWithColumnKey columnKeyStruct = cryptoMetaData.getENCRYPTION_WITH_COLUMN_KEY();
            List<String> pathList = columnKeyStruct.getPath_in_schema();
            byte[] columnKeyMetadata = columnKeyStruct.getKey_metadata();
            columnPath = ColumnPath.get(pathList.toArray(new String[pathList.size()]));
            byte[] encryptedMetadataBuffer = columnChunk.getEncrypted_column_metadata();
            column = ColumnChunkMetaData.getWithEncryptedMetadata(
                this,
                columnPath,
                messageType.getType(columnPath.toArray()).asPrimitiveType(),
                encryptedMetadataBuffer,
                columnKeyMetadata,
                fileDecryptor,
                rowGroup.getOrdinal(),
                columnOrdinal,
                createdBy);
          }

          column.setColumnIndexReference(toColumnIndexReference(columnChunk));
          column.setOffsetIndexReference(toOffsetIndexReference(columnChunk));

          // TODO
          // index_page_offset
          // key_value_metadata
          blockMetaData.addColumn(column);
        }
        blockMetaData.setPath(filePath);
        blocks.add(blockMetaData);
      }
    }
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    List<KeyValue> key_value_metadata = parquetMetadata.getKey_value_metadata();
    if (key_value_metadata != null) {
      for (KeyValue keyValue : key_value_metadata) {
        keyValueMetaData.put(keyValue.key, keyValue.value);
      }
    }
    EncryptionType encryptionType;
    if (encryptedFooter) {
      encryptionType = EncryptionType.ENCRYPTED_FOOTER;
    } else if (parquetMetadata.isSetEncryption_algorithm()) {
      encryptionType = EncryptionType.PLAINTEXT_FOOTER;
    } else {
      encryptionType = EncryptionType.UNENCRYPTED;
    }
    return new ParquetMetadata(
        new org.apache.parquet.hadoop.metadata.FileMetaData(
            messageType, keyValueMetaData, parquetMetadata.getCreated_by(), encryptionType, fileDecryptor),
        blocks);
  }

  private static IndexReference toColumnIndexReference(ColumnChunk columnChunk) {
    if (columnChunk.isSetColumn_index_offset() && columnChunk.isSetColumn_index_length()) {
      return new IndexReference(columnChunk.getColumn_index_offset(), columnChunk.getColumn_index_length());
    }
    return null;
  }

  private static IndexReference toOffsetIndexReference(ColumnChunk columnChunk) {
    if (columnChunk.isSetOffset_index_offset() && columnChunk.isSetOffset_index_length()) {
      return new IndexReference(columnChunk.getOffset_index_offset(), columnChunk.getOffset_index_length());
    }
    return null;
  }

  private static ColumnPath getPath(ColumnMetaData metaData) {
    String[] path = metaData.path_in_schema.toArray(new String[0]);
    return ColumnPath.get(path);
  }

  // Visible for testing
  MessageType fromParquetSchema(List<SchemaElement> schema, List<ColumnOrder> columnOrders) {
    Iterator<SchemaElement> iterator = schema.iterator();
    SchemaElement root = iterator.next();
    Types.MessageTypeBuilder builder = Types.buildMessage();
    if (root.isSetField_id()) {
      builder.id(root.field_id);
    }
    buildChildren(builder, iterator, root.getNum_children(), columnOrders, 0);
    return builder.named(root.name);
  }

  private void buildChildren(
      Types.GroupBuilder builder,
      Iterator<SchemaElement> schema,
      int childrenCount,
      List<ColumnOrder> columnOrders,
      int columnCount) {
    for (int i = 0; i < childrenCount; i++) {
      SchemaElement schemaElement = schema.next();

      // Create Parquet Type.
      Types.Builder childBuilder;
      if (schemaElement.type != null) {
        Types.PrimitiveBuilder primitiveBuilder = builder.primitive(
            getPrimitive(schemaElement.type), fromParquetRepetition(schemaElement.repetition_type));
        if (schemaElement.isSetType_length()) {
          primitiveBuilder.length(schemaElement.type_length);
        }
        if (schemaElement.isSetPrecision()) {
          primitiveBuilder.precision(schemaElement.precision);
        }
        if (schemaElement.isSetScale()) {
          primitiveBuilder.scale(schemaElement.scale);
        }
        if (columnOrders != null) {
          org.apache.parquet.schema.ColumnOrder columnOrder =
              fromParquetColumnOrder(columnOrders.get(columnCount));
          // As per parquet format 2.4.0 no UNDEFINED order is supported. So, set undefined column order for
          // the types
          // where ordering is not supported.
          if (columnOrder.getColumnOrderName() == ColumnOrderName.TYPE_DEFINED_ORDER
              && (schemaElement.type == Type.INT96
                  || schemaElement.converted_type == ConvertedType.INTERVAL)) {
            columnOrder = org.apache.parquet.schema.ColumnOrder.undefined();
          }
          primitiveBuilder.columnOrder(columnOrder);
        }
        childBuilder = primitiveBuilder;
      } else {
        childBuilder = builder.group(fromParquetRepetition(schemaElement.repetition_type));
        buildChildren(
            (Types.GroupBuilder) childBuilder,
            schema,
            schemaElement.num_children,
            columnOrders,
            columnCount);
      }

      if (schemaElement.isSetLogicalType()) {
        childBuilder.as(getLogicalTypeAnnotation(schemaElement.logicalType));
      }
      if (schemaElement.isSetConverted_type()) {
        OriginalType originalType = getLogicalTypeAnnotation(schemaElement.converted_type, schemaElement)
            .toOriginalType();
        OriginalType newOriginalType = (schemaElement.isSetLogicalType()
                && getLogicalTypeAnnotation(schemaElement.logicalType) != null)
            ? getLogicalTypeAnnotation(schemaElement.logicalType).toOriginalType()
            : null;
        if (!originalType.equals(newOriginalType)) {
          if (newOriginalType != null) {
            LOG.warn(
                "Converted type and logical type metadata mismatch (convertedType: {}, logical type: {}). Using value in converted type.",
                schemaElement.converted_type,
                schemaElement.logicalType);
          }
          childBuilder.as(originalType);
        }
      }
      if (schemaElement.isSetField_id()) {
        childBuilder.id(schemaElement.field_id);
      }

      childBuilder.named(schemaElement.name);
      ++columnCount;
    }
  }

  // Visible for testing
  FieldRepetitionType toParquetRepetition(Repetition repetition) {
    return FieldRepetitionType.valueOf(repetition.name());
  }

  // Visible for testing
  Repetition fromParquetRepetition(FieldRepetitionType repetition) {
    return Repetition.valueOf(repetition.name());
  }

  private static org.apache.parquet.schema.ColumnOrder fromParquetColumnOrder(ColumnOrder columnOrder) {
    if (columnOrder.isSetTYPE_ORDER()) {
      return org.apache.parquet.schema.ColumnOrder.typeDefined();
    }
    // The column order is not yet supported by this API
    return org.apache.parquet.schema.ColumnOrder.undefined();
  }

  @Deprecated
  public void writeDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to)
      throws IOException {
    writePageHeader(
        newDataPageHeader(uncompressedSize, compressedSize, valueCount, rlEncoding, dlEncoding, valuesEncoding),
        to);
  }

  // Statistics are no longer saved in page headers
  @Deprecated
  public void writeDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.statistics.Statistics statistics,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to)
      throws IOException {
    writePageHeader(
        newDataPageHeader(uncompressedSize, compressedSize, valueCount, rlEncoding, dlEncoding, valuesEncoding),
        to);
  }

  private PageHeader newDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    pageHeader.setData_page_header(new DataPageHeader(
        valueCount, getEncoding(valuesEncoding), getEncoding(dlEncoding), getEncoding(rlEncoding)));
    return pageHeader;
  }

  private PageHeader newDataPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc) {
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);
    pageHeader.setCrc(crc);
    pageHeader.setData_page_header(new DataPageHeader(
        valueCount, getEncoding(valuesEncoding), getEncoding(dlEncoding), getEncoding(rlEncoding)));
    return pageHeader;
  }

  // Statistics are no longer saved in page headers
  @Deprecated
  public void writeDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.statistics.Statistics statistics,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength,
      OutputStream to)
      throws IOException {
    writePageHeader(
        newDataPageV2Header(
            uncompressedSize,
            compressedSize,
            valueCount,
            nullCount,
            rowCount,
            dataEncoding,
            rlByteLength,
            dlByteLength),
        to);
  }

  public void writeDataPageV1Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to)
      throws IOException {
    writeDataPageV1Header(
        uncompressedSize, compressedSize, valueCount, rlEncoding, dlEncoding, valuesEncoding, to, null, null);
  }

  public void writeDataPageV1Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    writePageHeader(
        newDataPageHeader(uncompressedSize, compressedSize, valueCount, rlEncoding, dlEncoding, valuesEncoding),
        to,
        blockEncryptor,
        pageHeaderAAD);
  }

  public void writeDataPageV1Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc,
      OutputStream to)
      throws IOException {
    writeDataPageV1Header(
        uncompressedSize,
        compressedSize,
        valueCount,
        rlEncoding,
        dlEncoding,
        valuesEncoding,
        crc,
        to,
        null,
        null);
  }

  public void writeDataPageV1Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding rlEncoding,
      org.apache.parquet.column.Encoding dlEncoding,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    writePageHeader(
        newDataPageHeader(
            uncompressedSize, compressedSize, valueCount, rlEncoding, dlEncoding, valuesEncoding, crc),
        to,
        blockEncryptor,
        pageHeaderAAD);
  }

  public void writeDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength,
      OutputStream to)
      throws IOException {
    writeDataPageV2Header(
        uncompressedSize,
        compressedSize,
        valueCount,
        nullCount,
        rowCount,
        dataEncoding,
        rlByteLength,
        dlByteLength,
        to,
        null,
        null);
  }

  public void writeDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    writePageHeader(
        newDataPageV2Header(
            uncompressedSize,
            compressedSize,
            valueCount,
            nullCount,
            rowCount,
            dataEncoding,
            rlByteLength,
            dlByteLength),
        to,
        blockEncryptor,
        pageHeaderAAD);
  }

  private PageHeader newDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength) {
    DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
        valueCount, nullCount, rowCount, getEncoding(dataEncoding), dlByteLength, rlByteLength);
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE_V2, uncompressedSize, compressedSize);
    pageHeader.setData_page_header_v2(dataPageHeaderV2);
    return pageHeader;
  }

  public void writeDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength,
      int crc,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    writePageHeader(
        newDataPageV2Header(
            uncompressedSize,
            compressedSize,
            valueCount,
            nullCount,
            rowCount,
            dataEncoding,
            rlByteLength,
            dlByteLength,
            crc),
        to,
        blockEncryptor,
        pageHeaderAAD);
  }

  private PageHeader newDataPageV2Header(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      int nullCount,
      int rowCount,
      org.apache.parquet.column.Encoding dataEncoding,
      int rlByteLength,
      int dlByteLength,
      int crc) {
    DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
        valueCount, nullCount, rowCount, getEncoding(dataEncoding), dlByteLength, rlByteLength);
    PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE_V2, uncompressedSize, compressedSize);
    pageHeader.setData_page_header_v2(dataPageHeaderV2);
    pageHeader.setCrc(crc);
    return pageHeader;
  }

  public void writeDictionaryPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to)
      throws IOException {
    writeDictionaryPageHeader(uncompressedSize, compressedSize, valueCount, valuesEncoding, to, null, null);
  }

  public void writeDictionaryPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
    pageHeader.setDictionary_page_header(new DictionaryPageHeader(valueCount, getEncoding(valuesEncoding)));
    writePageHeader(pageHeader, to, blockEncryptor, pageHeaderAAD);
  }

  public void writeDictionaryPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc,
      OutputStream to)
      throws IOException {
    writeDictionaryPageHeader(uncompressedSize, compressedSize, valueCount, valuesEncoding, crc, to, null, null);
  }

  public void writeDictionaryPageHeader(
      int uncompressedSize,
      int compressedSize,
      int valueCount,
      org.apache.parquet.column.Encoding valuesEncoding,
      int crc,
      OutputStream to,
      BlockCipher.Encryptor blockEncryptor,
      byte[] pageHeaderAAD)
      throws IOException {
    PageHeader pageHeader = new PageHeader(PageType.DICTIONARY_PAGE, uncompressedSize, compressedSize);
    pageHeader.setCrc(crc);
    pageHeader.setDictionary_page_header(new DictionaryPageHeader(valueCount, getEncoding(valuesEncoding)));
    writePageHeader(pageHeader, to, blockEncryptor, pageHeaderAAD);
  }

  private static BoundaryOrder toParquetBoundaryOrder(
      org.apache.parquet.internal.column.columnindex.BoundaryOrder boundaryOrder) {
    switch (boundaryOrder) {
      case ASCENDING:
        return BoundaryOrder.ASCENDING;
      case DESCENDING:
        return BoundaryOrder.DESCENDING;
      case UNORDERED:
        return BoundaryOrder.UNORDERED;
      default:
        throw new IllegalArgumentException("Unsupported boundary order: " + boundaryOrder);
    }
  }

  private static org.apache.parquet.internal.column.columnindex.BoundaryOrder fromParquetBoundaryOrder(
      BoundaryOrder boundaryOrder) {
    switch (boundaryOrder) {
      case ASCENDING:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
      case DESCENDING:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
      case UNORDERED:
        return org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
      default:
        throw new IllegalArgumentException("Unsupported boundary order: " + boundaryOrder);
    }
  }

  public static ColumnIndex toParquetColumnIndex(
      PrimitiveType type, org.apache.parquet.internal.column.columnindex.ColumnIndex columnIndex) {
    if (!isMinMaxStatsSupported(type) || columnIndex == null) {
      return null;
    }
    ColumnIndex parquetColumnIndex = new ColumnIndex(
        columnIndex.getNullPages(),
        columnIndex.getMinValues(),
        columnIndex.getMaxValues(),
        toParquetBoundaryOrder(columnIndex.getBoundaryOrder()));
    parquetColumnIndex.setNull_counts(columnIndex.getNullCounts());
    List<Long> repLevelHistogram = columnIndex.getRepetitionLevelHistogram();
    if (repLevelHistogram != null && !repLevelHistogram.isEmpty()) {
      parquetColumnIndex.setRepetition_level_histograms(repLevelHistogram);
    }
    List<Long> defLevelHistogram = columnIndex.getDefinitionLevelHistogram();
    if (defLevelHistogram != null && !defLevelHistogram.isEmpty()) {
      parquetColumnIndex.setDefinition_level_histograms(defLevelHistogram);
    }
    return parquetColumnIndex;
  }

  public static org.apache.parquet.internal.column.columnindex.ColumnIndex fromParquetColumnIndex(
      PrimitiveType type, ColumnIndex parquetColumnIndex) {
    if (!isMinMaxStatsSupported(type)) {
      return null;
    }
    return ColumnIndexBuilder.build(
        type,
        fromParquetBoundaryOrder(parquetColumnIndex.getBoundary_order()),
        parquetColumnIndex.getNull_pages(),
        parquetColumnIndex.getNull_counts(),
        parquetColumnIndex.getMin_values(),
        parquetColumnIndex.getMax_values(),
        parquetColumnIndex.getRepetition_level_histograms(),
        parquetColumnIndex.getDefinition_level_histograms());
  }

  public static OffsetIndex toParquetOffsetIndex(
      org.apache.parquet.internal.column.columnindex.OffsetIndex offsetIndex) {
    List<PageLocation> pageLocations = new ArrayList<>(offsetIndex.getPageCount());
    List<Long> unencodedByteArrayDataBytes = new ArrayList<>(offsetIndex.getPageCount());
    for (int i = 0, n = offsetIndex.getPageCount(); i < n; ++i) {
      pageLocations.add(new PageLocation(
          offsetIndex.getOffset(i), offsetIndex.getCompressedPageSize(i), offsetIndex.getFirstRowIndex(i)));
      Optional<Long> unencodedByteArrayDataType = offsetIndex.getUnencodedByteArrayDataBytes(i);
      if (unencodedByteArrayDataType.isPresent() && unencodedByteArrayDataBytes.size() == i) {
        unencodedByteArrayDataBytes.add(unencodedByteArrayDataType.get());
      }
    }
    OffsetIndex parquetOffsetIndex = new OffsetIndex(pageLocations);
    if (unencodedByteArrayDataBytes.size() == pageLocations.size()) {
      // Do not add the field if we are missing that from any page.
      parquetOffsetIndex.setUnencoded_byte_array_data_bytes(unencodedByteArrayDataBytes);
    }
    return parquetOffsetIndex;
  }

  public static org.apache.parquet.internal.column.columnindex.OffsetIndex fromParquetOffsetIndex(
      OffsetIndex parquetOffsetIndex) {
    boolean hasUnencodedByteArrayDataBytes = parquetOffsetIndex.isSetUnencoded_byte_array_data_bytes()
        && parquetOffsetIndex.unencoded_byte_array_data_bytes.size()
            == parquetOffsetIndex.page_locations.size();
    OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
    for (int i = 0; i < parquetOffsetIndex.page_locations.size(); ++i) {
      PageLocation pageLocation = parquetOffsetIndex.page_locations.get(i);
      Optional<Long> unencodedByteArrayDataBytes = hasUnencodedByteArrayDataBytes
          ? Optional.of(parquetOffsetIndex.unencoded_byte_array_data_bytes.get(i))
          : Optional.empty();
      builder.add(
          pageLocation.getOffset(),
          pageLocation.getCompressed_page_size(),
          pageLocation.getFirst_row_index(),
          unencodedByteArrayDataBytes);
    }
    return builder.build();
  }

  public static BloomFilterHeader toBloomFilterHeader(
      org.apache.parquet.column.values.bloomfilter.BloomFilter bloomFilter) {

    BloomFilterAlgorithm algorithm = null;
    BloomFilterHash hashStrategy = null;
    BloomFilterCompression compression = null;

    if (bloomFilter.getAlgorithm() == BloomFilter.Algorithm.BLOCK) {
      algorithm = BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm());
    }

    if (bloomFilter.getHashStrategy() == BloomFilter.HashStrategy.XXH64) {
      hashStrategy = BloomFilterHash.XXHASH(new XxHash());
    }

    if (bloomFilter.getCompression() == BloomFilter.Compression.UNCOMPRESSED) {
      compression = BloomFilterCompression.UNCOMPRESSED(new Uncompressed());
    }

    if (algorithm != null && hashStrategy != null && compression != null) {
      return new BloomFilterHeader(bloomFilter.getBitsetSize(), algorithm, hashStrategy, compression);
    } else {
      throw new IllegalArgumentException(String.format(
          "Failed to build thrift structure for BloomFilterHeader," + "algorithm=%s, hash=%s, compression=%s",
          bloomFilter.getAlgorithm(), bloomFilter.getHashStrategy(), bloomFilter.getCompression()));
    }
  }

  public static org.apache.parquet.column.statistics.SizeStatistics fromParquetSizeStatistics(
      SizeStatistics statistics, PrimitiveType type) {
    if (statistics == null) {
      return null;
    }
    return new org.apache.parquet.column.statistics.SizeStatistics(
        type,
        statistics.getUnencoded_byte_array_data_bytes(),
        statistics.getRepetition_level_histogram(),
        statistics.getDefinition_level_histogram());
  }

  public static SizeStatistics toParquetSizeStatistics(org.apache.parquet.column.statistics.SizeStatistics stats) {
    if (stats == null) {
      return null;
    }
    SizeStatistics formatStats = new SizeStatistics();
    if (stats.getUnencodedByteArrayDataBytes().isPresent()) {
      formatStats.setUnencoded_byte_array_data_bytes(
          stats.getUnencodedByteArrayDataBytes().get());
    }
    formatStats.setRepetition_level_histogram(stats.getRepetitionLevelHistogram());
    formatStats.setDefinition_level_histogram(stats.getDefinitionLevelHistogram());
    return formatStats;
  }
}
