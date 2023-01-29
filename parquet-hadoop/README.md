<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Hadoop integration

**Todo:** Add a description

## Properties

It's possible to configure the [ParquetInputFormat](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetInputFormat.java) / [ParquetOutputFormat](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetOutputFormat.java) with Hadoop config or programmatically with setters.


**Example:**
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

Configuration conf = new Configuration();
conf.set("parquet.page.size","128");

Job writeJob = new Job(conf);
ParquetOutputFormat.setBlockSize(writeJob, 1024);

```

## Class: ParquetOutputFormat

**Property:** `parquet.summary.metadata.level`  
**Description:** Write summary files in the same directory as parquet files.  
If this property is set to `all`, write both summary file with row group info to _metadata and summary file without row group info to _common_metadata.  
If it is `common_only`, write only the summary file without the row group info to _common_metadata.  
If it is `none`, don't write summary files.  
**Default value:** `all`

---

**Property:** `parquet.enable.summary-metadata`  
**Description:** This property is deprecated, use `parquet.summary.metadata.level` instead.  
If it is `true`, it similar to `parquet.summary.metadata.level` with `all`. If it is `false`, it is similar to `NONE`.   
**Default value:** `true`  

---

**Property:** `parquet.block.size`  
**Description:** The block size in bytes. This property depends on the file system:

- If the file system (FS) used supports blocks like HDFS, the block size will be the maximum between the default block size of FS and this property. And the row group size will be equal to this property.  
  - block_size = max(default_fs_block_size, parquet.block.size)  
  - row_group_size = parquet.block.size`

- If the file system used doesn't support blocks, then this property will define the row group size.

Note that larger values of row group size will improve the IO when reading but consume more memory when writing  
**Default value:** `134217728` (128 MB)  

---

**Property:** `parquet.page.size`  
**Description:** The page size in bytes is for compression. When reading, each page can be decompressed independently.  
A block is composed of pages. The page is the smallest unit that must be read fully to access a single record.
If this value is too small, the compression will deteriorate.  
**Default value:** `1048576` (1 MB)

---

**Property:** `parquet.compression`  
**Description:** The compression algorithm used to compress pages. This property supersedes `mapred.output.compress*`.  
It can be `uncompressed`, `snappy`, `gzip` or `lzo`, `brotli`,`lz4` and `zstd`.  
If `parquet.compression` is not set, the following properties are checked:
 * mapred.output.compress=true
 * mapred.output.compression.codec=org.apache.hadoop.io.compress.SomeCodec

Note that custom codecs are explicitly disallowed. Only one of Snappy, GZip, LZO, LZ4, Brotli or ZSTD is accepted.  
**Default value:** `uncompressed`

---

**Property:** `parquet.write.support.class`  
**Description:** The write support class to convert the records written to the OutputFormat into the events accepted by the record consumer.  
Usually provided by a specific ParquetOutputFormat subclass and it should be the descendant class of `org.apache.parquet.hadoop.api.WriteSupport`

---

**Property:** `parquet.enable.dictionary`  
**Description:** Whether to enable or disable dictionary encoding. If it is true, then the dictionary encoding is enabled for all columns.  
If it is false, then the dictionary encoding is disabled for all columns.  
It is possible to enable or disable the encoding for some columns by specifying the column name in the property.  
Note that all configurations of this property will be combined (See the following example).  
**Default value:** `true`  
**Example:**
```java
// Enable dictionary encoding for all columns
conf.set("parquet.enable.dictionary", true);
// Disable dictionary encoding for 'column.path'
conf.set("parquet.enable.dictionary#column.path", false);
// The final configuration will be: Enable dictionary encoding for all columns except 'column.path'
```

---

**Property:** `parquet.dictionary.page.size`  
**Description:** The dictionary page size works like the page size but for dictionary.  
There is one dictionary page per column per row group when dictionary encoding is used.  
**Default value:** `1048576` (1 MB)

---

**Property:** `parquet.validation`  
**Description:** Whether to turn on validation using the schema.  
**Default value:** `false`

---

**Property:** `parquet.writer.version`  
**Description:** The writer version. It can be either `PARQUET_1_0` or `PARQUET_2_0`.  
`PARQUET_1_0` and `PARQUET_2_0` refer to DataPageHeaderV1 and DataPageHeaderV2.  
The v1 pages store levels uncompressed while v1 pages compress levels with the data.  
For more details, see the the [thrift definition](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).  
**Default value:** `PARQUET_1_0`  

---

**Property:** `parquet.memory.pool.ratio`  
**Description:** The memory manager balances the allocation size of each Parquet writer by resize them averagely.  
If the sum of each writer's allocation size is less than the total memory pool, the memory manager keeps their original value.  
If the sum exceeds, it decreases the allocation size of each writer by this ratio.  
This property should be between 0 and 1.  
**Default value:** `0.95`

---

**Property:** `parquet.memory.min.chunk.size`  
**Description:** The minimum allocation size in byte per Parquet writer. If the allocation size is less than the minimum, the memory manager will fail with an exception.  
**Default value:** `1048576` (1 MB)

---

**Property:** `parquet.writer.max-padding`  
**Description:** The maximum size in bytes allowed as padding to align row groups. This is also the minimum size of a row group.  
**Default value:** `8388608` (8 MB)

---

**Property:** `parquet.page.size.row.check.min`  
**Description:** The frequency of checks of the page size limit will be between
`parquet.page.size.row.check.min` and `parquet.page.size.row.check.max`.  
If the frequency is high, the page size will be accurate.  
If the frequency is low, the performance will be better.  
**Default value:** `100`

---

**Property:** `parquet.page.size.row.check.max`  
**Description:** The frequency of checks of the page size limit will be between
`parquet.page.size.row.check.min` and `parquet.page.size.row.check.max`.  
If the frequency is high, the page size will be accurate.  
If the frequency is low, the performance will be better.  
**Default value:** `10000`

---

**Property:** `parquet.page.size.check.estimate`  
**Description:** If it is true, the column writer estimates the size of the next page.  
It prevents issues with rows that vary significantly in size.  
**Default value:** `true`

---

**Property:** `parquet.columnindex.truncate.length`  
**Description:** The [column index](https://github.com/apache/parquet-format/blob/master/PageIndex.md) containing min/max and null count values for the pages in a column chunk.  
This property is the length to be used for truncating binary values if possible in a binary column index.  
**Default value:** `64`  

---

**Property:** `parquet.statistics.truncate.length`  
**Description:** The length which the min/max binary values in row groups tried to be truncated to.  
**Default value:** `2147483647`

---

**Property:** `parquet.bloom.filter.enabled`  
**Description:** Whether to enable writing bloom filter.  
If it is true, the bloom filter will be enable for all columns. If it is false, it will be disabled for all columns.  
It is also possible to enable it for some columns by specifying the column name within the property followed by #.  
**Default value:** `false`  
**Example:**
```java
// Enable the bloom filter for all columns
conf.set("parquet.bloom.filter.enabled", true);
// Disable the bloom filter for the column 'column.path'
conf.set("parquet.bloom.filter.enabled#column.path", false);
// The bloom filter will be enabled for all columns except 'column.path'
```

---

**Property:** `parquet.bloom.filter.expected.ndv`  
**Description:** The expected number of distinct values in a column, it is used to compute the optimal size of the bloom filter.  
Note that if this property is not set, the bloom filter will use the maximum size.  
If this property is set for a column, then no need to enable the bloom filter with `parquet.bloom.filter.enabled` property.  
**Example:**
```java
// The bloom filter will be enabled for 'column.path' with expected number of distinct values equals to 200
conf.set("parquet.bloom.filter.expected.ndv#column.path", 200)
```

---

**Property:** `parquet.bloom.filter.fpp`  
**Description:** The maximum expected false positive probability for the bloom filter in a column. Combined with `ndv`, `fpp` is
used to compute the optimal size of the bloom filter.
Note that if this property is not set, the default value 0.01 is used.  
**Example:**
```java
// The bloom filter will be enabled for 'column.path' with expected number of distinct values equals to 200 and maximum expected false positive probability sets to 0.02
conf.set("parquet.bloom.filter.expected.ndv#column.path", 200)
conf.set("parquet.bloom.filter.fpp#column.path", 0.02)
```

---

**Property:** `parquet.bloom.filter.max.bytes`  
**Description:** The maximum number of bytes for a bloom filter bitset.  
**Default value:** `1048576` (1MB)

---

**Property:** `parquet.page.row.count.limit`  
**Description:** The maximum number of rows per page.  
**Default value:** `20000`

---

**Property:** `parquet.page.write-checksum.enabled`  
**Description:** Whether to write out page level checksums.  
**Default value:** `true`

---

**Property:** `parquet.crypto.factory.class`  
**Description:** Class implementing EncryptionPropertiesFactory.  
**Default value:** None. If not set, the file won't be encrypted by a crypto factory.  

## Class: ParquetInputFormat

**Property:** `parquet.read.support.class`  
**Description:** The read support class that is used in
ParquetInputFormat to materialize records. It should be a the descendant class of `org.apache.parquet.hadoop.api.ReadSupport`

---

**Property:** `parquet.read.filter`  
**Description:** The filter class name that implements `org.apache.parquet.filter.UnboundRecordFilter`. This class is for the old filter API in the package `org.apache.parquet.filter`, it filters records during record assembly.

---

 **Property:** `parquet.private.read.filter.predicate`  
 **Description:** The filter object used in the new filter API in the package `org.apache.parquet.filter2.predicate`
 Note that this object should implements `org.apache.parquet.filter2.FilterPredicate` and the value of this property should be a gzip compressed base64 encoded java serialized object.  
 The new filter API can filter records or filter entire row groups of records without reading them at all.

**Notes:**
- User should either use the old filter API (`parquet.read.filter`) or the new one (`parquet.private.read.filter.predicate`). It is recommended to use the new API.
- It is also recommended to set the filter manually with [ParquetInputFormat.setFilterPredicate](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetInputFormat.java#L217-L227) instead of setting this property.
- The property `parquet.private.read.filter.predicate.human.readable` contains the filter in a human readable format. It is used only for debugging.

---

**Property:** `parquet.strict.typing`  
**Description:** Whether to enable type checking for conflicting schema.  
**Default value:** `true`

---

**Property:** `parquet.filter.record-level.enabled`  
**Description:** Whether record-level filtering is enabled.  
**Default value:** `true`

---

**Property:** `parquet.filter.stats.enabled`  
**Description:** Whether row group stats filtering is enabled.  
**Default value:** `true`

---

**Property:** `parquet.filter.dictionary.enabled`  
**Description:** Whether row group dictionary filtering is enabled.  
**Default value:** `true`

---

**Property:** `parquet.filter.columnindex.enabled`  
**Description:** Whether column index filtering of pages is enabled.  
**Default value:** `true`

---

**Property:** `parquet.page.verify-checksum.enabled`  
**Description:** whether page level checksum verification is enabled.  
**Default value:** `false`

---

**Property:** `parquet.filter.bloom.enabled`  
**Description:** whether row group bloom filtering is enabled.  
**Default value:** `true`

---

**Property:** `parquet.task.side.metadata`  
**Description:** Whether to turn on or off task side metadata loading:
   * If true then metadata is read on the task side and some tasks may finish immediately.
   * If false metadata is read on the client which is slower if there is a lot of metadata but tasks will only be spawn if there is work to do.

**Default value:** `true`

---

**Property:** `parquet.split.files`  
**Description:** If it is false, files are read sequentially.  
**Default value:** `true`

## Class: ReadSupport

**Property:** `parquet.read.schema`  
**Description:** The read projection schema.

## Class: UnmaterializableRecordCounter

**Property:** `parquet.read.bad.record.threshold`  
**Description:** The percentage of bad records to tolerate.  
**Default value:** `0`

## Class: ZstandardCodec

**Property:** `parquet.compression.codec.zstd.bufferPool.enabled`  
**Description:** If it is true, [RecyclingBufferPool](https://github.com/luben/zstd-jni/blob/master/src/main/java/com/github/luben/zstd/RecyclingBufferPool.java) is used.  
**Default value:** `true`

---

**Property:** `parquet.compression.codec.zstd.level`  
**Description:** The compression level of ZSTD. The valid range is 1~22. Generally the higher compression level, the higher compression ratio can be achieved, but the writing time will be longer.  
**Default value:** `3`

---

**Property:** `parquet.compression.codec.zstd.workers`  
**Description:** The number of threads will be spawned to compress in parallel. More workers improve speed, but also increase memory usage. When it is 0, it works as single-threaded mode.  
**Default value:** `0`

## Class: HadoopReadOptions

**Property:** `parquet.crypto.factory.class`  
**Description:** Class implementing DecryptionPropertiesFactory.
**Default value:** None. If not set, the file won't be decrypted by a crypto factory.

## Class: PropertiesDrivenCryptoFactory

**Property:** `parquet.encryption.column.keys`  
**Description:** List of columns to encrypt, with master key IDs (see HIVE-21848).Format: `<masterKeyID>:<colName>,<colName>;<masterKeyID>:<colName>...`. Note: nested column names must be specified as full dot-separated paths for each leaf column.  
**Default value:** None.

---

**Property:** `parquet.encryption.footer.key`  
**Description:** Master key ID for footer encryption/signing.  
**Default value:** None.

---

**Property:** `parquet.encryption.uniform.key`  
**Description:** Master key ID for uniform encryption of all columns and footer. If set, `column.keys` and `footer.key` parameters should not be used.  
**Default value:** None.

---

**Property:** `parquet.encryption.algorithm`  
**Description:** Parquet encryption algorithm. Can be `AES_GCM_V1` or `AES_GCM_CTR_V1`.  
**Default value:** `AES_GCM_V1`

---

**Property:** `parquet.encryption.plaintext.footer`  
**Description:** Write files in plaintext footer mode, that makes many footer fields visible (e.g. schema) but allows legacy readers to access unencrypted columns. The plaintext footer is signed with the footer key. 
If `false`, write files in encrypted footer mode, that fully encrypts the footer, and signs it with the footer key.  
**Default value:** `false`

---

**Property:** `parquet.encryption.kms.client.class`  
**Description:** Class implementing the KmsClient interface. "KMS" stands for “key management service”. The Client will interact with a KMS Server to wrap/unrwap encryption keys.  
**Default value:** None

---

**Property:** `parquet.encryption.kms.instance.id`  
**Description:** ID of the KMS instance that will be used for encryption (if multiple KMS instances are available).  
**Default value:** `DEFAULT`

---

**Property:** `parquet.encryption.kms.instance.url`  
**Description:** URL of the KMS instance.  
**Default value:** `DEFAULT`

---

**Property:** `parquet.encryption.key.access.token`  
**Description:** Authorization token that will be passed to KMS.  
**Default value:** `DEFAULT`

---

**Property:** `parquet.encryption.double.wrapping`  
**Description:** Use double wrapping - where data encryption keys (DEKs) are encrypted with key encryption keys (KEKs), which in turn are encrypted with master keys.
If `false`, DEKs are directly encrypted with master keys, KEKs are not used.  
**Default value:** `true`

---

**Property:** `parquet.encryption.cache.lifetime.seconds`  
**Description:** Lifetime of cached entities (key encryption keys, local wrapping keys, KMS client objects).  
**Default value:** `600` (10 minutes)

---

**Property:** `parquet.encryption.key.material.store.internally`  
**Description:** Store key material inside Parquet file footers; this mode doesn’t produce additional files. 
If `false`, key material is stored in separate new files, created in the same folder - this mode enables key rotation for immutable Parquet files.  
**Default value:** `true`

---

**Property:** `parquet.encryption.data.key.length.bits`  
**Description:** Length of data encryption keys (DEKs), randomly generated by parquet key management tools. Can be 128, 192 or 256 bits.  
**Default value:** `128`

---

**Property:** `parquet.encryption.kek.length.bits`  
**Description:** Length of key encryption keys (KEKs), randomly generated by parquet key management tools. Can be 128, 192 or 256 bits.  
**Default value:** `128`

