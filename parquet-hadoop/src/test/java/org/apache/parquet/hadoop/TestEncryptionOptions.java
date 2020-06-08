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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionKeyRetrieverMock;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.statistics.RandomValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/*
 * This file contains samples for writing and reading encrypted Parquet files in different
 * encryption and decryption configurations. The samples have the following goals:
 * 1) Demonstrate usage of different options for data encryption and decryption.
 * 2) Produce encrypted files for interoperability tests with other (eg parquet-cpp)
 *    readers that support encryption.
 * 3) Produce encrypted files with plaintext footer, for testing the ability of legacy
 *    readers to parse the footer and read unencrypted columns.
 * 4) Perform interoperability tests with other (eg parquet-cpp) writers, by reading
 *    encrypted files produced by these writers.
 *
 * The write sample produces number of parquet files, each encrypted with a different
 * encryption configuration as described below.
 * The name of each file is in the form of:
 * tester<encryption config number>.parquet.encrypted.
 *
 * The read sample creates a set of decryption configurations and then uses each of them
 * to read all encrypted files in the input directory.
 *
 * The different encryption and decryption configurations are listed below.
 *
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The write sample creates files with seven columns in the following
 * encryption configurations:
 *
 *  UNIFORM_ENCRYPTION:             Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  ENCRYPT_COLUMNS_AND_FOOTER:     Encrypt six columns and the footer, with different
 *                                  keys.
 *  ENCRYPT_COLUMNS_PLAINTEXT_FOOTER: Encrypt six columns, with different keys.
 *                                  Do not encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  ENCRYPT_COLUMNS_AND_FOOTER_AAD: Encrypt six columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  ENCRYPT_COLUMNS_AND_FOOTER_DISABLE_AAD_STORAGE:   Encrypt six columns and the footer,
 *                                  with different keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  ENCRYPT_COLUMNS_AND_FOOTER_CTR: Encrypt six columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 *  NO_ENCRYPTION:                  Do not encrypt anything
 *
 *
 * The read sample uses each of the following decryption configurations to read every
 * encrypted files in the input directory:
 *
 *  DECRYPT_WITH_KEY_RETRIEVER:     Decrypt using key retriever that holds the keys of
 *                                  the encrypted columns and the footer key.
 *  DECRYPT_WITH_KEY_RETRIEVER_AAD: Decrypt using key retriever that holds the keys of
 *                                  the encrypted columns and the footer key. Supplies
 *                                  aad_prefix to verify file identity.
 *  DECRYPT_WITH_EXPLICIT_KEYS:     Decrypt using explicit column and footer keys
 *                                  (instead of key retrieval callback).
 *  NO_DECRYPTION:                  Do not decrypt anything.
 */
public class TestEncryptionOptions {
  private static final Logger LOG = LoggerFactory.getLogger(TestEncryptionOptions.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  private static final byte[] FOOTER_ENCRYPTION_KEY = "0123456789012345".getBytes();
  private static final byte[][] COLUMN_ENCRYPTION_KEYS = { "1234567890123450".getBytes(),
    "1234567890123451".getBytes(), "1234567890123452".getBytes(), "1234567890123453".getBytes(),
    "1234567890123454".getBytes(), "1234567890123455".getBytes()};
  private static final String[] COLUMN_ENCRYPTION_KEY_IDS = { "kc1", "kc2", "kc3", "kc4", "kc5", "kc6"};
  private static final String FOOTER_ENCRYPTION_KEY_ID = "kf";
  private static final String AAD_PREFIX_STRING = "tester";
  private static final String BOOLEAN_FIELD_NAME = "boolean_field";
  private static final String INT32_FIELD_NAME = "int32_field";
  private static final String FLOAT_FIELD_NAME = "float_field";
  private static final String DOUBLE_FIELD_NAME = "double_field";
  private static final String BINARY_FIELD_NAME = "ba_field";
  private static final String FIXED_LENGTH_BINARY_FIELD_NAME = "flba_field";
  private static final String PLAINTEXT_INT32_FIELD_NAME = "plain_int32_field";

  private static final byte[] footerKeyMetadata = FOOTER_ENCRYPTION_KEY_ID.getBytes(StandardCharsets.UTF_8);
  private static final byte[] AADPrefix = AAD_PREFIX_STRING.getBytes(StandardCharsets.UTF_8);

  private static final int ROW_COUNT = 10000;
  private static final List<SingleRow> DATA = Collections.unmodifiableList(generateRandomData(ROW_COUNT));
  private static final List<SingleRow> LINEAR_DATA = Collections.unmodifiableList(generateLinearData(250));

  private static final MessageType SCHEMA =
    new MessageType("schema",
      new PrimitiveType(REQUIRED, BOOLEAN, BOOLEAN_FIELD_NAME),
      new PrimitiveType(REQUIRED, INT32, INT32_FIELD_NAME),
      new PrimitiveType(REQUIRED, FLOAT, FLOAT_FIELD_NAME),
      new PrimitiveType(REQUIRED, DOUBLE, DOUBLE_FIELD_NAME),
      new PrimitiveType(OPTIONAL, BINARY, BINARY_FIELD_NAME),
      Types.required(FIXED_LEN_BYTE_ARRAY).length(FIXED_LENGTH).named(FIXED_LENGTH_BINARY_FIELD_NAME),
      new PrimitiveType(OPTIONAL, INT32, PLAINTEXT_INT32_FIELD_NAME));

  private static final DecryptionKeyRetrieverMock decryptionKeyRetrieverMock = new DecryptionKeyRetrieverMock()
    .putKey(FOOTER_ENCRYPTION_KEY_ID, FOOTER_ENCRYPTION_KEY)
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[0], COLUMN_ENCRYPTION_KEYS[0])
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[1], COLUMN_ENCRYPTION_KEYS[1])
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[2], COLUMN_ENCRYPTION_KEYS[2])
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[3], COLUMN_ENCRYPTION_KEYS[3])
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[4], COLUMN_ENCRYPTION_KEYS[4])
    .putKey(COLUMN_ENCRYPTION_KEY_IDS[5], COLUMN_ENCRYPTION_KEYS[5]);

  public enum EncryptionConfiguration {
    UNIFORM_ENCRYPTION {
      public FileEncryptionProperties getEncryptionProperties() {
        return getUniformEncryptionEncryptionProperties();
      }
    },
    ENCRYPT_COLUMNS_AND_FOOTER {
      public FileEncryptionProperties getEncryptionProperties() {
        return getEncryptColumnsAndFooterEncryptionProperties();
      }
    },
    ENCRYPT_COLUMNS_PLAINTEXT_FOOTER {
      public FileEncryptionProperties getEncryptionProperties() {
        return getPlaintextFooterEncryptionProperties();
      }
    },
    ENCRYPT_COLUMNS_AND_FOOTER_AAD {
      public FileEncryptionProperties getEncryptionProperties() {
        return getEncryptWithAADEncryptionProperties();
      }
    },
    ENCRYPT_COLUMNS_AND_FOOTER_DISABLE_AAD_STORAGE {
      public FileEncryptionProperties getEncryptionProperties() {
        return getDisableAADStorageEncryptionProperties();
      }
    },
    ENCRYPT_COLUMNS_AND_FOOTER_CTR {
      public FileEncryptionProperties getEncryptionProperties() {
        return getCTREncryptionProperties();
      }
    },
    NO_ENCRYPTION {
      public FileEncryptionProperties getEncryptionProperties() {
        return null;
      }
    };

    abstract public FileEncryptionProperties getEncryptionProperties();
  }

  public enum DecryptionConfiguration {
    DECRYPT_WITH_KEY_RETRIEVER {
      public FileDecryptionProperties getDecryptionProperties() {
        return getKeyRetrieverDecryptionProperties();
      }
    },
    DECRYPT_WITH_KEY_RETRIEVER_AAD {
      public FileDecryptionProperties getDecryptionProperties() {
        return getKeyRetrieverAADDecryptionProperties();
      }
    },
    DECRYPT_WITH_EXPLICIT_KEYS {
      public FileDecryptionProperties getDecryptionProperties() {
        return getExplicitKeysDecryptionProperties();
      }
    },
    NO_DECRYPTION {
      public FileDecryptionProperties getDecryptionProperties() {
        return null;
      }
    };

    abstract public FileDecryptionProperties getDecryptionProperties();
  }

  @Test
  public void testWriteReadEncryptedParquetFiles() throws IOException {
    Path rootPath = new Path(temporaryFolder.getRoot().getPath());
    LOG.info("======== testWriteReadEncryptedParquetFiles {} ========", rootPath.toString());
    byte[] AADPrefix = AAD_PREFIX_STRING.getBytes(StandardCharsets.UTF_8);
    // Write using various encryption configuraions
    testWriteEncryptedParquetFiles(rootPath, DATA);
    // Read using various decryption configurations.
    testReadEncryptedParquetFiles(rootPath, DATA);
  }

  @Test
  public void testInteropReadEncryptedParquetFiles() throws IOException {
    Path rootPath = new Path(PARQUET_TESTING_PATH);
    LOG.info("======== testInteropReadEncryptedParquetFiles {} ========", rootPath.toString());
    byte[] AADPrefix = AAD_PREFIX_STRING.getBytes(StandardCharsets.UTF_8);
    // Read using various decryption configurations.
    testInteropReadEncryptedParquetFiles(rootPath, true/*readOnlyEncrypted*/, LINEAR_DATA);
  }

  private static List<SingleRow> generateRandomData(int rowCount) {
    List<SingleRow> dataList = new ArrayList<>(rowCount);
    for (int row = 0; row < rowCount; ++row) {
      SingleRow newRow = new SingleRow(RANDOM.nextBoolean(),
        intGenerator.nextValue(),  floatGenerator.nextValue(),
        doubleGenerator.nextValue(), binaryGenerator.nextValue().getBytes(),
        fixedBinaryGenerator.nextValue().getBytes(), intGenerator.nextValue());
      dataList.add(newRow);
    }
    return dataList;
  }

  private static List<SingleRow> generateLinearData(int rowCount) {
    List<SingleRow> dataList = new ArrayList<>(rowCount);
    String baseStr = "parquet";
    for (int row = 0; row < rowCount; ++row) {
      boolean boolean_val = ((row % 2) == 0) ? true : false;
      float float_val = (float) row * 1.1f;
      double double_val = (row * 1.1111111);

      byte[] binary_val = null;
      if ((row % 2) == 0) {
        char firstChar = (char) ((int) '0' + row / 100);
        char secondChar = (char) ((int) '0' + (row / 10) % 10);
        char thirdChar = (char) ((int) '0' + row % 10);
        binary_val = (baseStr + firstChar + secondChar + thirdChar).getBytes(StandardCharsets.UTF_8);
      }
      char[] fixed = new char[FIXED_LENGTH];
      char[] aChar = Character.toChars(row);
      Arrays.fill(fixed, aChar[0]);

      SingleRow newRow = new SingleRow(boolean_val,
        row, float_val, double_val,
        binary_val, new String(fixed).getBytes(StandardCharsets.UTF_8), null/*plaintext_int32_field*/);
      dataList.add(newRow);
    }
    return dataList;
  }

  public static class SingleRow {
    public final boolean boolean_field;
    public final int int32_field;
    public final float float_field;
    public final double double_field;
    public final byte[] ba_field;
    public final byte[] flba_field;
    public final Integer plaintext_int32_field; // Can be null, since it doesn't exist in C++-created files yet.

    public SingleRow(boolean boolean_field,
                     int int32_field,
                     float float_field,
                     double double_field,
                     byte[] ba_field,
                     byte[] flba_field,
                     Integer plaintext_int32_field) {
      this.boolean_field = boolean_field;
      this.int32_field = int32_field;
      this.float_field = float_field;
      this.double_field = double_field;
      this.ba_field = ba_field;
      this.flba_field = flba_field;
      this.plaintext_int32_field = plaintext_int32_field;
    }
  }

  private void testWriteEncryptedParquetFiles(Path root, List<SingleRow> data) throws IOException {
    Configuration conf = new Configuration();

    int pageSize = data.size() / 10;     // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

    SimpleGroupFactory f = new SimpleGroupFactory(SCHEMA);

    EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
    for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
      Path file = new Path(root, getFileName(encryptionConfiguration));
      FileEncryptionProperties encryptionProperties = encryptionConfiguration.getEncryptionProperties();
      LOG.info("\nWrite " + file.toString());
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withType(SCHEMA)
        .withConf(conf)
        .withEncryption(encryptionProperties)
        .build()) {

        for (SingleRow singleRow : data) {
          writer.write(
            f.newGroup()
              .append(BOOLEAN_FIELD_NAME, singleRow.boolean_field)
              .append(INT32_FIELD_NAME, singleRow.int32_field)
              .append(FLOAT_FIELD_NAME, singleRow.float_field)
              .append(DOUBLE_FIELD_NAME, singleRow.double_field)
              .append(BINARY_FIELD_NAME, Binary.fromConstantByteArray(singleRow.ba_field))
              .append(FIXED_LENGTH_BINARY_FIELD_NAME, Binary.fromConstantByteArray(singleRow.flba_field))
              .append(PLAINTEXT_INT32_FIELD_NAME, singleRow.plaintext_int32_field));

        }
      }
    }
  }

  private String getFileName(EncryptionConfiguration encryptionConfiguration) {
    return encryptionConfiguration.toString().toLowerCase() + ".parquet.encrypted";
  }

  private void testReadEncryptedParquetFiles(Path root, List<SingleRow> data) {
    Configuration conf = new Configuration();
    DecryptionConfiguration[] decryptionConfigurations = DecryptionConfiguration.values();
    for (DecryptionConfiguration decryptionConfiguration : decryptionConfigurations) {
      EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
      for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
        Path file = new Path(root, getFileName(encryptionConfiguration));
        LOG.info("==> Decryption configuration {}", decryptionConfiguration);
        FileDecryptionProperties fileDecryptionProperties = decryptionConfiguration.getDecryptionProperties();

        LOG.info("--> Read file {} {}", file.toString(), encryptionConfiguration);

        // Read only the non-encrypted columns
        if ((decryptionConfiguration == DecryptionConfiguration.NO_DECRYPTION) &&
          (encryptionConfiguration == EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
          conf.set("parquet.read.schema", Types.buildMessage()
            .optional(INT32).named(PLAINTEXT_INT32_FIELD_NAME)
            .named("FormatTestObject").toString());
        }

        int rowNum = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
          .withConf(conf)
          .withDecryption(fileDecryptionProperties)
          .build()) {
          for (Group group = reader.read(); group != null; group = reader.read()) {
            SingleRow rowExpected = data.get(rowNum++);
            // plaintext columns
            if ((null != rowExpected.plaintext_int32_field) &&
              rowExpected.plaintext_int32_field != group.getInteger(PLAINTEXT_INT32_FIELD_NAME, 0)) {
              addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
            }
            // encrypted columns
            if (decryptionConfiguration != DecryptionConfiguration.NO_DECRYPTION) {
              if (rowExpected.boolean_field != group.getBoolean(BOOLEAN_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong bool", encryptionConfiguration, decryptionConfiguration);
              }
              if (rowExpected.int32_field != group.getInteger(INT32_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
              }
              if (rowExpected.float_field != group.getFloat(FLOAT_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong float", encryptionConfiguration, decryptionConfiguration);
              }
              if (rowExpected.double_field != group.getDouble(DOUBLE_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong double", encryptionConfiguration, decryptionConfiguration);
              }
              if ((null != rowExpected.ba_field) &&
                !Arrays.equals(rowExpected.ba_field, group.getBinary(BINARY_FIELD_NAME, 0).getBytes())) {
                addErrorToErrorCollectorAndLog("Wrong byte array", encryptionConfiguration, decryptionConfiguration);
              }
              if (!Arrays.equals(rowExpected.flba_field,
                group.getBinary(FIXED_LENGTH_BINARY_FIELD_NAME, 0).getBytes())) {
                addErrorToErrorCollectorAndLog("Wrong fixed-length byte array",
                  encryptionConfiguration, decryptionConfiguration);
              }
            }
          }
        } catch (ParquetCryptoRuntimeException e) {
          String errorMessage = e.getMessage();
          checkResult(file.getName(), decryptionConfiguration, (null == errorMessage ? e.toString() : errorMessage));
        } catch (Exception e) {
          addErrorToErrorCollectorAndLog(
            "Unexpected exception: " + e.getClass().getName() + " with message: " + e.getMessage(),
            encryptionConfiguration, decryptionConfiguration);
        }
        conf.unset("parquet.read.schema");
      }
    }
  }

  private void testInteropReadEncryptedParquetFiles(Path root, boolean readOnlyEncrypted, List<SingleRow> data) throws IOException {
    Configuration conf = new Configuration();
    DecryptionConfiguration[] decryptionConfigurations = DecryptionConfiguration.values();
    for (DecryptionConfiguration decryptionConfiguration : decryptionConfigurations) {
      EncryptionConfiguration[] encryptionConfigurations = EncryptionConfiguration.values();
      for (EncryptionConfiguration encryptionConfiguration : encryptionConfigurations) {
        if (readOnlyEncrypted && (EncryptionConfiguration.NO_ENCRYPTION == encryptionConfiguration)) {
          continue;
        }
        Path file = new Path(root, getFileName(encryptionConfiguration));
        LOG.info("==> Decryption configuration {}", decryptionConfiguration);
        FileDecryptionProperties fileDecryptionProperties = decryptionConfiguration.getDecryptionProperties();

        LOG.info("--> Read file {} {}", file.toString(), encryptionConfiguration);

        // Read only the non-encrypted columns
        if ((decryptionConfiguration == DecryptionConfiguration.NO_DECRYPTION) &&
          (encryptionConfiguration == EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
          conf.set("parquet.read.schema", Types.buildMessage()
            .required(BOOLEAN).named(BOOLEAN_FIELD_NAME)
            .required(INT32).named(INT32_FIELD_NAME)
            .named("FormatTestObject").toString());
        }

        int rowNum = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
          .withConf(conf)
          .withDecryption(fileDecryptionProperties)
          .build()) {
          for (Group group = reader.read(); group != null; group = reader.read()) {
            SingleRow rowExpected = data.get(rowNum++);
            // plaintext columns
            if (rowExpected.boolean_field != group.getBoolean(BOOLEAN_FIELD_NAME, 0)) {
              addErrorToErrorCollectorAndLog("Wrong bool", encryptionConfiguration, decryptionConfiguration);
            }
            if (rowExpected.int32_field != group.getInteger(INT32_FIELD_NAME, 0)) {
              addErrorToErrorCollectorAndLog("Wrong int", encryptionConfiguration, decryptionConfiguration);
            }
            // encrypted columns
            if (decryptionConfiguration != DecryptionConfiguration.NO_DECRYPTION) {
              if (rowExpected.float_field != group.getFloat(FLOAT_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong float", encryptionConfiguration, decryptionConfiguration);
              }
              if (rowExpected.double_field != group.getDouble(DOUBLE_FIELD_NAME, 0)) {
                addErrorToErrorCollectorAndLog("Wrong double", encryptionConfiguration, decryptionConfiguration);
              }
            }
          }
        } catch (ParquetCryptoRuntimeException e) {
          String errorMessage = e.getMessage();
          checkResult(file.getName(), decryptionConfiguration, (null == errorMessage ? e.toString() : errorMessage));
        } catch (Exception e) {
          addErrorToErrorCollectorAndLog(
            "Unexpected exception: " + e.getClass().getName() + " with message: " + e.getMessage(),
            encryptionConfiguration, decryptionConfiguration);
        }
        conf.unset("parquet.read.schema");
      }
    }
  }


  /**
   * Check that the decryption result is as expected.
   */
  private void checkResult(String file, DecryptionConfiguration decryptionConfiguration, String exceptionMsg) {
    // Extract encryptionConfigurationNumber from the parquet file name.
    EncryptionConfiguration encryptionConfiguration = getEncryptionConfigurationFromFilename(file);

    // Encryption_configuration 5 contains aad_prefix and
    // disable_aad_prefix_storage.
    // An exception is expected to be thrown if the file is not decrypted with aad_prefix.
    if (encryptionConfiguration == EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER_DISABLE_AAD_STORAGE) {
      if (decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_KEY_RETRIEVER ||
        decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_EXPLICIT_KEYS) {
        if (!exceptionMsg.contains("AAD")) {
          addErrorToErrorCollectorAndLog("Expecting AAD related exception", exceptionMsg,
            encryptionConfiguration, decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    // Decryption configuration 2 contains aad_prefix. An exception is expected to
    // be thrown if the file was not encrypted with the same aad_prefix.
    if (decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_KEY_RETRIEVER_AAD) {
      if (encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER_DISABLE_AAD_STORAGE &&
        encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_AND_FOOTER_AAD &&
        encryptionConfiguration != EncryptionConfiguration.NO_ENCRYPTION) {
        if (!exceptionMsg.contains("AAD")) {
          addErrorToErrorCollectorAndLog("Expecting AAD related exception", exceptionMsg,
            encryptionConfiguration, decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    // Encryption_configuration 7 has null encryptor, so parquet is plaintext.
    // An exception is expected to be thrown if the file is being decrypted.
    if (encryptionConfiguration == EncryptionConfiguration.NO_ENCRYPTION) {
      if ((decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_KEY_RETRIEVER) ||
        (decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_KEY_RETRIEVER_AAD) ||
        (decryptionConfiguration == DecryptionConfiguration.DECRYPT_WITH_EXPLICIT_KEYS)) {
        if (!exceptionMsg.endsWith("Applying decryptor on plaintext file")) {
          addErrorToErrorCollectorAndLog("Expecting exception Applying decryptor on plaintext file",
            exceptionMsg, encryptionConfiguration, decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    // Decryption configuration 4 is null, so only plaintext file can be read. An exception is expected to
    // be thrown if the file is encrypted.
    if (decryptionConfiguration == DecryptionConfiguration.NO_DECRYPTION) {
      if ((encryptionConfiguration != EncryptionConfiguration.NO_ENCRYPTION &&
        encryptionConfiguration != EncryptionConfiguration.ENCRYPT_COLUMNS_PLAINTEXT_FOOTER)) {
        if (!exceptionMsg.endsWith("No keys available") && !exceptionMsg.endsWith("Null File Decryptor") && !exceptionMsg.endsWith("Footer key unavailable")) {
          addErrorToErrorCollectorAndLog("Expecting No keys available exception", exceptionMsg,
            encryptionConfiguration, decryptionConfiguration);
        } else {
          LOG.info("Exception as expected: " + exceptionMsg);
        }
        return;
      }
    }
    if (null != exceptionMsg && !exceptionMsg.isEmpty()) {
      addErrorToErrorCollectorAndLog("Didn't expect an exception", exceptionMsg,
        encryptionConfiguration, decryptionConfiguration);
    }
  }

  private EncryptionConfiguration getEncryptionConfigurationFromFilename(String file) {
    if (!file.endsWith(".parquet.encrypted")) {
      return null;
    }
    String fileNamePrefix = file.replaceFirst(".parquet.encrypted", "");
    try {
      EncryptionConfiguration encryptionConfiguration = EncryptionConfiguration.valueOf(fileNamePrefix.toUpperCase());
      return encryptionConfiguration;
    } catch (IllegalArgumentException e) {
      LOG.error("File name doesn't match any known encryption configuration: " + file);
      errorCollector.addError(e);
      return null;
    }
  }

  private void addErrorToErrorCollectorAndLog(String errorMessage, String exceptionMessage, EncryptionConfiguration encryptionConfiguration,
                                              DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage = String.format("%s - %s Error: %s, but got [%s]",
      encryptionConfiguration, decryptionConfiguration, errorMessage, exceptionMessage);

    errorCollector.addError(new Throwable(fullErrorMessage));
    LOG.error(fullErrorMessage);
  }

  private void addErrorToErrorCollectorAndLog(String errorMessage, EncryptionConfiguration encryptionConfiguration,
                                                     DecryptionConfiguration decryptionConfiguration) {
    String fullErrorMessage = String.format("%s - %s Error: %s",
      encryptionConfiguration, decryptionConfiguration, errorMessage);

    errorCollector.addError(new Throwable(fullErrorMessage));
    LOG.error(fullErrorMessage);
  }

  private static Map<ColumnPath, ColumnEncryptionProperties> getColumnEncryptionPropertiesMap() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = new HashMap<>();

    ColumnEncryptionProperties columnPropertiesDouble = ColumnEncryptionProperties
      .builder(DOUBLE_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[0])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[0])
      .build();
    columnPropertiesMap.put(columnPropertiesDouble.getPath(), columnPropertiesDouble);

    ColumnEncryptionProperties columnPropertiesFloat = ColumnEncryptionProperties
      .builder(FLOAT_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[1])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[1])
      .build();
    columnPropertiesMap.put(columnPropertiesFloat.getPath(), columnPropertiesFloat);

    ColumnEncryptionProperties columnPropertiesBool = ColumnEncryptionProperties
      .builder(BOOLEAN_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[2])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[2])
      .build();
    columnPropertiesMap.put(columnPropertiesBool.getPath(), columnPropertiesBool);

    ColumnEncryptionProperties columnPropertiesInt32 = ColumnEncryptionProperties
      .builder(INT32_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[3])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[3])
      .build();
    columnPropertiesMap.put(columnPropertiesInt32.getPath(), columnPropertiesInt32);

    ColumnEncryptionProperties columnPropertiesBinary = ColumnEncryptionProperties
      .builder(BINARY_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[4])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[4])
      .build();
    columnPropertiesMap.put(columnPropertiesBinary.getPath(), columnPropertiesBinary);

    ColumnEncryptionProperties columnPropertiesFixed = ColumnEncryptionProperties
      .builder(FIXED_LENGTH_BINARY_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[5])
      .withKeyID(COLUMN_ENCRYPTION_KEY_IDS[5])
      .build();
    columnPropertiesMap.put(columnPropertiesFixed.getPath(), columnPropertiesFixed);

    return columnPropertiesMap;
  }

  private static Map<ColumnPath, ColumnDecryptionProperties> getColumnDecryptionPropertiesMap() {
    Map<ColumnPath, ColumnDecryptionProperties> columnMap = new HashMap<>();

    ColumnDecryptionProperties columnDecryptionPropsDouble = ColumnDecryptionProperties
      .builder(DOUBLE_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[0])
      .build();
    columnMap.put(columnDecryptionPropsDouble.getPath(), columnDecryptionPropsDouble);

    ColumnDecryptionProperties columnDecryptionPropsFloat = ColumnDecryptionProperties
      .builder(FLOAT_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[1])
      .build();
    columnMap.put(columnDecryptionPropsFloat.getPath(), columnDecryptionPropsFloat);

    ColumnDecryptionProperties columnDecryptionPropsBool = ColumnDecryptionProperties
      .builder(BOOLEAN_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[2])
      .build();
    columnMap.put(columnDecryptionPropsBool.getPath(), columnDecryptionPropsBool);

    ColumnDecryptionProperties columnDecryptionPropsInt32 = ColumnDecryptionProperties
      .builder(INT32_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[3])
      .build();
    columnMap.put(columnDecryptionPropsInt32.getPath(), columnDecryptionPropsInt32);

    ColumnDecryptionProperties columnDecryptionPropsBinary = ColumnDecryptionProperties
      .builder(BINARY_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[4])
      .build();
    columnMap.put(columnDecryptionPropsBinary.getPath(), columnDecryptionPropsBinary);

    ColumnDecryptionProperties columnDecryptionPropsFixed = ColumnDecryptionProperties
      .builder(FIXED_LENGTH_BINARY_FIELD_NAME)
      .withKey(COLUMN_ENCRYPTION_KEYS[5])
      .build();
    columnMap.put(columnDecryptionPropsFixed.getPath(), columnDecryptionPropsFixed);

    return columnMap;
  }

  /**
   * Encryption configuration 1: Encrypt all columns and the footer with the same key.
   */
  private static FileEncryptionProperties getUniformEncryptionEncryptionProperties() {
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata).build();
  }

  /**
   * Encryption configuration 2: Encrypt six columns and the footer, with different keys.
   */
  private static FileEncryptionProperties getEncryptColumnsAndFooterEncryptionProperties() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = getColumnEncryptionPropertiesMap();
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap)
      .build();
  }

  /**
   * Encryption configuration 3: Encrypt six columns, with different keys.
   * Don't encrypt footer.
   * (plaintext footer mode, readable by legacy readers)
   */
  private static FileEncryptionProperties getPlaintextFooterEncryptionProperties() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = getColumnEncryptionPropertiesMap();
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap)
      .withPlaintextFooter()
      .build();
  }

  /**
   * Encryption configuration 4: Encrypt six columns and the footer, with different keys.
   * Use aad_prefix.
   */
  private static FileEncryptionProperties getEncryptWithAADEncryptionProperties() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = getColumnEncryptionPropertiesMap();
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap)
      .withAADPrefix(AADPrefix)
      .build();
  }

  /**
   * Encryption configuration 5: Encrypt six columns and the footer, with different keys.
   * Use aad_prefix and disable_aad_prefix_storage.
   */
  private static FileEncryptionProperties getDisableAADStorageEncryptionProperties() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = getColumnEncryptionPropertiesMap();
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap)
      .withAADPrefix(AADPrefix)
      .withoutAADPrefixStorage()
      .build();
  }

  /**
   *  Encryption configuration 6: Encrypt six columns and the footer, with different keys.
   *  Use AES_GCM_CTR_V1 algorithm.
   */
  private static FileEncryptionProperties getCTREncryptionProperties() {
    Map<ColumnPath, ColumnEncryptionProperties> columnPropertiesMap = getColumnEncryptionPropertiesMap();
    return FileEncryptionProperties.builder(FOOTER_ENCRYPTION_KEY)
      .withFooterKeyMetadata(footerKeyMetadata)
      .withEncryptedColumns(columnPropertiesMap)
      .withAlgorithm(ParquetCipher.AES_GCM_CTR_V1)
      .build();
  }


  /**
   * Decryption configuration 1: Decrypt using key retriever callback that holds the keys
   * of the encrypted columns and the footer key.
   */
  private static FileDecryptionProperties getKeyRetrieverDecryptionProperties() {
    return FileDecryptionProperties.builder()
      .withKeyRetriever(decryptionKeyRetrieverMock)
      .build();
  }

  /**
   * Decryption configuration 2: Decrypt using key retriever callback that holds the keys
   * of the encrypted columns and the footer key. Supply aad_prefix.
   */
  private static FileDecryptionProperties getKeyRetrieverAADDecryptionProperties() {
    return FileDecryptionProperties.builder()
      .withKeyRetriever(decryptionKeyRetrieverMock)
      .withAADPrefix(AADPrefix)
      .build();
  }

  /**
   * Decryption configuration 3: Decrypt using explicit column and footer keys.
   */
  private static FileDecryptionProperties getExplicitKeysDecryptionProperties() {
    Map<ColumnPath, ColumnDecryptionProperties> columnMap = getColumnDecryptionPropertiesMap();

    return FileDecryptionProperties.builder()
      .withColumnKeys(columnMap)
      .withFooterKey(FOOTER_ENCRYPTION_KEY)
      .build();
  }
}
