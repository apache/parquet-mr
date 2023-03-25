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
package org.apache.parquet.hadoop.metadata;

import static java.util.Collections.unmodifiableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.schema.MessageType;

/**
 * File level meta data (Schema, codec, ...)
 */
public final class FileMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  public enum EncryptionType {UNENCRYPTED, PLAINTEXT_FOOTER, ENCRYPTED_FOOTER}

  private final MessageType schema;
  private final Map<String, String> keyValueMetaData;
  private final String createdBy;
  private final InternalFileDecryptor fileDecryptor;
  private final EncryptionType encryptionType;

  /**
   * FileMetaData for writers.
   * @param schema the schema for the file
   * @param keyValueMetaData the app specific metadata
   * @param createdBy the description of the library that created the file
   *
   * @throws NullPointerException if schema or keyValueMetaData is {@code null}
   */
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy) {
    this(schema, keyValueMetaData, createdBy, null, null);
  }

  @Deprecated
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy,
                      InternalFileDecryptor fileDecryptor) {
    this(schema, keyValueMetaData, createdBy, null, fileDecryptor);
  }

  /**
   * FileMetaData for readers (decryptors).
   */
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy,
                      EncryptionType encryptionType, InternalFileDecryptor fileDecryptor) {
    super();
    this.schema = Objects.requireNonNull(schema, "schema cannot be null");
    this.keyValueMetaData = unmodifiableMap(Objects
        .requireNonNull(keyValueMetaData, "keyValueMetaData cannot be null"));
    this.createdBy = createdBy;
    this.fileDecryptor = fileDecryptor;
    this.encryptionType = encryptionType;
  }

  /**
   * @return the schema for the file
   */
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "FileMetaData{schema: " + schema + ", metadata: " + keyValueMetaData + "}";
  }

  /**
   * @return meta data for extensions
   */
  public Map<String, String> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  /**
   * @return the description of the library that created the file
   */
  public String getCreatedBy() {
    return createdBy;
  }

  public InternalFileDecryptor getFileDecryptor() {
    return fileDecryptor;
  }

  public EncryptionType getEncryptionType() {
    return encryptionType;
  }
}
