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

package org.apache.parquet.crypto.keytools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

/**
 * Parquet encryption specification defines "key metadata" as an arbitrary byte array, generated by file writers for each encryption key,
 * and passed to the low level API for storage in the file footer . The "key metadata" field is made available to file readers to enable
 * recovery of the key. This simple interface can be utilized for implementation of any key management scheme.
 * <p>
 * The keytools package (PARQUET-1373) implements one approach, of many possible, to key management and to generation of the "key metadata"
 * fields. This approach, based on the "envelope encryption" pattern, allows to work with KMS servers. It keeps the actual material,
 * required to recover a key, in a "key material" object (see the KeyMaterial class for details).
 * <p>
 * KeyMetadata class writes (and reads) the "key metadata" field as a flat json object, with the following fields:
 * 1. "keyMaterialType" - a String, with the type of  key material. In the current version, only one value is allowed - "PKMT1" (stands
 * for "parquet key management tools, version 1")
 * 2. "internalStorage" - a boolean. If true, means that "key material" is kept inside the "key metadata" field. If false, "key material"
 * is kept externally (outside Parquet files) - in this case, "key metadata" keeps a reference to the external "key material".
 * 3. "keyReference" - a String, with the reference to the external "key material". Written only if internalStorage is false.
 * <p>
 * If internalStorage is true, "key material" is a part of "key metadata", and the json keeps additional fields, described in the
 * KeyMaterial class.
 */
public class KeyMetadata {
  static final String KEY_MATERIAL_INTERNAL_STORAGE_FIELD = "internalStorage";
  private static final String KEY_REFERENCE_FIELD = "keyReference";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final boolean isInternalStorage;
  private final String keyReference;
  private final KeyMaterial keyMaterial;

  private KeyMetadata(boolean isInternalStorage, String keyReference, KeyMaterial keyMaterial) {
    this.isInternalStorage = isInternalStorage;
    this.keyReference = keyReference;
    this.keyMaterial = keyMaterial;
  }

  static KeyMetadata parse(byte[] keyMetadataBytes) {
    String keyMetaDataString = new String(keyMetadataBytes, StandardCharsets.UTF_8);
    Map<String, Object> keyMetadataJson = null;
    try {
      keyMetadataJson = OBJECT_MAPPER.readValue(
          new StringReader(keyMetaDataString), new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to parse key metadata " + keyMetaDataString, e);
    }

    // 1. Extract "key material type", and make sure it is supported
    String keyMaterialType = (String) keyMetadataJson.get(KeyMaterial.KEY_MATERIAL_TYPE_FIELD);
    if (!KeyMaterial.KEY_MATERIAL_TYPE1.equals(keyMaterialType)) {
      throw new ParquetCryptoRuntimeException(
          "Wrong key material type: " + keyMaterialType + " vs " + KeyMaterial.KEY_MATERIAL_TYPE1);
    }

    // 2. Check if "key material" is stored internally in Parquet file key metadata, or is stored externally
    Boolean isInternalStorage = (Boolean) keyMetadataJson.get(KEY_MATERIAL_INTERNAL_STORAGE_FIELD);
    String keyReference;
    KeyMaterial keyMaterial;

    if (isInternalStorage) {
      // 3.1 "key material" is stored internally, inside "key metadata" - parse it
      keyMaterial = KeyMaterial.parse(keyMetadataJson);
      keyReference = null;
    } else {
      // 3.2 "key material" is stored externally. "key metadata" keeps a reference to it
      keyReference = (String) keyMetadataJson.get(KEY_REFERENCE_FIELD);
      keyMaterial = null;
    }

    return new KeyMetadata(isInternalStorage, keyReference, keyMaterial);
  }

  // For external material only. For internal material, create serialized KeyMaterial directly
  static String createSerializedForExternalMaterial(String keyReference) {
    Map<String, Object> keyMetadataMap = new HashMap<String, Object>(3);
    // 1. Write "key material type"
    keyMetadataMap.put(KeyMaterial.KEY_MATERIAL_TYPE_FIELD, KeyMaterial.KEY_MATERIAL_TYPE1);
    // 2. Write internal storage as false
    keyMetadataMap.put(KEY_MATERIAL_INTERNAL_STORAGE_FIELD, Boolean.FALSE);
    // 3. For externally stored "key material", "key metadata" keeps only a reference to it
    keyMetadataMap.put(KEY_REFERENCE_FIELD, keyReference);

    try {
      return OBJECT_MAPPER.writeValueAsString(keyMetadataMap);
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to serialize key metadata", e);
    }
  }

  boolean keyMaterialStoredInternally() {
    return isInternalStorage;
  }

  KeyMaterial getKeyMaterial() {
    return keyMaterial;
  }

  String getKeyReference() {
    return keyReference;
  }
}
