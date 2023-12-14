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
package org.apache.parquet.thrift.struct;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.thrift.TBase;

/**
 * Commandline runner for compatibility checker
 * <p>
 * java CompatibilityRunner generate-json {category_name} {class_name} {dest_dir}
 * The above command will generate json representaion of thrift schema and store it as {dest_dir}/{category_name}.json
 * <p>
 * java CompatibilityRunner compare-json {old_json_path} {new_json_path}
 * The above command will succeed when the new schema is compatible with the old schema.
 * It will fail when they are not compatible. For compatibility rules: {@link CompatibilityChecker}
 */
public class CompatibilityRunner {
  public static void main(String[] args) throws Exception {
    LinkedList<String> arguments = new LinkedList<String>(Arrays.asList(args));
    String operator = arguments.pollFirst();
    if (operator.equals("generate-json")) {
      // java CompatibilityRunner generate-json tfe_request com.twitter.logs.TfeRequestLog old_json/
      generateJson(arguments);
    }

    if (operator.equals("compare-json")) {
      compareJson(arguments);
    }
  }

  private static void compareJson(LinkedList<String> arguments) throws IOException {
    String oldJsonPath = arguments.pollFirst();
    String newJsonPath = arguments.pollFirst();
    boolean allowEmptyStruct = "--allow-empty-struct".equals(arguments.pollFirst());

    File oldJsonFile = new File(oldJsonPath);
    checkExist(oldJsonFile);
    File newJsonFile = new File(newJsonPath);
    checkExist(newJsonFile);

    ObjectMapper mapper = new ObjectMapper();
    ThriftType.StructType oldStruct = mapper.readValue(oldJsonFile, ThriftType.StructType.class);
    ThriftType.StructType newStruct = mapper.readValue(newJsonFile, ThriftType.StructType.class);

    CompatibilityReport report = new CompatibilityChecker().checkCompatibility(oldStruct, newStruct);
    if (!report.isCompatible) {
      System.err.println("schema not compatible");
      System.err.println(report.getMessages());
      System.exit(1);
    }

    if (report.hasEmptyStruct()) {
      if (allowEmptyStruct) {
        System.err.println("WARNING: schema contains empty struct, but allowing because --allow-empty-struct was passed");
        System.err.println(report.getMessages());
      } else {
        System.err.println("schema contains empty struct");
        System.err.println(report.getMessages());
        System.exit(1);
      }
    }

    System.out.println("[success] schema is compatible");
  }

  private static void checkExist(File f) {
    if (!f.exists()) throw new RuntimeException("can not find file " + f);
  }

  private static void generateJson(LinkedList<String> arguments) throws ClassNotFoundException, IOException {
    String catName = arguments.pollFirst();
    String className = arguments.pollFirst();
    String storedPath = arguments.pollFirst();
    File storeDir = new File(storedPath);
    ThriftType.StructType structType =
        ThriftSchemaConverter.toStructType((Class<? extends TBase<?, ?>>) Class.forName(className));
    ObjectMapper mapper = new ObjectMapper();

    String fileName = catName + ".json";
    mapper.writerWithDefaultPrettyPrinter().writeValue(new File(storeDir, fileName), structType);
  }
}
