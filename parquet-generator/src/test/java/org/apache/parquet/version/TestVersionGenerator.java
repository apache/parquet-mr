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
package org.apache.parquet.version;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import javax.tools.ToolProvider;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestVersionGenerator {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  @SuppressWarnings("resource")
  public void testVersionGenerator() throws Exception {
    File javaPackage = folder.newFolder("org", "apache", "parquet");
    File javaFile = new File(javaPackage, "Version.java");

    // Write the JAVA file out
    try (FileWriter fw = new FileWriter(javaFile)) {
      new VersionGenerator(fw).run();
    }

    // Compile the JAVA file
    ToolProvider.getSystemJavaCompiler().run(null, null, null,
        javaFile.getPath());

    // Load and instantiate compiled class.
    URLClassLoader classLoader = URLClassLoader
        .newInstance(new URL[] { folder.getRoot().toURI().toURL() });
    Class<?> cls =
        Class.forName("org.apache.parquet.Version", true, classLoader);
    Object instance = cls.newInstance();

    // Grab the version information as a String
    String actualToString = instance.toString();

    Properties props = loadProperties();
    String expectedToString = String.format("%s [%s]",
        props.getProperty("versionNumber"), props.getProperty("fullVersion"));

    Assert.assertEquals(expectedToString, actualToString);
  }

  private Properties loadProperties() throws IOException {
    final Properties properties = new Properties();
    try (final InputStream stream =
        this.getClass().getResourceAsStream("/parquet-version.properties")) {
      if (stream == null) {
        throw new IOException("/parquet-version.properties not found");
      }
      properties.load(stream);
    }
    return properties;
  }

}
