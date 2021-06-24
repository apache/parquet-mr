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
import java.io.Writer;
import java.util.Properties;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class VersionGenerator implements AutoCloseable {

  private final Writer writer;
  private final VelocityEngine velocityEngine;
  private final Template template;

  @Deprecated
  public VersionGenerator(File file) throws IOException {
    this(new FileWriter(file));
  }

  public VersionGenerator(Writer writer) throws IOException {
    this.writer = writer;

    this.velocityEngine = new VelocityEngine();
    this.velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER,
        "classpath");
    this.velocityEngine.setProperty("classpath.resource.loader.class",
        ClasspathResourceLoader.class.getName());
    this.velocityEngine.init();

    this.template = velocityEngine.getTemplate("/templates/version.vm");
  }

  public void run() throws IOException {
    final Properties properties = new Properties();
    try (final InputStream stream =
        this.getClass().getResourceAsStream("/parquet-version.properties")) {
      if (stream == null) {
        throw new IOException("/parquet-version.properties not found");
      }
      properties.load(stream);
    }

    VelocityContext context = new VelocityContext();
    context.put("fullVersion", properties.getProperty("fullVersion"));
    context.put("versionNumber", properties.getProperty("versionNumber"));

    this.template.merge(context, writer);

    close();
  }

  @Override
  public void close() throws IOException {
    this.writer.close();
  }

  @SuppressWarnings("resource")
  public static void main(String[] args) throws IOException {
    File srcFile = new File(args[0] + "/org/apache/parquet/Version.java");
    srcFile = srcFile.getAbsoluteFile();
    File parent = srcFile.getParentFile();
    if (!parent.exists()) {
      if (!parent.mkdirs()) {
        throw new IOException("Failed to mkdirs for " + parent);
      }
    }
    try (FileWriter fw = new FileWriter(srcFile)) {
      new VersionGenerator(fw).run();
    }
  }

}
