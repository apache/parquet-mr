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
package org.apache.parquet;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.parquet.VersionParser.ParsedVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test doesn't do much, but it makes sure that the Version class
 * was properly generated, and that it's VERSION_NUMBER field has been
 * populated correctly. The hope is to catch any issues like the version
 * being an empty string or something along those lines.
 */
public class VersionTest {

  private void assertVersionValid(String v) {
    try {
      org.semver.Version.parse(v);
    } catch (RuntimeException e) {
      throw new RuntimeException(v + " is not a valid semver!" , e);
    }
  }

  @Test
  public void testVersion() {
    assertVersionValid(Version.VERSION_NUMBER);
  }

  @Test
  public void testFullVersion() {
    ParsedVersion version = VersionParser.parse(Version.FULL_VERSION);

    assertVersionValid(version.semver);
    assertEquals(Version.VERSION_NUMBER, version.semver);
    assertEquals("parquet-mr", version.application);
  }
}
