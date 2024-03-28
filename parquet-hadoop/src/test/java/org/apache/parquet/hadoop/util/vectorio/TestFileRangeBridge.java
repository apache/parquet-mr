/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util.vectorio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link FileRangeBridge} class; requires
 * Hadoop 3.3.5+
 */
public class TestFileRangeBridge {

  private static final Logger LOG = LoggerFactory.getLogger(TestFileRangeBridge.class);

  /**
   * Class required for tests to run: {@value }.
   * Probed in setup.
   */
  static final String CLASSNAME = "org.apache.hadoop.fs.FileRange";

  @Before
  public void setUp() {

    // look for the FileRange; if not found, skip
    try {
      this.getClass().getClassLoader().loadClass(CLASSNAME);
    } catch (ReflectiveOperationException e) {
      assumeNoException(e);
    }
  }

  /**
   * Attempt to explicit instantiate the bridge;
   * package private constructor.
   */
  @Test
  public void testInstantiate() throws Throwable {
    new FileRangeBridge();
  }

  /**
   * Is the shared bridge instance non-null, and does the
   * {@link FileRangeBridge#bridgeAvailable()} predicate
   * return true.
   */
  @Test
  public void testFileRangeBridgeAvailable() throws Throwable {
    assertNotNull("FileRangeBridge instance null", FileRangeBridge.instance());
    assertTrue("Bridge not available", FileRangeBridge.bridgeAvailable());
  }

  /**
   * Create a range and validate the getters return the original values.
   */
  @Test
  public void testCreateFileRange() {
    Object reference = "backref";
    FileRangeBridge.WrappedFileRange range = FileRangeBridge.instance().createFileRange(512L, 16384, reference);
    LOG.info("created range {}", range);
    assertNotNull("null range", range);
    assertNotNull("null range instance", range.getFileRange());
    assertEquals("offset of " + range, 512L, range.getOffset());
    assertEquals("length of " + range, 16384, range.getLength());
    assertSame("backref of " + range, reference, range.getReference());

    // this isn't set until readVectored() is called
    assertNull("non-null range future", range.getData());
  }

  /**
   * Verify there is no parameter validation.
   */
  @Test
  public void testCreateInvalidRange() {
    FileRangeBridge.instance().createFileRange(-1, -1, null);
  }
}
