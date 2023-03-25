/**
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

import org.junit.Assert;
import org.junit.Test;

public class TestPreconditions {

  @Test
  public void testCheckArgumentWithoutParams() {
    try {
      Preconditions.checkArgument(true, "Test message");
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message");
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Should format message",
          "Test message", e.getMessage());
    }
  }

  @Test
  public void testCheckArgumentWithOneParam() {
    try {
      Preconditions.checkArgument(true, "Test message %s", 12);
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message %s", 12);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Should format message",
          "Test message 12", e.getMessage());
    }
  }

  @Test
  public void testCheckArgumentWithTwoParams() {
    try {
      Preconditions.checkArgument(true, "Test message %s %s", 12, null);
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message %s %s", 12, null);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null", e.getMessage());
    }
  }

  @Test
  public void testCheckArgumentWithThreeParams() {
    try {
      Preconditions.checkArgument(true, "Test message %s %s %s", 12, null, "column");
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message %s %s %s", 12, null, "column");
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null column", e.getMessage());
    }
  }

  @Test
  public void testCheckArgumentWithMoreThanThreeParams() {
    try {
      Preconditions.checkArgument(true, "Test message %s %s %s %s", 12, null, "column", true);
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message %s %s %s %s", 12, null, "column", true);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null column true", e.getMessage());
    }
  }

  @Test
  public void checkArgumentMessageOnlySupportsStringTypeTemplate() {
    try {
      Preconditions.checkArgument(true, "Test message %d", 12);
    } catch (IllegalArgumentException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkArgument(false, "Test message %d", 12);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("%d is not a valid format option",
          "d != java.lang.String", e.getMessage());
    }
  }  

  @Test
  public void testCheckState() {
    try {
      Preconditions.checkState(true, "Test message: %s %s", 12, null);
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message: %s %s", 12, null);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message: 12 null", e.getMessage());
    }
  }

  @Test
  public void testCheckStateWithoutArguments() {
    try {
      Preconditions.checkState(true, "Test message");
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message");
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message", e.getMessage());
    }
  }

  @Test
  public void testCheckStateWithOneArgument() {
    try {
      Preconditions.checkState(true, "Test message %s", 12);
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message %s", 12);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message 12", e.getMessage());
    }
  }

  @Test
  public void testCheckStateWithTwoArguments() {
    try {
      Preconditions.checkState(true, "Test message %s %s", 12, null);
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message %s %s", 12, null);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null", e.getMessage());
    }
  }

  @Test
  public void testCheckStateWithThreeArguments() {
    try {
      Preconditions.checkState(true, "Test message %s %s %s", 12, null, "column");
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message %s %s %s", 12, null, "column");
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null column", e.getMessage());
    }
  }

  @Test
  public void testCheckStateWithMoreThanThreeParams() {
    try {
      Preconditions.checkState(true, "Test message %s %s %s %s", 12, null, "column", true);
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message %s %s %s %s", 12, null, "column", true);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Should format message",
          "Test message 12 null column true", e.getMessage());
    }
  }  

  @Test
  public void checkStateMessageOnlySupportsStringTypeTemplate() {
    try {
      Preconditions.checkState(true, "Test message %d", 12);
    } catch (IllegalStateException e) {
      Assert.fail("Should not throw exception when isValid is true");
    }

    try {
      Preconditions.checkState(false, "Test message %d", 12);
      Assert.fail("Should throw exception when isValid is false");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("%d is not a valid format option",
          "d != java.lang.String", e.getMessage());
    }
  }  
}
