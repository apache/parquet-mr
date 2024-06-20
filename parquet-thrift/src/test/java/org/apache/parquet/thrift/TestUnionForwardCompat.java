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
package org.apache.parquet.thrift;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.parquet.thrift.test.compat.AString;
import org.apache.parquet.thrift.test.compat.EmptyStruct;
import org.apache.parquet.thrift.test.compat.TestForwardCompatRootV0;
import org.apache.parquet.thrift.test.compat.TestForwardCompatRootV1;
import org.apache.parquet.thrift.test.compat.TestForwardCompatRootV2;
import org.apache.parquet.thrift.test.compat.TestForwardCompatUnionV1;
import org.apache.parquet.thrift.test.compat.TestForwardCompatUnionV2;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import junit.framework.Assert;

public class TestUnionForwardCompat {



  @Test
  public void testForward() throws TException, ReflectiveOperationException {
    ByteArrayOutputStream out0 = new ByteArrayOutputStream();
    TestForwardCompatRootV0 v0 = new TestForwardCompatRootV0();
    v0.setStr("foo");
    v0.write(protocol(out0));

    {
      ByteArrayOutputStream out1 = new ByteArrayOutputStream();
      TestForwardCompatRootV1 v1 = read(TestForwardCompatRootV1.class, out0);
      Assert.assertEquals(v0.getStr(), v1.getStr());
      Assert.assertNull(v1.getU());

      v1.setU(TestForwardCompatUnionV1.empty1(new EmptyStruct()));
      Assert.assertTrue(v1.getU().isSetEmpty1());
      v1.write(protocol(out1));

      TestForwardCompatRootV2 v2 = read(TestForwardCompatRootV2.class, out1);
      Assert.assertEquals(v1.getStr(), v2.getStr());
      Assert.assertTrue(v2.getU().isSetEmpty1());

    }

    {
      TestForwardCompatRootV2 v2 = new TestForwardCompatRootV2();
      v2.read(protocol(out0.toByteArray()));
      Assert.assertEquals(v0.getStr(), v2.getStr());
      Assert.assertNull(v2.getU());
    }
  }

  @Test
  public void testBackwardV1ToV0() throws TException, ReflectiveOperationException {
    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
    TestForwardCompatRootV1 v1 = new TestForwardCompatRootV1();
    v1.setStr("foo");
    v1.setU(TestForwardCompatUnionV1.empty1(new EmptyStruct()));
    v1.write(protocol(out1));

    TestForwardCompatRootV0 v0 = read(TestForwardCompatRootV0.class, out1);
    Assert.assertEquals(v0.getStr(), v1.getStr());
  }

  @Test
  public void testBackwardV2ToV1() throws TException, ReflectiveOperationException {
    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    TestForwardCompatRootV2 v2 = new TestForwardCompatRootV2();
    v2.setStr("foo");
    v2.setU(TestForwardCompatUnionV2.struct2(new AString("foo")));
    v2.write(protocol(out2));

    TestForwardCompatRootV1 v1 = read(TestForwardCompatRootV1.class, out2);
    Assert.assertEquals(v2.getStr(), v1.getStr());
    Assert.assertNull(v1.getU().getFieldValue());
    Assert.assertNull(v1.getU().getSetField());
  }

  private <T extends TBase<T, ?>> T read(Class<T> c, ByteArrayOutputStream buf) throws TException, ReflectiveOperationException {
    T t = c.newInstance();
    t.read(protocol(buf.toByteArray()));
    return t;
  }

  private TProtocol protocol(byte[] byteArray) {
    return protocol(new ByteArrayInputStream(byteArray));
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }
}
