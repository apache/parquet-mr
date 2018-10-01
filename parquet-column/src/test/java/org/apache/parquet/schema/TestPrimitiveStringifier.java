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
package org.apache.parquet.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.schema.PrimitiveStringifier.DATE_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.DEFAULT_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.INTERVAL_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_NANOS_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.TIME_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.UNSIGNED_STRINGIFIER;
import static org.apache.parquet.schema.PrimitiveStringifier.UTF8_STRINGIFIER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestPrimitiveStringifier {

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  @Test
  public void testDefaultStringifier() {
    PrimitiveStringifier stringifier = DEFAULT_STRINGIFIER;

    assertEquals("true", stringifier.stringify(true));
    assertEquals("false", stringifier.stringify(false));

    assertEquals("0.0", stringifier.stringify(0.0));
    assertEquals("123456.7891234567", stringifier.stringify(123456.7891234567));
    assertEquals("-98765.43219876543", stringifier.stringify(-98765.43219876543));

    assertEquals("0.0", stringifier.stringify(0.0f));
    assertEquals("987.6543", stringifier.stringify(987.6543f));
    assertEquals("-123.4567", stringifier.stringify(-123.4567f));

    assertEquals("0", stringifier.stringify(0));
    assertEquals("1234567890", stringifier.stringify(1234567890));
    assertEquals("-987654321", stringifier.stringify(-987654321));

    assertEquals("0", stringifier.stringify(0l));
    assertEquals("1234567890123456789", stringifier.stringify(1234567890123456789l));
    assertEquals("-987654321987654321", stringifier.stringify(-987654321987654321l));

    assertEquals("null", stringifier.stringify(null));
    assertEquals("0x", stringifier.stringify(Binary.EMPTY));
    assertEquals("0x0123456789ABCDEF", stringifier.stringify(Binary.fromConstantByteArray(
        new byte[] { 0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF })));
  }

  @Test
  public void testUnsignedStringifier() {
    PrimitiveStringifier stringifier = UNSIGNED_STRINGIFIER;

    assertEquals("0", stringifier.stringify(0));
    assertEquals("2147483647", stringifier.stringify(2147483647));
    assertEquals("4294967295", stringifier.stringify(0xFFFFFFFF));

    assertEquals("0", stringifier.stringify(0l));
    assertEquals("9223372036854775807", stringifier.stringify(9223372036854775807l));
    assertEquals("18446744073709551615", stringifier.stringify(0xFFFFFFFFFFFFFFFFl));

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
  }

  @Test
  public void testUTF8Stringifier() {
    PrimitiveStringifier stringifier = UTF8_STRINGIFIER;

    assertEquals("null", stringifier.stringify(null));
    assertEquals("", stringifier.stringify(Binary.EMPTY));
    assertEquals("This is a UTF-8 test", stringifier.stringify(Binary.fromString("This is a UTF-8 test")));
    assertEquals("これはUTF-8のテストです",
        stringifier.stringify(Binary.fromConstantByteArray("これはUTF-8のテストです".getBytes(UTF_8))));

    checkThrowingUnsupportedException(stringifier, Binary.class);
  }

  @Test
  public void testIntervalStringifier() {
    PrimitiveStringifier stringifier = INTERVAL_STRINGIFIER;

    assertEquals("null", stringifier.stringify(null));

    assertEquals("<INVALID>", stringifier.stringify(Binary.EMPTY));
    assertEquals("<INVALID>",
        stringifier.stringify(Binary.fromConstantByteArray(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 })));
    assertEquals("<INVALID>",
        stringifier.stringify(Binary.fromReusedByteArray(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 })));

    ByteBuffer buffer = ByteBuffer.allocate(12);
    assertEquals("interval(0 months, 0 days, 0 millis)",
        stringifier.stringify(Binary.fromConstantByteBuffer(buffer)));

    buffer.putInt(0x03000000);
    buffer.putInt(0x06000000);
    buffer.putInt(0x09000000);
    buffer.flip();
    assertEquals("interval(3 months, 6 days, 9 millis)",
        stringifier.stringify(Binary.fromConstantByteBuffer(buffer)));

    buffer.clear();
    buffer.putInt(0xFFFFFFFF);
    buffer.putInt(0xFEFFFFFF);
    buffer.putInt(0xFDFFFFFF);
    buffer.flip();
    assertEquals("interval(4294967295 months, 4294967294 days, 4294967293 millis)",
        stringifier.stringify(Binary.fromReusedByteBuffer(buffer)));

    checkThrowingUnsupportedException(stringifier, Binary.class);
  }

  @Test
  public void testDateStringifier() {
    PrimitiveStringifier stringifier = DATE_STRINGIFIER;

    assertEquals("1970-01-01", stringifier.stringify(0));

    Calendar cal = Calendar.getInstance(UTC);
    cal.clear();
    cal.set(2017, Calendar.DECEMBER, 14);
    assertEquals("2017-12-14", stringifier.stringify((int) MILLISECONDS.toDays(cal.getTimeInMillis())));

    cal.clear();
    cal.set(1492, Calendar.AUGUST, 3);
    assertEquals("1492-08-03", stringifier.stringify((int) MILLISECONDS.toDays(cal.getTimeInMillis())));

    checkThrowingUnsupportedException(stringifier, Integer.TYPE);
  }

  @Test
  public void testTimestampMillisStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.TIMESTAMP_MILLIS_STRINGIFIER;

    assertEquals("1970-01-01T00:00:00.000", stringifier.stringify(0l));

    Calendar cal = Calendar.getInstance(UTC);
    cal.clear();
    cal.set(2017, Calendar.DECEMBER, 15, 10, 9, 54);
    cal.set(Calendar.MILLISECOND, 120);
    assertEquals("2017-12-15T10:09:54.120", stringifier.stringify(cal.getTimeInMillis()));

    cal.clear();
    cal.set(1948, Calendar.NOVEMBER, 23, 20, 19, 1);
    cal.set(Calendar.MILLISECOND, 9);
    assertEquals("1948-11-23T20:19:01.009", stringifier.stringify(cal.getTimeInMillis()));

    checkThrowingUnsupportedException(stringifier, Long.TYPE);
  }

  @Test
  public void testTimestampMicrosStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.TIMESTAMP_MICROS_STRINGIFIER;

    assertEquals("1970-01-01T00:00:00.000000", stringifier.stringify(0l));

    Calendar cal = Calendar.getInstance(UTC);
    cal.clear();
    cal.set(2053, Calendar.JULY, 10, 22, 13, 24);
    cal.set(Calendar.MILLISECOND, 84);
    long micros = cal.getTimeInMillis() * 1000 + 900;
    assertEquals("2053-07-10T22:13:24.084900", stringifier.stringify(micros));

    cal.clear();
    cal.set(1848, Calendar.MARCH, 15, 9, 23, 59);
    cal.set(Calendar.MILLISECOND, 765);
    micros = cal.getTimeInMillis() * 1000 - 1;
    assertEquals("1848-03-15T09:23:59.765001", stringifier.stringify(micros));

    checkThrowingUnsupportedException(stringifier, Long.TYPE);
  }

  @Test
  public void testTimestampNanosStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.TIMESTAMP_NANOS_STRINGIFIER;

    assertEquals("1970-01-01T00:00:00.000000000", stringifier.stringify(0l));

    Calendar cal = Calendar.getInstance(UTC);
    cal.clear();
    cal.set(2053, Calendar.JULY, 10, 22, 13, 24);
    cal.set(Calendar.MILLISECOND, 84);
    long micros = cal.getTimeInMillis() * 1000_000 + 1900;
    assertEquals("2053-07-10T22:13:24.084001900", stringifier.stringify(micros));

    cal.clear();
    cal.set(1848, Calendar.MARCH, 15, 9, 23, 59);
    cal.set(Calendar.MILLISECOND, 765);
    micros = cal.getTimeInMillis() * 1000_000 - 1;
    assertEquals("1848-03-15T09:23:59.765000001", stringifier.stringify(micros));

    checkThrowingUnsupportedException(stringifier, Long.TYPE);
  }

  @Test
  public void testTimeStringifier() {
    PrimitiveStringifier stringifier = TIME_STRINGIFIER;

    assertEquals("00:00:00.000", stringifier.stringify(0));
    assertEquals("00:00:00.000000", stringifier.stringify(0l));

    assertEquals("12:34:56.789", stringifier.stringify((int) convert(MILLISECONDS, 12, 34, 56, 789)));
    assertEquals("12:34:56.789012", stringifier.stringify(convert(MICROSECONDS, 12, 34, 56, 789012)));

    assertEquals("-12:34:56.789", stringifier.stringify((int) convert(MILLISECONDS, -12, -34, -56, -789)));
    assertEquals("-12:34:56.789012", stringifier.stringify(convert(MICROSECONDS, -12, -34, -56, -789012)));

    assertEquals("123:12:34.567", stringifier.stringify((int) convert(MILLISECONDS, 123, 12, 34, 567)));
    assertEquals("12345:12:34.056789", stringifier.stringify(convert(MICROSECONDS, 12345, 12, 34, 56789)));

    assertEquals("-123:12:34.567", stringifier.stringify((int) convert(MILLISECONDS, -123, -12, -34, -567)));
    assertEquals("-12345:12:34.056789", stringifier.stringify(convert(MICROSECONDS, -12345, -12, -34, -56789)));

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
  }

  @Test
  public void testTimeNanoStringifier() {
    PrimitiveStringifier stringifier = TIME_NANOS_STRINGIFIER;

    assertEquals("00:00:00.000000000", stringifier.stringify(0l));

    assertEquals("12:34:56.789012987", stringifier.stringify(convert(NANOSECONDS, 12, 34, 56, 789012987)));
    assertEquals("-12:34:56.000789012", stringifier.stringify(convert(NANOSECONDS, -12, -34, -56, -789012)));
    assertEquals("12345:12:34.000056789", stringifier.stringify(convert(NANOSECONDS, 12345, 12, 34, 56789)));
    assertEquals("-12345:12:34.000056789", stringifier.stringify(convert(NANOSECONDS, -12345, -12, -34, -56789)));

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE);
  }

  private long convert(TimeUnit unit, long hours, long minutes, long seconds, long rest) {
    return unit.convert(hours, HOURS) + unit.convert(minutes, MINUTES) + unit.convert(seconds, SECONDS) + rest;
  }

  @Test
  public void testDecimalStringifier() {
    PrimitiveStringifier stringifier = PrimitiveStringifier.createDecimalStringifier(4);

    assertEquals("0.0000", stringifier.stringify(0));
    assertEquals("123456.7890", stringifier.stringify(1234567890));
    assertEquals("-98765.4321", stringifier.stringify(-987654321));

    assertEquals("0.0000", stringifier.stringify(0l));
    assertEquals("123456789012345.6789", stringifier.stringify(1234567890123456789l));
    assertEquals("-98765432109876.5432", stringifier.stringify(-987654321098765432l));

    assertEquals("null", stringifier.stringify(null));
    assertEquals("<INVALID>", stringifier.stringify(Binary.EMPTY));
    assertEquals("0.0000", stringifier.stringify(Binary.fromReusedByteArray(new byte[] { 0 })));
    assertEquals("9876543210987654321098765432109876543210987654.3210", stringifier.stringify(Binary
        .fromConstantByteArray(new BigInteger("98765432109876543210987654321098765432109876543210").toByteArray())));
    assertEquals("-1234567890123456789012345678901234567890123456.7890", stringifier.stringify(Binary
        .fromConstantByteArray(new BigInteger("-12345678901234567890123456789012345678901234567890").toByteArray())));

    checkThrowingUnsupportedException(stringifier, Integer.TYPE, Long.TYPE, Binary.class);
  }

  private void checkThrowingUnsupportedException(PrimitiveStringifier stringifier, Class<?>... excludes) {
    Set<Class<?>> set = new HashSet<>(Arrays.asList(excludes));
    if (!set.contains(Integer.TYPE)) {
      try {
        stringifier.stringify(0);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (!set.contains(Long.TYPE)) {
      try {
        stringifier.stringify(0l);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (!set.contains(Float.TYPE)) {
      try {
        stringifier.stringify(0.0f);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (!set.contains(Double.TYPE)) {
      try {
        stringifier.stringify(0.0);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (!set.contains(Boolean.TYPE)) {
      try {
        stringifier.stringify(false);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
    if (!set.contains(Binary.class)) {
      try {
        stringifier.stringify(Binary.EMPTY);
        fail("An UnsupportedOperationException should have been thrown");
      } catch (UnsupportedOperationException e) {
      }
    }
  }

}
