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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import javax.naming.OperationNotSupportedException;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/**
 * Class that provides string representations for the primitive values. These string values are to be used for
 * logging/debugging purposes. The method {@code stringify} is overloaded for each primitive types. The overloaded
 * methods not implemented for the related types throw {@link OperationNotSupportedException}.
 */
public abstract class PrimitiveStringifier {
  private final String name;

  private PrimitiveStringifier(String name) {
    this.name = name;
  }

  @Override
  public final String toString() {
    return name;
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(boolean value) {
    throw new UnsupportedOperationException(
        "stringify(boolean) was called on a non-boolean stringifier: " + toString());
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(int value) {
    throw new UnsupportedOperationException("stringify(int) was called on a non-int stringifier: " + toString());
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(long value) {
    throw new UnsupportedOperationException("stringify(long) was called on a non-long stringifier: " + toString());
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(float value) {
    throw new UnsupportedOperationException(
        "stringify(float) was called on a non-float stringifier: " + toString());
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(double value) {
    throw new UnsupportedOperationException(
        "stringify(double) was called on a non-double stringifier: " + toString());
  }

  /**
   * @param value the value to be stringified
   * @return the string representation for {@code value}
   * @throws UnsupportedOperationException if value type is not supported by this stringifier
   */
  public String stringify(Binary value) {
    throw new UnsupportedOperationException(
        "stringify(Binary) was called on a non-Binary stringifier: " + toString());
  }

  private static final String BINARY_NULL = "null";
  private static final String BINARY_HEXA_PREFIX = "0x";
  private static final String BINARY_INVALID = "<INVALID>";

  abstract static class BinaryStringifierBase extends PrimitiveStringifier {
    private BinaryStringifierBase(String name) {
      super(name);
    }

    @Override
    public final String stringify(Binary value) {
      return value == null ? BINARY_NULL : stringifyNotNull(value);
    }

    abstract String stringifyNotNull(Binary value);
  }

  static final PrimitiveStringifier DEFAULT_STRINGIFIER = new BinaryStringifierBase("DEFAULT_STRINGIFIER") {
    private final char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    @Override
    public String stringify(boolean value) {
      return Boolean.toString(value);
    }

    @Override
    public String stringify(int value) {
      return Integer.toString(value);
    }

    @Override
    public String stringify(long value) {
      return Long.toString(value);
    }

    @Override
    public String stringify(float value) {
      return Float.toString(value);
    }

    @Override
    public String stringify(double value) {
      return Double.toString(value);
    }

    @Override
    String stringifyNotNull(Binary value) {
      ByteBuffer buffer = value.toByteBuffer();
      StringBuilder builder = new StringBuilder(2 + buffer.remaining() * 2);
      builder.append(BINARY_HEXA_PREFIX);
      for (int i = buffer.position(), n = buffer.limit(); i < n; ++i) {
        byte b = buffer.get(i);
        builder.append(digits[(b >>> 4) & 0x0F]);
        builder.append(digits[b & 0x0F]);
      }
      return builder.toString();
    }
  };

  static final PrimitiveStringifier UNSIGNED_STRINGIFIER = new PrimitiveStringifier("UNSIGNED_STRINGIFIER") {
    private static final long INT_MASK = 0x00000000FFFFFFFFl;

    // Implemented based on com.google.common.primitives.UnsignedInts.toString(int, int)
    @Override
    public String stringify(int value) {
      return Long.toString(value & INT_MASK);
    }

    // Implemented based on com.google.common.primitives.UnsignedLongs.toString(long, int)
    @Override
    public String stringify(long value) {
      if (value == 0) {
        // Simply return "0"
        return "0";
      } else if (value > 0) {
        return Long.toString(value);
      } else {
        char[] buf = new char[64];
        int i = buf.length;
        // Split x into high-order and low-order halves.
        // Individual digits are generated from the bottom half into which
        // bits are moved continuously from the top half.
        long top = value >>> 32;
        long bot = (value & INT_MASK) + ((top % 10) << 32);
        top /= 10;
        while ((bot > 0) || (top > 0)) {
          buf[--i] = Character.forDigit((int) (bot % 10), 10);
          bot = (bot / 10) + ((top % 10) << 32);
          top /= 10;
        }
        // Generate string
        return new String(buf, i, buf.length - i);
      }
    }
  };

  static final PrimitiveStringifier UTF8_STRINGIFIER = new BinaryStringifierBase("UTF8_STRINGIFIER") {
    @Override
    String stringifyNotNull(Binary value) {
      return value.toStringUsingUTF8();
    }
  };

  static final PrimitiveStringifier INTERVAL_STRINGIFIER = new BinaryStringifierBase("INTERVAL_STRINGIFIER") {
    @Override
    String stringifyNotNull(Binary value) {
      if (value.length() != 12) {
        return BINARY_INVALID;
      }
      ByteBuffer buffer = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      int pos = buffer.position();
      String months = UNSIGNED_STRINGIFIER.stringify(buffer.getInt(pos));
      String days = UNSIGNED_STRINGIFIER.stringify(buffer.getInt(pos + 4));
      String millis = UNSIGNED_STRINGIFIER.stringify(buffer.getInt(pos + 8));
      return "interval(" + months + " months, " + days + " days, " + millis + " millis)";
    }
  };

  private static class DateStringifier extends PrimitiveStringifier {
    private final DateTimeFormatter formatter;

    private DateStringifier(String name, String format) {
      super(name);
      formatter = DateTimeFormatter.ofPattern(format).withZone(ZoneOffset.UTC);
    }

    @Override
    public String stringify(int value) {
      return toFormattedString(getInstant(value));
    }

    @Override
    public String stringify(long value) {
      return toFormattedString(getInstant(value));
    }

    private String toFormattedString(Instant instant) {
      return formatter.format(instant);
    }

    Instant getInstant(int value) {
      // throw the related unsupported exception
      super.stringify(value);
      return null;
    }

    Instant getInstant(long value) {
      // throw the related unsupported exception
      super.stringify(value);
      return null;
    }
  }

  static final PrimitiveStringifier DATE_STRINGIFIER = new DateStringifier("DATE_STRINGIFIER", "yyyy-MM-dd") {
    @Override
    Instant getInstant(int value) {
      return Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(value));
    }
    ;
  };

  static final PrimitiveStringifier TIMESTAMP_MILLIS_STRINGIFIER =
      new DateStringifier("TIMESTAMP_MILLIS_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSS") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochMilli(value);
        }
      };

  static final PrimitiveStringifier TIMESTAMP_MICROS_STRINGIFIER =
      new DateStringifier("TIMESTAMP_MICROS_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochSecond(
              MICROSECONDS.toSeconds(value), MICROSECONDS.toNanos(value % SECONDS.toMicros(1)));
        }
      };

  static final PrimitiveStringifier TIMESTAMP_NANOS_STRINGIFIER =
      new DateStringifier("TIMESTAMP_NANOS_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochSecond(
              NANOSECONDS.toSeconds(value), NANOSECONDS.toNanos(value % SECONDS.toNanos(1)));
        }
      };

  static final PrimitiveStringifier TIMESTAMP_MILLIS_UTC_STRINGIFIER =
      new DateStringifier("TIMESTAMP_MILLIS_UTC_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSSZ") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochMilli(value);
        }
      };

  static final PrimitiveStringifier TIMESTAMP_MICROS_UTC_STRINGIFIER =
      new DateStringifier("TIMESTAMP_MICROS_UTC_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochSecond(
              MICROSECONDS.toSeconds(value), MICROSECONDS.toNanos(value % SECONDS.toMicros(1)));
        }
      };

  static final PrimitiveStringifier TIMESTAMP_NANOS_UTC_STRINGIFIER =
      new DateStringifier("TIMESTAMP_NANOS_UTC_STRINGIFIER", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZ") {
        @Override
        Instant getInstant(long value) {
          return Instant.ofEpochSecond(
              NANOSECONDS.toSeconds(value), NANOSECONDS.toNanos(value % SECONDS.toNanos(1)));
        }
      };

  private abstract static class TimeStringifier extends PrimitiveStringifier {
    private final boolean withZone;

    TimeStringifier(String name, boolean withZone) {
      super(name);
      this.withZone = withZone;
    }

    protected String toTimeString(long duration, TimeUnit unit) {
      String additionalFormat = (unit == MILLISECONDS ? "3d" : unit == MICROSECONDS ? "6d" : "9d");
      String timeZone = withZone ? "+0000" : "";
      String format = "%02d:%02d:%02d.%0" + additionalFormat + timeZone;
      return String.format(
          format,
          unit.toHours(duration),
          convert(duration, unit, MINUTES, HOURS),
          convert(duration, unit, SECONDS, MINUTES),
          convert(duration, unit, unit, SECONDS));
    }

    protected long convert(long duration, TimeUnit from, TimeUnit to, TimeUnit higher) {
      return Math.abs(to.convert(duration, from) % to.convert(1, higher));
    }
  }

  static final PrimitiveStringifier TIME_STRINGIFIER = new TimeStringifier("TIME_STRINGIFIER", false) {
    @Override
    public String stringify(int millis) {
      return toTimeString(millis, MILLISECONDS);
    }

    @Override
    public String stringify(long micros) {
      return toTimeString(micros, MICROSECONDS);
    }
  };

  static final PrimitiveStringifier TIME_NANOS_STRINGIFIER = new TimeStringifier("TIME_NANOS_STRINGIFIER", false) {
    @Override
    public String stringify(long nanos) {
      return toTimeString(nanos, NANOSECONDS);
    }
  };

  static final PrimitiveStringifier TIME_UTC_STRINGIFIER = new TimeStringifier("TIME_UTC_STRINGIFIER", true) {
    @Override
    public String stringify(int millis) {
      return toTimeString(millis, MILLISECONDS);
    }

    @Override
    public String stringify(long micros) {
      return toTimeString(micros, MICROSECONDS);
    }
  };

  static final PrimitiveStringifier TIME_NANOS_UTC_STRINGIFIER =
      new TimeStringifier("TIME_NANOS_UTC_STRINGIFIER", true) {
        @Override
        public String stringify(long nanos) {
          return toTimeString(nanos, NANOSECONDS);
        }
      };

  static PrimitiveStringifier createDecimalStringifier(final int scale) {
    return new BinaryStringifierBase("DECIMAL_STRINGIFIER(scale: " + scale + ")") {
      @Override
      public String stringify(int value) {
        return stringifyWithScale(BigInteger.valueOf(value));
      }

      @Override
      public String stringify(long value) {
        return stringifyWithScale(BigInteger.valueOf(value));
      }

      @Override
      String stringifyNotNull(Binary value) {
        try {
          return stringifyWithScale(new BigInteger(value.getBytesUnsafe()));
        } catch (NumberFormatException e) {
          return BINARY_INVALID;
        }
      }

      private String stringifyWithScale(BigInteger i) {
        return new BigDecimal(i, scale).toString();
      }
    };
  }

  static final PrimitiveStringifier UUID_STRINGIFIER = new PrimitiveStringifier("UUID_STRINGIFIER") {
    private final char[] digit = "0123456789abcdef".toCharArray();

    @Override
    public String stringify(Binary value) {
      byte[] bytes = value.getBytesUnsafe();
      StringBuilder builder = new StringBuilder(36);
      appendHex(bytes, 0, 4, builder);
      builder.append('-');
      appendHex(bytes, 4, 2, builder);
      builder.append('-');
      appendHex(bytes, 6, 2, builder);
      builder.append('-');
      appendHex(bytes, 8, 2, builder);
      builder.append('-');
      appendHex(bytes, 10, 6, builder);
      return builder.toString();
    }

    private void appendHex(byte[] array, int offset, int length, StringBuilder builder) {
      for (int i = offset, n = offset + length; i < n; ++i) {
        int value = array[i] & 0xff;
        builder.append(digit[value >>> 4]).append(digit[value & 0x0f]);
      }
    }
  };

  static final PrimitiveStringifier FLOAT16_STRINGIFIER = new BinaryStringifierBase("FLOAT16_STRINGIFIER") {

    @Override
    String stringifyNotNull(Binary value) {
      return Float16.toFloatString(value);
    }
  };

  static final PrimitiveStringifier WKB_STRINGIFIER = new BinaryStringifierBase("WKB_STRINGIFIER") {

    @Override
    String stringifyNotNull(Binary value) {

      Geometry geometry;
      try {
        WKBReader reader = new WKBReader();
        geometry = reader.read(value.getBytesUnsafe());
        return geometry.toText();
      } catch (ParseException e) {
        return BINARY_INVALID;
      }
    }
  };
}
