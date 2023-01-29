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
package org.apache.parquet.filter2.recordlevel;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class PhoneBookWriter {
  private static final String schemaString =
      "message user {\n"
          + "  required int64 id;\n"
          + "  optional binary name (UTF8);\n"
          + "  optional group location {\n"
          + "    optional double lon;\n"
          + "    optional double lat;\n"
          + "  }\n"
          + "  optional group phoneNumbers {\n"
          + "    repeated group phone {\n"
          + "      required int64 number;\n"
          + "      optional binary kind (UTF8);\n"
          + "    }\n"
          + "  }\n"
          + "}\n";

  private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

  public static class Location {
    private final Double lon;
    private final Double lat;

    public Location(Double lon, Double lat) {
      this.lon = lon;
      this.lat = lat;
    }

    public Double getLon() {
      return lon;
    }

    public Double getLat() {
      return lat;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Location location = (Location) o;

      if (lat != null ? !lat.equals(location.lat) : location.lat != null) return false;
      if (lon != null ? !lon.equals(location.lon) : location.lon != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = lon != null ? lon.hashCode() : 0;
      result = 31 * result + (lat != null ? lat.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "Location [lon=" + lon + ", lat=" + lat + "]";
    }
  }

  public static class PhoneNumber {
    private final long number;
    private final String kind;

    public PhoneNumber(long number, String kind) {
      this.number = number;
      this.kind = kind;
    }

    public long getNumber() {
      return number;
    }

    public String getKind() {
      return kind;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PhoneNumber that = (PhoneNumber) o;

      if (number != that.number) return false;
      if (kind != null ? !kind.equals(that.kind) : that.kind != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (number ^ (number >>> 32));
      result = 31 * result + (kind != null ? kind.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "PhoneNumber [number=" + number + ", kind=" + kind + "]";
    }
  }

  public static class User {
    private final long id;
    private final String name;
    private final List<PhoneNumber> phoneNumbers;
    private final Location location;

    public User(long id, String name, List<PhoneNumber> phoneNumbers, Location location) {
      this.id = id;
      this.name = name;
      this.phoneNumbers = phoneNumbers;
      this.location = location;
    }

    public long getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public List<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
    }

    public Location getLocation() {
      return location;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      User user = (User) o;

      if (id != user.id) return false;
      if (location != null ? !location.equals(user.location) : user.location != null) return false;
      if (name != null ? !name.equals(user.name) : user.name != null) return false;
      if (phoneNumbers != null ? !phoneNumbers.equals(user.phoneNumbers) : user.phoneNumbers != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (id ^ (id >>> 32));
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (phoneNumbers != null ? phoneNumbers.hashCode() : 0);
      result = 31 * result + (location != null ? location.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "User [id=" + id + ", name=" + name + ", phoneNumbers=" + phoneNumbers + ", location=" + location + "]";
    }

    public User cloneWithName(String name) {
      return new User(id, name, phoneNumbers, location);
    }
  }

  public static SimpleGroup groupFromUser(User user) {
    SimpleGroup root = new SimpleGroup(schema);
    root.append("id", user.getId());

    if (user.getName() != null) {
      root.append("name", user.getName());
    }

    if (user.getPhoneNumbers() != null) {
      Group phoneNumbers = root.addGroup("phoneNumbers");
      for (PhoneNumber number : user.getPhoneNumbers()) {
        Group phone = phoneNumbers.addGroup("phone");
        phone.append("number", number.getNumber());
        if (number.getKind() != null) {
          phone.append("kind", number.getKind());
        }
      }
    }

    if (user.getLocation() != null) {
      Group location = root.addGroup("location");
      if (user.getLocation().getLon() != null) {
        location.append("lon", user.getLocation().getLon());
      }
      if (user.getLocation().getLat() != null) {
        location.append("lat", user.getLocation().getLat());
      }
    }
    return root;
  }

  private static User userFromGroup(Group root) {
    return new User(getLong(root, "id"), getString(root, "name"), getPhoneNumbers(getGroup(root, "phoneNumbers")),
        getLocation(getGroup(root, "location")));
  }

  private static List<PhoneNumber> getPhoneNumbers(Group phoneNumbers) {
    if (phoneNumbers == null) {
      return null;
    }
    List<PhoneNumber> list = new ArrayList<>();
    for (int i = 0, n = phoneNumbers.getFieldRepetitionCount("phone"); i < n; ++i) {
      Group phone = phoneNumbers.getGroup("phone", i);
      list.add(new PhoneNumber(getLong(phone, "number"), getString(phone, "kind")));
    }
    return list;
  }

  private static Location getLocation(Group location) {
    if (location == null) {
      return null;
    }
    return new Location(getDouble(location, "lon"), getDouble(location, "lat"));
  }

  private static boolean isNull(Group group, String field) {
    // Use null value if the field is not in the group schema
    if (!group.getType().containsField(field)) {
      return true;
    }
    int repetition = group.getFieldRepetitionCount(field);
    if (repetition == 0) {
      return true;
    } else if (repetition == 1) {
      return false;
    }
    throw new AssertionError("Invalid repetitionCount " + repetition + " for field " + field + " in group " + group);
  }

  private static Long getLong(Group group, String field) {
    return isNull(group, field) ? null : group.getLong(field, 0);
  }

  private static String getString(Group group, String field) {
    return isNull(group, field) ? null : group.getString(field, 0);
  }

  private static Double getDouble(Group group, String field) {
    return isNull(group, field) ? null : group.getDouble(field, 0);
  }

  private static Group getGroup(Group group, String field) {
    return isNull(group, field) ? null : group.getGroup(field, 0);
  }

  public static File writeToFile(List<User> users) throws IOException {
    File f = File.createTempFile("phonebook", ".parquet");
    f.deleteOnExit();
    if (!f.delete()) {
      throw new IOException("couldn't delete tmp file" + f);
    }

    writeToFile(f, users);

    return f;
  }

  public static void writeToFile(File f, List<User> users) throws IOException {
    write(ExampleParquetWriter.builder(new Path(f.getAbsolutePath())), users);
  }

  public static void write(ParquetWriter.Builder<Group, ?> builder, List<User> users) throws IOException {
    builder.config(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    try (ParquetWriter<Group> writer = builder.build()) {
      for (User u : users) {
        writer.write(groupFromUser(u));
      }
    }
  }

  public static ParquetReader<Group> createReader(Path file, Filter filter) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    return ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .withFilter(filter)
        .build();
  }

  public static List<Group> readFile(File f, Filter filter) throws IOException {
    ParquetReader<Group> reader = createReader(new Path(f.getAbsolutePath()), filter);

    Group current;
    List<Group> users = new ArrayList<Group>();

    current = reader.read();
    while (current != null) {
      users.add(current);
      current = reader.read();
    }

    return users;
  }

  public static List<User> readUsers(ParquetReader.Builder<Group> builder) throws IOException {
    return readUsers(builder, false);
  }

  /**
   * Returns a list of users from the underlying [[ParquetReader]] builder.
   * If `validateRowIndexes` is set to true, this method will also validate the ROW_INDEXes for the
   * rows read from ParquetReader - ROW_INDEX for a row should be same as underlying user id.
   */
  public static List<User> readUsers(ParquetReader.Builder<Group> builder, boolean validateRowIndexes) throws IOException {
    ParquetReader<Group> reader = builder.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString()).build();

    List<User> users = new ArrayList<>();
    for (Group group = reader.read(); group != null; group = reader.read()) {
      User u = userFromGroup(group);
      users.add(u);
      if (validateRowIndexes) {
        assertEquals("Row index should be equal to User id", u.id, reader.getCurrentRowIndex());
      }
    }
    return users;
  }

  public static void main(String[] args) throws IOException {
    File f = new File(args[0]);
    writeToFile(f, TestRecordLevelFilters.makeUsers());
  }

}
