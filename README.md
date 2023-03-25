<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

Parquet MR [![Build Status](https://github.com/apache/parquet-mr/workflows/Test/badge.svg)](https://github.com/apache/parquet-mr/actions)
======

Parquet-MR contains the java implementation of the [Parquet format](https://github.com/apache/parquet-format).
Parquet is a columnar storage format for Hadoop; it provides efficient storage and encoding of data.
Parquet uses the [record shredding and assembly algorithm](https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) described in the Dremel paper to represent nested structures.

You can find some details about the format and intended use cases in our [Hadoop Summit 2013 presentation](http://www.slideshare.net/julienledem/parquet-hadoop-summit-2013)

## Building

Parquet-MR uses Maven to build and depends on the thrift compiler (protoc is now managed by maven plugin).

### Install Thrift

To build and install the thrift compiler, run:

```
wget -nv http://archive.apache.org/dist/thrift/0.16.0/thrift-0.16.0.tar.gz
tar xzf thrift-0.16.0.tar.gz
cd thrift-0.16.0
chmod +x ./configure
./configure --disable-libs
sudo make install
```

If you're on OSX and use homebrew, you can instead install Thrift 0.16.0 with `brew` and ensure that it comes first in your `PATH`.

```
brew install thrift
export PATH="/usr/local/opt/thrift@0.16.0/bin:$PATH"
```

### Build Parquet with Maven

Once protobuf and thrift are available in your path, you can build the project by running:

```
LC_ALL=C mvn clean install
```

## Features

Parquet is a very active project, and new features are being added quickly. Here are a few features:


* Type-specific encoding
* Hive integration (deprecated)
* Pig integration
* Cascading integration (deprecated)
* Crunch integration
* Apache Arrow integration
* Scrooge integration (deprecated)
* Impala integration (non-nested)
* Java Map/Reduce API
* Native Avro support
* Native Thrift support
* Native Protocol Buffers support
* Complex structure support
* Run-length encoding (RLE)
* Bit Packing
* Adaptive dictionary encoding
* Predicate pushdown
* Column stats
* Delta encoding
* Index pages
* Java Vector API support (experimental)

## Java Vector API support
`The feature is experimental and is currently not part of the parquet distribution`.
Parquet-MR has supported Java Vector API to speed up reading, to enable this feature:
* Java 17+, 64-bit
* Requiring the CPU to support instruction sets:
  * avx512vbmi
  * avx512_vbmi2
* To build the jars: `mvn clean package -P vector-plugins`
* For Apache Spark to enable this feature:
  * Build parquet and replace the parquet-encoding-{VERSION}.jar on the spark jars folder
  * Build parquet-encoding-vector and copy parquet-encoding-vector-{VERSION}.jar to the spark jars folder
  * Edit spark class#VectorizedRleValuesReader, function#readNextGroup refer to parquet class#ParquetReadRouter, function#readBatchUsing512Vector
  * Build spark with maven and replace spark-sql_2.12-{VERSION}.jar on the spark jars folder

## Map/Reduce integration

[Input](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetInputFormat.java) and [Output](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetOutputFormat.java) formats.
Note that to use an Input or Output format, you need to implement a WriteSupport or ReadSupport class, which will implement the conversion of your object to and from a Parquet schema.

We've implemented this for 2 popular data formats to provide a clean migration path as well:

### Thrift
Thrift integration is provided by the [parquet-thrift](https://github.com/apache/parquet-mr/tree/master/parquet-thrift) sub-project.

### Avro
Avro conversion is implemented via the [parquet-avro](https://github.com/apache/parquet-mr/tree/master/parquet-avro) sub-project.

### Protobuf
Protobuf conversion is implemented via the [parquet-protobuf](https://github.com/apache/parquet-mr/tree/master/parquet-protobuf) sub-project.

### Create your own objects
* The ParquetOutputFormat can be provided a WriteSupport to write your own objects to an event based RecordConsumer.
* the ParquetInputFormat can be provided a ReadSupport to materialize your own objects by implementing a RecordMaterializer

See the APIs:
* [Record conversion API](https://github.com/apache/parquet-mr/tree/master/parquet-column/src/main/java/org/apache/parquet/io/api)
* [Hadoop API](https://github.com/apache/parquet-mr/tree/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/api)

## Apache Pig integration
A [Loader](https://github.com/apache/parquet-mr/blob/master/parquet-pig/src/main/java/org/apache/parquet/pig/ParquetLoader.java) and a [Storer](https://github.com/apache/parquet-mr/blob/master/parquet-pig/src/main/java/org/apache/parquet/pig/ParquetStorer.java) are provided to read and write Parquet files with Apache Pig

Storing data into Parquet in Pig is simple:
```
-- options you might want to fiddle with
SET parquet.page.size 1048576 -- default. this is your min read/write unit.
SET parquet.block.size 134217728 -- default. your memory budget for buffering data
SET parquet.compression lzo -- or you can use none, gzip, snappy
STORE mydata into '/some/path' USING parquet.pig.ParquetStorer;
```
Reading in Pig is also simple:
```
mydata = LOAD '/some/path' USING parquet.pig.ParquetLoader();
```

If the data was stored using Pig, things will "just work". If the data was stored using another method, you will need to provide the Pig schema equivalent to the data you stored (you can also write the schema to the file footer while writing it -- but that's pretty advanced). We will provide a basic automatic schema conversion soon.

## Hive integration

Hive integration is provided via the [parquet-hive](https://github.com/apache/parquet-mr/tree/master/parquet-hive) sub-project.

Hive integration is now deprecated within the Parquet project. It is now maintained by Apache Hive.

## Build

To run the unit tests: `mvn test`

To build the jars: `mvn package`

The build runs in [GitHub Actions](https://github.com/apache/parquet-mr/actions):
[![Build Status](https://github.com/apache/parquet-mr/workflows/Test/badge.svg)](https://github.com/apache/parquet-mr/actions)

## Add Parquet as a dependency in Maven
The current release is version `1.12.0`

```xml
  <dependencies>
    <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-common</artifactId>
      <version>1.12.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-encoding</artifactId>
      <version>1.12.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-column</artifactId>
      <version>1.12.0</version>
    </dependency>
    <dependency>
      <groupId>org.apache.parquet</groupId>
      <artifactId>parquet-hadoop</artifactId>
      <version>1.12.0</version>
    </dependency>
  </dependencies>
```

### How To Contribute

We prefer to receive contributions in the form of GitHub pull requests. Please send pull requests against the [parquet-mr](https://github.com/apache/parquet-mr) Git repository. If you've previously forked Parquet from its old location, you will need to add a remote or update your origin remote to https://github.com/apache/parquet-mr.git

If you are looking for some ideas on what to contribute, check out jira issues for this project labeled ["pick-me-up"](https://issues.apache.org/jira/browse/PARQUET-5?jql=project%20%3D%20PARQUET%20and%20labels%20%3D%20pick-me-up%20and%20status%20%3D%20open).
Comment on the issue and/or contact [dev@parquet.apache.org](http://mail-archives.apache.org/mod_mbox/parquet-dev/) with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, you can still post it to our [issue tracker](https://issues.apache.org/jira/browse/PARQUET), or email the mailing list [dev@parquet.apache.org](http://mail-archives.apache.org/mod_mbox/parquet-dev/)

To contribute a patch:

  1. Break your work into small, single-purpose patches if possible. It’s much harder to merge in a large change with a lot of disjoint features.
  2. Create a JIRA for your patch on the [Parquet Project JIRA](https://issues.apache.org/jira/browse/PARQUET).
  3. Submit the patch as a GitHub pull request against the master branch. For a tutorial, see the GitHub guides on forking a repo and sending a pull request. Prefix your pull request name with the JIRA name (ex: https://github.com/apache/parquet-mr/pull/240).
  4. Make sure that your code passes the unit tests. You can run the tests with `mvn test` in the root directory.
  5. Add new unit tests for your code.

We tend to do fairly close readings of pull requests, and you may get a lot of comments. Some common issues that are not code structure related, but still important:
  * Use 2 spaces for whitespace. Not tabs, not 4 spaces. The number of the spacing shall be 2.
  * Give your operators some room. Not `a+b` but `a + b` and not `foo(int a,int b)` but `foo(int a, int b)`.
  * Generally speaking, stick to the [Sun Java Code Conventions](http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html)
  * Make sure tests pass!

Thank you for getting involved!

## Authors and contributors

* [Contributors](https://github.com/apache/parquet-mr/graphs/contributors)
* [Committers](dev/COMMITTERS.md)

## Code of Conduct

We hold ourselves and the Parquet developer community to two codes of conduct:
  1. [The Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html)
  2. [The Twitter OSS Code of Conduct](https://github.com/twitter/code-of-conduct/blob/master/code-of-conduct.md)

## Discussions
* Mailing list: [dev@parquet.apache.org](http://mail-archives.apache.org/mod_mbox/parquet-dev/)
* Bug trackter: [jira](https://issues.apache.org/jira/browse/PARQUET)
* Discussions also take place in github pull requests

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
See also:
