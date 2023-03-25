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
package org.apache.parquet.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.parquet.cli.commands.CSVSchemaCommand;
import org.apache.parquet.cli.commands.CatCommand;
import org.apache.parquet.cli.commands.CheckParquet251Command;
import org.apache.parquet.cli.commands.ColumnMaskingCommand;
import org.apache.parquet.cli.commands.ColumnSizeCommand;
import org.apache.parquet.cli.commands.ConvertCSVCommand;
import org.apache.parquet.cli.commands.ConvertCommand;
import org.apache.parquet.cli.commands.ParquetMetadataCommand;
import org.apache.parquet.cli.commands.PruneColumnsCommand;
import org.apache.parquet.cli.commands.RewriteCommand;
import org.apache.parquet.cli.commands.ScanCommand;
import org.apache.parquet.cli.commands.SchemaCommand;
import org.apache.parquet.cli.commands.ShowBloomFilterCommand;
import org.apache.parquet.cli.commands.ShowColumnIndexCommand;
import org.apache.parquet.cli.commands.ShowDictionaryCommand;
import org.apache.parquet.cli.commands.ShowFooterCommand;
import org.apache.parquet.cli.commands.ShowPagesCommand;
import org.apache.parquet.cli.commands.ToAvroCommand;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.parquet.cli.commands.TransCompressionCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

@Parameters(commandDescription = "Parquet file utils")
public class Main extends Configured implements Tool {

  @Parameter(names = {"-v", "--verbose", "--debug"},
      description = "Print extra debugging information")
  private boolean debug = false;

  @VisibleForTesting
  @Parameter(names="--dollar-zero",
      description="A way for the runtime path to be passed in", hidden=true)
  String programName = DEFAULT_PROGRAM_NAME;

  @VisibleForTesting
  static final String DEFAULT_PROGRAM_NAME = "parquet";

  private static Set<String> HELP_ARGS = ImmutableSet.of("-h", "-help", "--help", "help");

  private final Logger console;
  private final Help help;

  @VisibleForTesting
  final JCommander jc;

  Main(Logger console) {
    this.console = console;
    this.jc = new JCommander(this);
    this.help = new Help(jc, console);
    jc.setProgramName(DEFAULT_PROGRAM_NAME);
    jc.addCommand("help", help, "-h", "-help", "--help");
    jc.addCommand("meta", new ParquetMetadataCommand(console));
    jc.addCommand("pages", new ShowPagesCommand(console));
    jc.addCommand("dictionary", new ShowDictionaryCommand(console));
    jc.addCommand("check-stats", new CheckParquet251Command(console));
    jc.addCommand("schema", new SchemaCommand(console));
    jc.addCommand("csv-schema", new CSVSchemaCommand(console));
    jc.addCommand("convert-csv", new ConvertCSVCommand(console));
    jc.addCommand("convert", new ConvertCommand(console));
    jc.addCommand("to-avro", new ToAvroCommand(console));
    jc.addCommand("cat", new CatCommand(console, 0));
    jc.addCommand("head", new CatCommand(console, 10));
    jc.addCommand("column-index", new ShowColumnIndexCommand(console));
    jc.addCommand("column-size", new ColumnSizeCommand(console));
    jc.addCommand("prune", new PruneColumnsCommand(console));
    jc.addCommand("trans-compression", new TransCompressionCommand(console));
    jc.addCommand("masking", new ColumnMaskingCommand(console));
    jc.addCommand("footer", new ShowFooterCommand(console));
    jc.addCommand("bloom-filter", new ShowBloomFilterCommand(console));
    jc.addCommand("scan", new ScanCommand(console));
    jc.addCommand("rewrite", new RewriteCommand(console));
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      jc.parse(args);
    } catch (MissingCommandException e) {
      console.error(e.getMessage());
      return 1;
    } catch (ParameterException e) {
      help.setProgramName(programName);
      String cmd = jc.getParsedCommand();
      if (args.length == 1) { // i.e., just the command (missing required arguments)
        help.helpCommands.add(cmd);
        help.run();
        return 1;
      } else { // check for variants like 'cmd --help' etc.
        for (String arg : args) {
          if (HELP_ARGS.contains(arg)) {
            help.helpCommands.add(cmd);
            help.run();
            return 0;
          }
        }
      }
      console.error(e.getMessage());
      return 1;
    }

    help.setProgramName(programName);

    // configure log4j
    if (debug) {
      org.apache.log4j.Logger console = org.apache.log4j.Logger.getLogger(Main.class);
      console.setLevel(Level.DEBUG);
    }

    String parsed = jc.getParsedCommand();
    if (parsed == null) {
      help.run();
      return 1;
    } else if ("help".equals(parsed)) {
      return help.run();
    }

    Command command = (Command) jc.getCommands().get(parsed).getObjects().get(0);
    if (command == null) {
      help.run();
      return 1;
    }

    try {
      if (command instanceof Configurable) {
        ((Configurable) command).setConf(getConf());
      }
      return command.run();
    } catch (IllegalArgumentException e) {
      if (debug) {
        console.error("Argument error", e);
      } else {
        console.error("Argument error: {}", e.getMessage());
      }
      return 1;
    } catch (IllegalStateException e) {
      if (debug) {
        console.error("State error", e);
      } else {
        console.error("State error: {}", e.getMessage());
      }
      return 1;
    } catch (Exception e) {
      console.error("Unknown error", e);
      return 1;
    }
  }

  public static void main(String[] args) throws Exception {
    // reconfigure logging with the kite CLI configuration
    PropertyConfigurator.configure(
        Main.class.getResource("/cli-logging.properties"));
    Logger console = LoggerFactory.getLogger(Main.class);
    // use Log4j for any libraries using commons-logging
    LogFactory.getFactory().setAttribute(
        "org.apache.commons.logging.Log",
        "org.apache.commons.logging.impl.Log4JLogger");
    int rc = ToolRunner.run(new Configuration(), new Main(console), args);
    System.exit(rc);
  }
}
