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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import java.util.List;

@Parameters(commandDescription = "Retrieves details on the functions of other commands")
public class Help implements Command {
  @Parameter(description = "<commands>")
  List<String> helpCommands = Lists.newArrayList();

  private final JCommander jc;
  private final Logger console;
  private String programName;

  public Help(JCommander jc, Logger console) {
    this.jc = jc;
    this.console = console;
  }

  public void setProgramName(String programName) {
    this.programName = programName;
  }

  @Override
  public int run() {
    if (helpCommands.isEmpty()) {
      printGenericHelp();

    } else {
      for (String cmd : helpCommands) {
        JCommander commander = jc.getCommands().get(cmd);
        if (commander == null) {
          console.error("\nUnknown command: {}\n", cmd);
          printGenericHelp();
          return 1;
        }

        boolean hasRequired = false;
        if (commander.getParameters().stream().anyMatch(p -> p.getNames().isEmpty())) {
          console.info("\nUsage: {} [general options] {} {} [command options]",
            new Object[] { programName, cmd, commander.getMainParameterDescription() });
        } else {
          console.info("\nUsage: {} [general options] {} [command options]",
            new Object[] { programName, cmd });
        }
        console.info("\n  Description:");
        console.info("\n    {}", jc.getCommandDescription(cmd));
        if (!commander.getParameters().isEmpty()) {
          console.info("\n  Command options:\n");
          for (ParameterDescription param : commander.getParameters()) {
            hasRequired = printOption(console, param) || hasRequired;
          }
          if (hasRequired) {
            console.info("\n  * = required");
          }
        }
        List<String> examples = ((Command) commander.getObjects().get(0)).getExamples();
        if (examples != null) {
          console.info("\n  Examples:");
          for (String example : examples) {
            if (example.startsWith("#")) {
              // comment
              console.info("\n    {}", example);
            } else {
              console.info("    {} {} {}",
                  new Object[] {programName, cmd, example});
            }
          }
        }
        // add an extra newline in case there are more commands
        console.info("");
      }
    }
    return 0;
  }

  public void printGenericHelp() {
    boolean hasRequired = false;
    console.info(
        "\nUsage: {} [options] [command] [command options]",
        programName);
    console.info("\n  Options:\n");
    for (ParameterDescription param : jc.getParameters()) {
      hasRequired = printOption(console, param) || hasRequired;
    }
    if (hasRequired) {
      console.info("\n  * = required");
    }
    console.info("\n  Commands:\n");
    for (String command : jc.getCommands().keySet()) {
      console.info("    {}\n\t{}",
          command, jc.getCommandDescription(command));
    }

    jc.getCommands().keySet().stream().filter(s -> !s.equals("help")).findFirst().ifPresent(command -> {
      console.info("\n  Examples:");
      console.info("\n    # print information for {}\n    {} help {}",
        command, programName, command);
      console.info("\n  See '{} help <command>' for more information on a " +
        "specific command.", programName);
    });
  }

  private boolean printOption(Logger console, ParameterDescription param) {
    boolean required = param.getParameter().required();
    if (!param.getParameter().hidden()) {
      console.info("  {} {}\n\t{}{}", new Object[]{
          required ? "*" : " ",
          param.getNames().trim(),
          param.getDescription(),
          formatDefault(param)});
    }
    return required;
  }

  private String formatDefault(ParameterDescription param) {
    Object defaultValue = param.getDefault();
    if (defaultValue == null || param.getParameter().arity() < 1) {
      return "";
    }
    return " (default: " + ((defaultValue instanceof String) ?
        "\"" + defaultValue + "\"" :
        defaultValue.toString()) + ")";
  }

  @Override
  public List<String> getExamples() {
    return null;
  }
}
