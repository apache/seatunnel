/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.tools.x2seatunnel.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** X2SeaTunnel command line options configuration */
public class CommandLineOptions {

    /** Create command line options */
    public static Options createOptions() {
        Options options = new Options();

        // Source file parameter
        options.addOption(
                Option.builder("s")
                        .longOpt("source")
                        .hasArg()
                        .desc("Source configuration file path")
                        .required(false)
                        .build());

        // Target file parameter
        options.addOption(
                Option.builder("t")
                        .longOpt("target")
                        .hasArg()
                        .desc("Target configuration file path")
                        .required(false)
                        .build());

        // Source type parameter
        options.addOption(
                Option.builder("st")
                        .longOpt("source-type")
                        .hasArg()
                        .desc(
                                "Source configuration type (datax, sqloop, flume, auto, default: datax)")
                        .build());

        // Custom template parameter
        options.addOption(
                Option.builder("T")
                        .longOpt("template")
                        .hasArg()
                        .desc("Custom template file name")
                        .build());

        // Report file parameter
        options.addOption(
                Option.builder("r")
                        .longOpt("report")
                        .hasArg()
                        .desc("Conversion report file path")
                        .build());

        // Report directory (output directory for individual file reports in batch mode)
        options.addOption(
                Option.builder("R")
                        .longOpt("report-dir")
                        .hasArg()
                        .desc(
                                "Report output directory in batch mode, individual file reports and summary.md will be output to this directory")
                        .build());

        // Version information
        options.addOption(
                Option.builder("v").longOpt("version").desc("Show version information").build());

        // Help information
        options.addOption(
                Option.builder("h").longOpt("help").desc("Show help information").build());

        // Verbose logging
        options.addOption(
                Option.builder().longOpt("verbose").desc("Enable verbose log output").build());

        // YAML configuration file
        options.addOption(
                Option.builder("c")
                        .longOpt("config")
                        .hasArg()
                        .desc(
                                "YAML configuration file path, containing source, target, report, template and other settings")
                        .required(false)
                        .build());

        // Batch conversion source directory
        options.addOption(
                Option.builder("d")
                        .longOpt("directory")
                        .hasArg()
                        .desc("Source file directory to be converted")
                        .required(false)
                        .build());

        // Batch conversion output directory
        options.addOption(
                Option.builder("o")
                        .longOpt("output-dir")
                        .hasArg()
                        .desc("Batch conversion output directory")
                        .required(false)
                        .build());

        // Batch conversion file matching pattern
        options.addOption(
                Option.builder("p")
                        .longOpt("pattern")
                        .hasArg()
                        .desc(
                                "Batch conversion file wildcard pattern, comma separated, e.g.: *.json,*.xml")
                        .build());

        return options;
    }
}
