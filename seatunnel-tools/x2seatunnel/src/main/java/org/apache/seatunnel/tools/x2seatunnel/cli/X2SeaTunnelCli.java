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

import org.apache.seatunnel.tools.x2seatunnel.core.ConversionEngine;
import org.apache.seatunnel.tools.x2seatunnel.util.BatchConversionReport;
import org.apache.seatunnel.tools.x2seatunnel.util.ConversionConfig;
import org.apache.seatunnel.tools.x2seatunnel.util.DirectoryProcessor;
import org.apache.seatunnel.tools.x2seatunnel.util.FilePattern;
import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;
import org.apache.seatunnel.tools.x2seatunnel.util.YamlConfigParser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.List;

/** X2SeaTunnel command-line tool main class */
public class X2SeaTunnelCli {

    private static final Logger logger = LoggerFactory.getLogger(X2SeaTunnelCli.class);

    private static final String TOOL_NAME = "x2seatunnel";

    public static void main(String[] args) {
        try {
            X2SeaTunnelCli cli = new X2SeaTunnelCli();
            cli.run(args);
        } catch (Exception e) {
            logger.error("Execution failed: {}", e.getMessage());
            System.exit(1);
        }
    }

    public void run(String[] args) {
        Options options = CommandLineOptions.createOptions();

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            // Support YAML configuration file
            ConversionConfig yamlConfig = null;
            if (cmd.hasOption("c") || cmd.hasOption("config")) {
                String configPath = cmd.getOptionValue("c", cmd.getOptionValue("config"));
                yamlConfig = YamlConfigParser.parse(configPath);
                logger.info("Loaded YAML configuration: {}", configPath);
            }

            // Read batch mode parameters in advance
            String directory = null;
            String outputDir = null;
            String reportDir = null;
            // Custom template for batch mode
            String batchTemplate = null;
            if (cmd.hasOption("d")) directory = cmd.getOptionValue("d");
            if (cmd.hasOption("directory")) directory = cmd.getOptionValue("directory");
            if (cmd.hasOption("o")) outputDir = cmd.getOptionValue("o");
            if (cmd.hasOption("output-dir")) outputDir = cmd.getOptionValue("output-dir");
            if (cmd.hasOption("R")) reportDir = cmd.getOptionValue("R");
            if (cmd.hasOption("report-dir")) reportDir = cmd.getOptionValue("report-dir");
            if (cmd.hasOption("T")) batchTemplate = cmd.getOptionValue("T");
            if (cmd.hasOption("template")) batchTemplate = cmd.getOptionValue("template");

            // If batch mode is specified, execute batch logic first and return directly
            if (directory != null) {
                if (outputDir == null) {
                    logger.error("Batch conversion requires output directory: -o/--output-dir");
                    printUsage();
                    System.exit(1);
                }
                logger.info(
                        "Starting batch conversion, source directory={}, output directory={}",
                        directory,
                        outputDir);
                FileUtils.createDirectory(outputDir);
                if (reportDir != null) {
                    logger.info("Report directory={}", reportDir);
                    FileUtils.createDirectory(reportDir);
                }
                DirectoryProcessor dp = new DirectoryProcessor(directory, outputDir);
                List<String> sources = dp.listSourceFiles();
                String pattern = cmd.getOptionValue("p", cmd.getOptionValue("pattern"));
                sources = FilePattern.filter(sources, pattern);
                if (sources.isEmpty()) {
                    logger.warn(
                            "No files to convert found in source directory: {} with pattern: {}",
                            directory,
                            pattern);
                }
                ConversionEngine engine = new ConversionEngine();
                BatchConversionReport batchReport = new BatchConversionReport();

                // Set batch conversion configuration information
                batchReport.setConversionConfig(
                        directory, outputDir, reportDir, pattern, batchTemplate);

                int total = sources.size();
                for (int i = 0; i < total; i++) {
                    String src = sources.get(i);
                    String tgt = dp.resolveTargetPath(src);
                    String rpt;
                    if (reportDir != null) {
                        String name = FileUtils.getFileNameWithoutExtension(src);
                        rpt = Paths.get(reportDir, name + ".md").toString();
                    } else {
                        rpt = cmd.getOptionValue("r", cmd.getOptionValue("report"));
                        if (rpt == null) {
                            rpt = dp.resolveReportPath(src);
                        }
                    }
                    logger.info("[{} / {}] Processing file: {}", i + 1, total, src);
                    try {
                        engine.convert(src, tgt, "datax", "seatunnel", batchTemplate, rpt);
                        batchReport.recordSuccess(src, tgt, rpt);
                        System.out.println(
                                String.format(
                                        "[%d/%d] Conversion completed: %s -> %s",
                                        i + 1, total, src, tgt));
                    } catch (Exception e) {
                        logger.error(
                                "File conversion failed: {} -> {} , error: {}",
                                src,
                                tgt,
                                e.getMessage());
                        batchReport.recordFailure(src, e.getMessage());
                    }
                }
                String summary;
                if (reportDir != null) {
                    summary = Paths.get(reportDir, "summary.md").toString();
                } else {
                    summary = cmd.getOptionValue("r", cmd.getOptionValue("report"));
                    if (summary == null) {
                        summary = Paths.get(outputDir, "summary.md").toString();
                    }
                }
                batchReport.writeReport(summary);
                System.out.println(
                        "Batch conversion completed! Output directory: "
                                + outputDir
                                + ", Report: "
                                + summary);
                return;
            }

            // Validate required parameters: only required to specify -s/-t in non-YAML and
            // non-batch mode
            if (yamlConfig == null && directory == null) {
                if (!cmd.hasOption("s") && !cmd.hasOption("source")) {
                    logger.error("Missing required parameter: -s/--source");
                    printUsage();
                    System.exit(1);
                }
                if (!cmd.hasOption("t") && !cmd.hasOption("target")) {
                    logger.error("Missing required parameter: -t/--target");
                    printUsage();
                    System.exit(1);
                }
            }

            // Get parameter values, command line takes priority, then YAML
            String sourceFile = yamlConfig != null ? yamlConfig.getSource() : null;
            String targetFile = yamlConfig != null ? yamlConfig.getTarget() : null;
            String sourceType =
                    yamlConfig != null && yamlConfig.getSourceType() != null
                            ? yamlConfig.getSourceType()
                            : "datax";
            String customTemplate = yamlConfig != null ? yamlConfig.getTemplate() : null;
            String reportFile = yamlConfig != null ? yamlConfig.getReport() : null;
            // Command line parameters override YAML configuration
            if (cmd.hasOption("s")) sourceFile = cmd.getOptionValue("s");
            if (cmd.hasOption("source")) sourceFile = cmd.getOptionValue("source");
            if (cmd.hasOption("t")) targetFile = cmd.getOptionValue("t");
            if (cmd.hasOption("target")) targetFile = cmd.getOptionValue("target");
            if (cmd.hasOption("st")) sourceType = cmd.getOptionValue("st");
            if (cmd.hasOption("source-type")) sourceType = cmd.getOptionValue("source-type");
            if (cmd.hasOption("T")) customTemplate = cmd.getOptionValue("T");
            if (cmd.hasOption("template")) customTemplate = cmd.getOptionValue("template");
            if (cmd.hasOption("r")) reportFile = cmd.getOptionValue("r");
            if (cmd.hasOption("report")) reportFile = cmd.getOptionValue("report");
            String targetType = "seatunnel"; // Fixed as seatunnel

            // Execute conversion
            ConversionEngine engine = new ConversionEngine();
            engine.convert(
                    sourceFile, targetFile, sourceType, targetType, customTemplate, reportFile);

            System.out.println("Configuration conversion completed!");
            System.out.println("Source file: " + sourceFile);
            System.out.println("Target file: " + targetFile);
            if (reportFile != null) {
                System.out.println("Conversion report: " + reportFile);
            }

        } catch (ParseException e) {
            logger.error("Parameter parsing failed: {}", e.getMessage());
            printHelp(options);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Error occurred during conversion: {}", e.getMessage());
            System.exit(1);
        }
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                TOOL_NAME,
                "X2SeaTunnel configuration conversion tool",
                options,
                "\\nExamples:\\n"
                        + "  "
                        + TOOL_NAME
                        + " -s datax.json -t seatunnel.conf\\n"
                        + "  "
                        + TOOL_NAME
                        + " --source datax.json --target seatunnel.conf --source-type datax --report report.md\\n");
    }

    private void printUsage() {
        System.out.println("Usage: x2seatunnel [OPTIONS]");
        System.out.println(
                "Common batch mode: x2seatunnel -d <source_dir> -o <output_dir> [-R <report_dir>] [-p <pattern>]");
        System.out.println("Use -h or --help to view complete help information");
    }
}
