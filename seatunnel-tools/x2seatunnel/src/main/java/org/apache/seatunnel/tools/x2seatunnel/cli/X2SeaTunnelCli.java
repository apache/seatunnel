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

/** X2SeaTunnel 命令行工具主类 */
public class X2SeaTunnelCli {

    private static final Logger logger = LoggerFactory.getLogger(X2SeaTunnelCli.class);

    private static final String TOOL_NAME = "x2seatunnel";
    private static final String VERSION = "1.0.0-SNAPSHOT";

    public static void main(String[] args) {
        try {
            X2SeaTunnelCli cli = new X2SeaTunnelCli();
            cli.run(args);
        } catch (Exception e) {
            logger.error("执行失败: {}", e.getMessage());
            System.exit(1);
        }
    }

    public void run(String[] args) {
        Options options = CommandLineOptions.createOptions();

        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            // 支持 YAML 配置文件
            ConversionConfig yamlConfig = null;
            if (cmd.hasOption("c") || cmd.hasOption("config")) {
                String configPath = cmd.getOptionValue("c", cmd.getOptionValue("config"));
                yamlConfig = YamlConfigParser.parse(configPath);
                logger.info("加载 YAML 配置: {}", configPath);
            }

            // 提前读取批量模式参数
            String directory = null;
            String outputDir = null;
            String reportDir = null;
            // 批量模式自定义模板
            String batchTemplate = null;
            if (cmd.hasOption("d")) directory = cmd.getOptionValue("d");
            if (cmd.hasOption("directory")) directory = cmd.getOptionValue("directory");
            if (cmd.hasOption("o")) outputDir = cmd.getOptionValue("o");
            if (cmd.hasOption("output-dir")) outputDir = cmd.getOptionValue("output-dir");
            if (cmd.hasOption("R")) reportDir = cmd.getOptionValue("R");
            if (cmd.hasOption("report-dir")) reportDir = cmd.getOptionValue("report-dir");
            if (cmd.hasOption("T")) batchTemplate = cmd.getOptionValue("T");
            if (cmd.hasOption("template")) batchTemplate = cmd.getOptionValue("template");

            // 如果指定批量模式，先执行批量逻辑并直接返回
            if (directory != null) {
                if (outputDir == null) {
                    logger.error("批量转换必须指定输出目录: -o/--output-dir");
                    printUsage();
                    System.exit(1);
                }
                logger.info("开始批量转换，源目录={}, 输出目录={}", directory, outputDir);
                FileUtils.createDirectory(outputDir);
                if (reportDir != null) {
                    logger.info("报告目录={}", reportDir);
                    FileUtils.createDirectory(reportDir);
                }
                DirectoryProcessor dp = new DirectoryProcessor(directory, outputDir);
                List<String> sources = dp.listSourceFiles();
                String pattern = cmd.getOptionValue("p", cmd.getOptionValue("pattern"));
                sources = FilePattern.filter(sources, pattern);
                if (sources.isEmpty()) {
                    logger.warn("源目录中未找到待转换文件: {} 匹配模式: {}", directory, pattern);
                }
                ConversionEngine engine = new ConversionEngine();
                BatchConversionReport batchReport = new BatchConversionReport();

                // 设置批量转换配置信息
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
                    logger.info("[{} / {}] 处理文件: {}", i + 1, total, src);
                    try {
                        engine.convert(src, tgt, "datax", "seatunnel", batchTemplate, rpt);
                        batchReport.recordSuccess(src, tgt, rpt);
                        System.out.println(
                                String.format("[%d/%d] 转换完成: %s -> %s", i + 1, total, src, tgt));
                    } catch (Exception e) {
                        logger.error("文件转换失败: {} -> {} , 错误: {}", src, tgt, e.getMessage());
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
                System.out.println("批量转换完成！输出目录：" + outputDir + "，报告：" + summary);
                return;
            }

            // 验证必需的参数：仅在非 YAML 且非批量模式下必须指定 -s/-t
            if (yamlConfig == null && directory == null) {
                if (!cmd.hasOption("s") && !cmd.hasOption("source")) {
                    logger.error("缺少必需的参数：-s/--source");
                    printUsage();
                    System.exit(1);
                }
                if (!cmd.hasOption("t") && !cmd.hasOption("target")) {
                    logger.error("缺少必需的参数：-t/--target");
                    printUsage();
                    System.exit(1);
                }
            }

            // 获取参数值，优先命令行，其次 YAML
            String sourceFile = yamlConfig != null ? yamlConfig.getSource() : null;
            String targetFile = yamlConfig != null ? yamlConfig.getTarget() : null;
            String sourceType =
                    yamlConfig != null && yamlConfig.getSourceType() != null
                            ? yamlConfig.getSourceType()
                            : "datax";
            String customTemplate = yamlConfig != null ? yamlConfig.getTemplate() : null;
            String reportFile = yamlConfig != null ? yamlConfig.getReport() : null;
            // 命令行参数覆盖 YAML 配置
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
            String targetType = "seatunnel"; // 固定为seatunnel

            // 执行转换
            ConversionEngine engine = new ConversionEngine();
            engine.convert(
                    sourceFile, targetFile, sourceType, targetType, customTemplate, reportFile);

            System.out.println("配置转换完成！");
            System.out.println("源文件: " + sourceFile);
            System.out.println("目标文件: " + targetFile);
            if (reportFile != null) {
                System.out.println("转换报告: " + reportFile);
            }

        } catch (ParseException e) {
            logger.error("参数解析失败: {}", e.getMessage());
            printHelp(options);
            System.exit(1);
        } catch (Exception e) {
            logger.error("转换过程中发生错误: {}", e.getMessage());
            System.exit(1);
        }
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
                TOOL_NAME,
                "X2SeaTunnel 配置转换工具",
                options,
                "\\n示例:\\n"
                        + "  "
                        + TOOL_NAME
                        + " -s datax.json -t seatunnel.conf\\n"
                        + "  "
                        + TOOL_NAME
                        + " --source datax.json --target seatunnel.conf --source-type datax --report report.md\\n");
    }

    private void printUsage() {
        System.out.println("使用方法：x2seatunnel [OPTIONS]");
        System.out.println(
                "常用批量模式：x2seatunnel -d <source_dir> -o <output_dir> [-R <report_dir>] [-p <pattern>]");
        System.out.println("使用 -h 或 --help 查看完整帮助信息");
    }
}
