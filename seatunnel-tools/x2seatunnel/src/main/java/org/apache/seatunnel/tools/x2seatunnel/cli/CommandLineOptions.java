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
 * Unless required by applicable law or agreed in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.tools.x2seatunnel.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** X2SeaTunnel 命令行选项配置 */
public class CommandLineOptions {

    /** 创建命令行选项 */
    public static Options createOptions() {
        Options options = new Options();

        // 源文件参数
        options.addOption(
                Option.builder("s")
                        .longOpt("source")
                        .hasArg()
                        .desc("源配置文件路径")
                        .required(false)
                        .build());

        // 目标文件参数
        options.addOption(
                Option.builder("t")
                        .longOpt("target")
                        .hasArg()
                        .desc("目标配置文件路径")
                        .required(false)
                        .build());

        // 源类型参数
        options.addOption(
                Option.builder("st")
                        .longOpt("source-type")
                        .hasArg()
                        .desc("源配置类型 (datax, sqloop, flume, auto，默认: datax)")
                        .build());

        // 自定义模板参数
        options.addOption(
                Option.builder("T").longOpt("template").hasArg().desc("自定义模板文件名").build());

        // 报告文件参数
        options.addOption(Option.builder("r").longOpt("report").hasArg().desc("转换报告文件路径").build());

        // 报告目录（批量模式下单文件报告输出目录）
        options.addOption(
                Option.builder("R")
                        .longOpt("report-dir")
                        .hasArg()
                        .desc("批量模式下报告输出目录，单文件报告和汇总summary.md将输出到该目录")
                        .build());

        // 版本信息
        options.addOption(Option.builder("v").longOpt("version").desc("显示版本信息").build());

        // 帮助信息
        options.addOption(Option.builder("h").longOpt("help").desc("显示帮助信息").build());

        // 详细日志
        options.addOption(Option.builder().longOpt("verbose").desc("启用详细日志输出").build());

        // YAML 配置文件
        options.addOption(
                Option.builder("c")
                        .longOpt("config")
                        .hasArg()
                        .desc("YAML 配置文件路径，包含 source, target, report, template 等设置")
                        .required(false)
                        .build());

        // 批量转换源目录
        options.addOption(
                Option.builder("d")
                        .longOpt("directory")
                        .hasArg()
                        .desc("待转换源文件目录")
                        .required(false)
                        .build());

        // 批量转换输出目录
        options.addOption(
                Option.builder("o")
                        .longOpt("output-dir")
                        .hasArg()
                        .desc("批量转换输出目录")
                        .required(false)
                        .build());

        // 批量转换文件匹配模式
        options.addOption(
                Option.builder("p")
                        .longOpt("pattern")
                        .hasArg()
                        .desc("批量转换文件通配符模式，逗号分隔，例如: *.json,*.xml")
                        .build());

        return options;
    }
}
