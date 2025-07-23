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

package org.apache.seatunnel.tools.x2seatunnel.report;

import org.apache.seatunnel.tools.x2seatunnel.model.MappingResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** MarkdownReportGenerator 单元测试 - 验证增强的报告功能 */
public class MarkdownReportGeneratorEnhancedTest {

    private MarkdownReportGenerator reportGenerator;
    private MappingResult mappingResult;

    @BeforeEach
    public void setUp() {
        reportGenerator = new MarkdownReportGenerator();
        mappingResult = new MappingResult();

        // 设置测试数据：包含各种类型的映射
        setupTestMappingResult();
    }

    private void setupTestMappingResult() {
        // 添加成功映射
        mappingResult.addSuccessMapping(
                "job.content[0].reader.parameter.username", "source.Jdbc.user", "root");
        mappingResult.addSuccessMapping(
                "job.content[0].reader.parameter.password", "source.Jdbc.password", "123456");
        mappingResult.addSuccessMapping(
                "job.content[0].reader.parameter.connection[0].jdbcUrl[0]",
                "source.Jdbc.url",
                "jdbc:mysql://localhost:3306/test");
        mappingResult.addSuccessMapping(
                "job.content[0].reader.parameter.connection[0].table[0]",
                "source.Jdbc.table",
                "users");

        // 添加默认值字段（转换器自动构造的）
        mappingResult.addDefaultValueField(
                "source.Jdbc.driver", "com.mysql.cj.jdbc.Driver", "根据JDBC URL自动推断");
        mappingResult.addDefaultValueField("source.Jdbc.query", "SELECT * FROM users", "根据表名自动生成");

        // 添加默认值字段
        mappingResult.addDefaultValueField("env.parallelism", "1", "使用默认并行度");
        mappingResult.addDefaultValueField("env.job.mode", "BATCH", "DataX默认为批处理模式");
        mappingResult.addDefaultValueField("source.Jdbc.fetchSize", "1000", "使用默认fetch大小");

        // 添加缺失字段
        mappingResult.addMissingRequiredField(
                "job.content[0].reader.parameter.host", "DataX配置中未找到该字段");

        // 添加未映射字段
        mappingResult.addUnmappedField(
                "job.content[0].reader.parameter.splitPk", "id", "DataX特有配置，SeaTunnel不需要");
        mappingResult.addUnmappedField(
                "job.content[0].reader.parameter.where", "status=1", "DataX特有配置，SeaTunnel不需要");

        mappingResult.setSuccess(true);
    }

    @Test
    public void testEmptyMappingResult() {
        MappingResult emptyResult = new MappingResult();
        emptyResult.setSuccess(true);

        String report =
                reportGenerator.generateReport(
                        emptyResult,
                        "examples/empty-datax.json",
                        "examples/empty-seatunnel.conf",
                        "datax");

        // 验证空结果能正常生成报告，不测试具体格式
        assertTrue(report.length() > 0, "空结果应该能生成报告");
        assertTrue(
                report.contains("0") || report.contains("无") || report.contains("empty"),
                "应该反映空状态");
    }

    @Test
    public void testFailedConversionReport() {
        MappingResult failedResult = new MappingResult();
        failedResult.setSuccess(false);
        failedResult.setErrorMessage("模板解析失败：语法错误");

        String report =
                reportGenerator.generateReport(
                        failedResult,
                        "examples/error-datax.json",
                        "examples/error-seatunnel.conf",
                        "datax");

        // 验证失败报告能正常生成，不测试具体格式
        assertTrue(report.length() > 0, "失败结果应该能生成报告");
        assertTrue(
                report.contains("失败")
                        || report.contains("错误")
                        || report.contains("error")
                        || report.contains("fail"),
                "应该反映失败状态");
        assertTrue(report.contains("模板解析失败"), "应该包含错误信息");
    }

    @Test
    public void testBasicReportGeneration() {
        String report =
                reportGenerator.generateReport(
                        mappingResult,
                        "examples/test-datax.json",
                        "examples/test-seatunnel.conf",
                        "datax");

        // 只测试基本功能：能生成报告且包含基本信息
        assertTrue(report.length() > 0, "应该能生成报告");
        assertTrue(
                report.contains("X2SeaTunnel")
                        || report.contains("转换")
                        || report.contains("report"),
                "应该包含工具相关信息");
        assertTrue(report.contains("datax") || report.contains("test"), "应该包含输入文件信息");
    }
}
