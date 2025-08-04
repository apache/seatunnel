/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

/** MarkdownReportGenerator unit tests - verifying enhanced report functionality */
public class MarkdownReportGeneratorEnhancedTest {

    private MarkdownReportGenerator reportGenerator;
    private MappingResult mappingResult;

    @BeforeEach
    public void setUp() {
        reportGenerator = new MarkdownReportGenerator();
        mappingResult = new MappingResult();

        // Set up test data: containing various types of mappings
        setupTestMappingResult();
    }

    private void setupTestMappingResult() {
        // Add successful mappings
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

        mappingResult.addDefaultValueField(
                "source.Jdbc.driver",
                "com.mysql.cj.jdbc.Driver",
                "Automatically inferred from JDBC URL");
        mappingResult.addDefaultValueField(
                "source.Jdbc.query",
                "SELECT * FROM users",
                "Automatically generated from table name");

        mappingResult.addDefaultValueField("env.parallelism", "1", "Using default parallelism");
        mappingResult.addDefaultValueField("env.job.mode", "BATCH", "DataX defaults to BATCH mode");
        mappingResult.addDefaultValueField(
                "source.Jdbc.fetchSize", "1000", "Using default fetch size");

        mappingResult.addMissingRequiredField(
                "job.content[0].reader.parameter.host", "Field not found in DataX configuration");

        mappingResult.addUnmappedField(
                "job.content[0].reader.parameter.splitPk",
                "id",
                "DataX-specific configuration, not needed in SeaTunnel");
        mappingResult.addUnmappedField(
                "job.content[0].reader.parameter.where",
                "status=1",
                "DataX-specific configuration, not needed in SeaTunnel");

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

        // Verify that an empty result can generate a report, without testing the specific format
        assertTrue(report.length() > 0, "An empty result should generate a report");
        assertTrue(
                report.contains("0") || report.contains("none") || report.contains("empty"),
                "Should reflect the empty state");
    }

    @Test
    public void testFailedConversionReport() {
        MappingResult failedResult = new MappingResult();
        failedResult.setSuccess(false);
        failedResult.setErrorMessage("Template parsing failed: syntax error");

        String report =
                reportGenerator.generateReport(
                        failedResult,
                        "examples/error-datax.json",
                        "examples/error-seatunnel.conf",
                        "datax");

        // Verify that a failure report can be generated, without testing the specific format
        assertTrue(report.length() > 0, "A failed result should generate a report");
        assertTrue(
                report.contains("Failed")
                        || report.contains("Error")
                        || report.contains("error")
                        || report.contains("fail"),
                "Should reflect the failure state");
        assertTrue(report.contains("Template parsing failed"), "Should contain the error message");
    }

    @Test
    public void testBasicReportGeneration() {
        String report =
                reportGenerator.generateReport(
                        mappingResult,
                        "examples/test-datax.json",
                        "examples/test-seatunnel.conf",
                        "datax");

        // Test only basic functionality: ensures a report is generated and contains basic info
        assertTrue(report.length() > 0, "Should be able to generate a report");
        assertTrue(
                report.contains("X2SeaTunnel")
                        || report.contains("Conversion")
                        || report.contains("report"),
                "Should contain tool-related information");
        assertTrue(
                report.contains("datax") || report.contains("test"),
                "Should contain input file information");
    }
}
