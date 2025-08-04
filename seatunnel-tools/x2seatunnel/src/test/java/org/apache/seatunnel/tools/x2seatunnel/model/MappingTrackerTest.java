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

package org.apache.seatunnel.tools.x2seatunnel.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MappingTracker unit tests */
public class MappingTrackerTest {

    private MappingTracker mappingTracker;

    @BeforeEach
    public void setUp() {
        mappingTracker = new MappingTracker();
    }

    @Test
    public void testRecordDirectMapping() {
        // Test recording direct mapping
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.username",
                "source.Jdbc.user",
                "root",
                "Directly extracted from DataX");
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.password",
                "source.Jdbc.password",
                "123456",
                "Directly extracted from DataX");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getSuccessMappings().size());
        assertEquals(
                "job.content[0].reader.parameter.username",
                result.getSuccessMappings().get(0).getSourceField());
        assertEquals("source.Jdbc.user", result.getSuccessMappings().get(0).getTargetField());
        assertEquals("root", result.getSuccessMappings().get(0).getValue());
    }

    @Test
    public void testRecordTransformMapping() {
        // Test recording transform mapping fields
        mappingTracker.recordTransformMapping(
                "job.content[0].reader.parameter.connection[0].jdbcUrl[0]",
                "source.Jdbc.driver",
                "com.mysql.cj.jdbc.Driver",
                "jdbc_driver_mapper");
        mappingTracker.recordTransformMapping(
                "job.content[0].reader.parameter.username", "source.Jdbc.user", "ROOT", "upper");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getTransformMappings().size());
        assertEquals("source.Jdbc.driver", result.getTransformMappings().get(0).getTargetField());
        assertEquals("com.mysql.cj.jdbc.Driver", result.getTransformMappings().get(0).getValue());
        assertEquals("jdbc_driver_mapper", result.getTransformMappings().get(0).getFilterName());
    }

    @Test
    public void testRecordDefaultValue() {
        // Test recording default value fields
        mappingTracker.recordDefaultValue("env.parallelism", "1", "Using default parallelism");
        mappingTracker.recordDefaultValue(
                "env.job.mode", "BATCH", "DataX defaults to batch processing mode");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getDefaultValues().size());
        assertEquals("env.parallelism", result.getDefaultValues().get(0).getFieldName());
        assertEquals("1", result.getDefaultValues().get(0).getValue());
        assertEquals("Using default parallelism", result.getDefaultValues().get(0).getReason());
    }

    @Test
    public void testRecordMissingField() {
        // Test recording missing fields
        mappingTracker.recordMissingField(
                "job.content[0].reader.parameter.host", "Field not found in DataX configuration");
        mappingTracker.recordMissingField(
                "job.content[0].reader.parameter.port",
                "Field value is empty in DataX configuration");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getMissingRequiredFields().size());
        assertEquals(
                "job.content[0].reader.parameter.host",
                result.getMissingRequiredFields().get(0).getFieldName());
        assertEquals(
                "Field not found in DataX configuration",
                result.getMissingRequiredFields().get(0).getReason());
    }

    @Test
    public void testRecordUnmappedField() {
        // Test recording unmapped fields
        mappingTracker.recordUnmappedField(
                "job.content[0].reader.parameter.fetchSize",
                "1000",
                "DataX specific configuration, not needed by SeaTunnel");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(1, result.getUnmappedFields().size());
        assertEquals(
                "job.content[0].reader.parameter.fetchSize",
                result.getUnmappedFields().get(0).getFieldName());
        assertEquals("1000", result.getUnmappedFields().get(0).getValue());
        assertEquals(
                "DataX specific configuration, not needed by SeaTunnel",
                result.getUnmappedFields().get(0).getReason());
    }

    @Test
    public void testMixedMappingTypes() {
        // Test mixed mapping types
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.username",
                "source.Jdbc.user",
                "root",
                "Direct mapping");
        mappingTracker.recordTransformMapping(
                "job.content[0].reader.parameter.connection[0].jdbcUrl[0]",
                "source.Jdbc.driver",
                "com.mysql.cj.jdbc.Driver",
                "jdbc_driver_mapper");
        mappingTracker.recordDefaultValue("env.parallelism", "1", "Default value");
        mappingTracker.recordMissingField("missing.field", "Missing field");
        mappingTracker.recordUnmappedField("unmapped.field", "value", "Unmapped");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(1, result.getSuccessMappings().size());
        assertEquals(1, result.getTransformMappings().size());
        assertEquals(1, result.getDefaultValues().size());
        assertEquals(1, result.getMissingRequiredFields().size());
        assertEquals(1, result.getUnmappedFields().size());
        assertTrue(result.isSuccess());
    }

    @Test
    public void testReset() {
        // Add some mapping records
        mappingTracker.recordDirectMapping("test.field", "target.field", "value", "test");
        mappingTracker.recordTransformMapping(
                "source.field", "target.field", "transformed.value", "upper");

        // Verify records exist
        MappingResult result1 = mappingTracker.generateMappingResult();
        assertEquals(1, result1.getSuccessMappings().size());
        assertEquals(1, result1.getTransformMappings().size());

        // Verify cleared after reset
        mappingTracker.reset();
        MappingResult result2 = mappingTracker.generateMappingResult();
        assertEquals(0, result2.getSuccessMappings().size());
        assertEquals(0, result2.getTransformMappings().size());
        assertEquals(0, result2.getDefaultValues().size());
        assertEquals(0, result2.getMissingRequiredFields().size());
        assertEquals(0, result2.getUnmappedFields().size());
    }

    @Test
    public void testGetStatistics() {
        // Add various types of mapping records
        mappingTracker.recordDirectMapping("direct1", "target1", "value1", "test");
        mappingTracker.recordDirectMapping("direct2", "target2", "value2", "test");
        mappingTracker.recordTransformMapping("transform1", "target3", "transformValue1", "upper");
        mappingTracker.recordDefaultValue("default1", "defaultValue1", "default test");
        mappingTracker.recordMissingField("missing1", "missing test");
        mappingTracker.recordUnmappedField("unmapped1", "unmappedValue1", "unmapped test");

        String statistics = mappingTracker.getStatisticsText();

        assertTrue(statistics.contains("Direct mappings: 2"));
        assertTrue(statistics.contains("Transform mappings: 1"));
        assertTrue(statistics.contains("Default values: 1"));
        assertTrue(statistics.contains("Missing: 1"));
        assertTrue(statistics.contains("Unmapped: 1"));

        MappingTracker.MappingStatistics stats = mappingTracker.getStatistics();
        assertEquals(2, stats.getDirectMappings());
        assertEquals(1, stats.getTransformMappings());
        assertEquals(1, stats.getDefaultValues());
        assertEquals(1, stats.getMissingFields());
        assertEquals(1, stats.getUnmappedFields());
    }
}
