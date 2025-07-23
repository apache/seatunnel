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

/** MappingTracker 单元测试 */
public class MappingTrackerTest {

    private MappingTracker mappingTracker;

    @BeforeEach
    public void setUp() {
        mappingTracker = new MappingTracker();
    }

    @Test
    public void testRecordDirectMapping() {
        // 测试记录直接映射
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.username",
                "source.Jdbc.user",
                "root",
                "从DataX直接提取");
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.password",
                "source.Jdbc.password",
                "123456",
                "从DataX直接提取");

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
        // 测试记录转换映射字段
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
        // 测试记录默认值字段
        mappingTracker.recordDefaultValue("env.parallelism", "1", "使用默认并行度");
        mappingTracker.recordDefaultValue("env.job.mode", "BATCH", "DataX默认为批处理模式");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getDefaultValues().size());
        assertEquals("env.parallelism", result.getDefaultValues().get(0).getFieldName());
        assertEquals("1", result.getDefaultValues().get(0).getValue());
        assertEquals("使用默认并行度", result.getDefaultValues().get(0).getReason());
    }

    @Test
    public void testRecordMissingField() {
        // 测试记录缺失字段
        mappingTracker.recordMissingField("job.content[0].reader.parameter.host", "DataX配置中未找到该字段");
        mappingTracker.recordMissingField("job.content[0].reader.parameter.port", "DataX配置中字段值为空");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(2, result.getMissingRequiredFields().size());
        assertEquals(
                "job.content[0].reader.parameter.host",
                result.getMissingRequiredFields().get(0).getFieldName());
        assertEquals("DataX配置中未找到该字段", result.getMissingRequiredFields().get(0).getReason());
    }

    @Test
    public void testRecordUnmappedField() {
        // 测试记录未映射字段
        mappingTracker.recordUnmappedField(
                "job.content[0].reader.parameter.fetchSize", "1000", "DataX特有配置，SeaTunnel不需要");

        MappingResult result = mappingTracker.generateMappingResult();

        assertEquals(1, result.getUnmappedFields().size());
        assertEquals(
                "job.content[0].reader.parameter.fetchSize",
                result.getUnmappedFields().get(0).getFieldName());
        assertEquals("1000", result.getUnmappedFields().get(0).getValue());
        assertEquals("DataX特有配置，SeaTunnel不需要", result.getUnmappedFields().get(0).getReason());
    }

    @Test
    public void testMixedMappingTypes() {
        // 测试混合各种映射类型
        mappingTracker.recordDirectMapping(
                "job.content[0].reader.parameter.username", "source.Jdbc.user", "root", "直接映射");
        mappingTracker.recordTransformMapping(
                "job.content[0].reader.parameter.connection[0].jdbcUrl[0]",
                "source.Jdbc.driver",
                "com.mysql.cj.jdbc.Driver",
                "jdbc_driver_mapper");
        mappingTracker.recordDefaultValue("env.parallelism", "1", "默认值");
        mappingTracker.recordMissingField("missing.field", "缺失字段");
        mappingTracker.recordUnmappedField("unmapped.field", "value", "未映射");

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
        // 添加一些映射记录
        mappingTracker.recordDirectMapping("test.field", "target.field", "value", "test");
        mappingTracker.recordTransformMapping(
                "source.field", "target.field", "transformed.value", "upper");

        // 验证有记录
        MappingResult result1 = mappingTracker.generateMappingResult();
        assertEquals(1, result1.getSuccessMappings().size());
        assertEquals(1, result1.getTransformMappings().size());

        // 重置后验证清空
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
        // 添加各种类型的映射记录
        mappingTracker.recordDirectMapping("direct1", "target1", "value1", "test");
        mappingTracker.recordDirectMapping("direct2", "target2", "value2", "test");
        mappingTracker.recordTransformMapping("transform1", "target3", "transformValue1", "upper");
        mappingTracker.recordDefaultValue("default1", "defaultValue1", "default test");
        mappingTracker.recordMissingField("missing1", "missing test");
        mappingTracker.recordUnmappedField("unmapped1", "unmappedValue1", "unmapped test");

        String statistics = mappingTracker.getStatisticsText();
        assertTrue(statistics.contains("直接映射: 2"));
        assertTrue(statistics.contains("转换映射: 1"));
        assertTrue(statistics.contains("默认值: 1"));
        assertTrue(statistics.contains("缺失: 1"));
        assertTrue(statistics.contains("未映射: 1"));

        MappingTracker.MappingStatistics stats = mappingTracker.getStatistics();
        assertEquals(2, stats.getDirectMappings());
        assertEquals(1, stats.getTransformMappings());
        assertEquals(1, stats.getDefaultValues());
        assertEquals(1, stats.getMissingFields());
        assertEquals(1, stats.getUnmappedFields());
    }
}
