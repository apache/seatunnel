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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for HiveTableTemplateUtils */
public class HiveTableTemplateUtilsTest {

    private TableSchema tableSchema;

    @BeforeEach
    void setUp() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, "ID field"),
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, 0, true, null, "Name field"),
                        PhysicalColumn.of("age", BasicType.INT_TYPE, 0, true, null, "Age field"),
                        PhysicalColumn.of(
                                "department",
                                BasicType.STRING_TYPE,
                                0,
                                true,
                                null,
                                "Department field"));

        tableSchema = TableSchema.builder().columns(columns).build();
    }

    @Test
    void testGetDefaultNonPartitionedTemplate() {
        String template = HiveTableTemplateUtils.getDefaultNonPartitionedTemplate();

        assertTrue(template.contains("CREATE TABLE IF NOT EXISTS"));
        assertTrue(template.contains("${database}"));
        assertTrue(template.contains("${table}"));
        assertTrue(template.contains("${rowtype_fields}"));
        assertTrue(template.contains("STORED AS PARQUET"));
        assertTrue(template.contains("${table_location}"));
    }

    @Test
    void testGetDefaultPartitionedTemplate() {
        String template = HiveTableTemplateUtils.getDefaultPartitionedTemplate();

        assertTrue(template.contains("CREATE TABLE IF NOT EXISTS"));
        assertTrue(template.contains("${database}"));
        assertTrue(template.contains("${table}"));
        assertTrue(template.contains("${rowtype_fields}"));
        assertTrue(template.contains("PARTITIONED BY"));
        assertTrue(template.contains("${rowtype_partition_fields}"));
        assertTrue(template.contains("STORED AS PARQUET"));
        assertTrue(template.contains("${table_location}"));
    }

    @Test
    void testGenerateFieldsDefinitionWithoutPartitions() {
        List<String> partitionFields = Collections.emptyList();
        String fieldsDefinition =
                HiveTableTemplateUtils.generateFieldsDefinition(tableSchema, partitionFields);

        assertTrue(fieldsDefinition.contains("`id` bigint COMMENT 'ID field'"));
        assertTrue(fieldsDefinition.contains("`name` string COMMENT 'Name field'"));
        assertTrue(fieldsDefinition.contains("`age` int COMMENT 'Age field'"));
        assertTrue(fieldsDefinition.contains("`department` string COMMENT 'Department field'"));
    }

    @Test
    void testGenerateFieldsDefinitionWithPartitions() {
        List<String> partitionFields = Arrays.asList("department");
        String fieldsDefinition =
                HiveTableTemplateUtils.generateFieldsDefinition(tableSchema, partitionFields);

        assertTrue(fieldsDefinition.contains("`id` bigint COMMENT 'ID field'"));
        assertTrue(fieldsDefinition.contains("`name` string COMMENT 'Name field'"));
        assertTrue(fieldsDefinition.contains("`age` int COMMENT 'Age field'"));
        // department should be excluded from regular fields
        assertTrue(!fieldsDefinition.contains("`department`"));
    }

    @Test
    void testGeneratePartitionDefinition() {
        List<String> partitionFields = Arrays.asList("department");
        String partitionDefinition =
                HiveTableTemplateUtils.generatePartitionDefinition(tableSchema, partitionFields);

        assertTrue(partitionDefinition.contains("`department` string COMMENT 'Partition field'"));
    }

    @Test
    void testGeneratePartitionDefinitionWithNewField() {
        List<String> partitionFields = Arrays.asList("year", "month");
        String partitionDefinition =
                HiveTableTemplateUtils.generatePartitionDefinition(tableSchema, partitionFields);

        assertTrue(partitionDefinition.contains("`year` string COMMENT 'Partition field'"));
        assertTrue(partitionDefinition.contains("`month` string COMMENT 'Partition field'"));
    }

    @Test
    void testReplaceTemplateVariables() {
        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) "
                        + "PARTITIONED BY (${rowtype_partition_fields}) LOCATION '${table_location}'";

        String result =
                HiveTableTemplateUtils.replaceTemplateVariables(
                        template,
                        "test_db",
                        "test_table",
                        "`id` bigint, `name` string",
                        "`department` string",
                        "/user/hive/warehouse/test_db.db/test_table");

        assertTrue(result.contains("`test_db`.`test_table`"));
        assertTrue(result.contains("`id` bigint, `name` string"));
        assertTrue(result.contains("`department` string"));
        assertTrue(result.contains("'/user/hive/warehouse/test_db.db/test_table'"));
    }

    @Test
    void testGetDefaultTableLocation() {
        String location = HiveTableTemplateUtils.getDefaultTableLocation("test_db", "test_table");
        assertEquals("file:/tmp/hive/warehouse/test_db.db/test_table", location);
    }

    @Test
    void testExtractPartitionFieldsFromTemplate() {
        String template =
                "CREATE TABLE test (id bigint) PARTITIONED BY (year string, month string)";
        List<String> partitionFields =
                HiveTableTemplateUtils.extractPartitionFieldsFromTemplate(template);

        assertEquals(2, partitionFields.size());
        assertTrue(partitionFields.contains("year"));
        assertTrue(partitionFields.contains("month"));
    }

    @Test
    void testExtractPartitionFieldsFromTemplateWithBackticks() {
        String template =
                "CREATE TABLE test (id bigint) PARTITIONED BY (`year` string, `month` string)";
        List<String> partitionFields =
                HiveTableTemplateUtils.extractPartitionFieldsFromTemplate(template);

        assertEquals(2, partitionFields.size());
        assertTrue(partitionFields.contains("year"));
        assertTrue(partitionFields.contains("month"));
    }

    @Test
    void testExtractPartitionFieldsFromNonPartitionedTemplate() {
        String template = "CREATE TABLE test (id bigint) STORED AS PARQUET";
        List<String> partitionFields =
                HiveTableTemplateUtils.extractPartitionFieldsFromTemplate(template);

        assertEquals(0, partitionFields.size());
    }

    @Test
    void testValidateTemplateValid() {
        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS PARQUET";

        // Should not throw exception
        HiveTableTemplateUtils.validateTemplate(template);
    }

    @Test
    void testValidateTemplateInvalidNoCreateTable() {
        String template = "INSERT INTO `${database}`.`${table}` VALUES (1, 'test')";

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    HiveTableTemplateUtils.validateTemplate(template);
                });
    }

    @Test
    void testValidateTemplateInvalidNoDatabase() {
        String template =
                "CREATE TABLE IF NOT EXISTS `${table}` (${rowtype_fields}) STORED AS PARQUET";

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    HiveTableTemplateUtils.validateTemplate(template);
                });
    }

    @Test
    void testValidateTemplateInvalidNoTable() {
        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.table (${rowtype_fields}) STORED AS PARQUET";

        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    HiveTableTemplateUtils.validateTemplate(template);
                });
    }

    @Test
    void testValidateTemplateNull() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    HiveTableTemplateUtils.validateTemplate(null);
                });
    }

    @Test
    void testValidateTemplateEmpty() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    HiveTableTemplateUtils.validateTemplate("");
                });
    }

    @Test
    void testExtractTableTypeFromTemplate_external_vs_managed() {
        String managed =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (id int) STORED AS PARQUET";
        String external =
                "CREATE EXTERNAL TABLE IF NOT EXISTS `${database}`.`${table}` (id int) STORED AS PARQUET";
        assertEquals("MANAGED_TABLE", HiveTableTemplateUtils.extractTableTypeFromTemplate(managed));
        assertEquals(
                "EXTERNAL_TABLE", HiveTableTemplateUtils.extractTableTypeFromTemplate(external));
    }

    @Test
    void testExtractLocationFromTemplate_with_and_without_variable() {
        String withVar = "CREATE TABLE t (id int) LOCATION '${table_location}'";
        String withoutVar = "CREATE TABLE t (id int) LOCATION '/custom/warehouse/db.tbl'";
        String extractedWithVar =
                HiveTableTemplateUtils.extractLocationFromTemplate(withVar, "db", "tbl");
        String extractedWithoutVar =
                HiveTableTemplateUtils.extractLocationFromTemplate(withoutVar, "db", "tbl");
        assertEquals("file:/tmp/hive/warehouse/db.db/tbl", extractedWithVar);
        assertEquals("/custom/warehouse/db.tbl", extractedWithoutVar);
    }

    @Test
    void testExtractTblPropertiesFromTemplate_various_pairs() {
        String tpl =
                "CREATE TABLE t (id int) STORED AS PARQUET TBLPROPERTIES (\n"
                        + "  'k1' = 'v1',\n"
                        + "  \"k2\"=\"v2\",\n"
                        + "  'seatunnel.created.time'='123456789'\n"
                        + ")";
        java.util.Map<String, String> props =
                HiveTableTemplateUtils.extractTblPropertiesFromTemplate(tpl);
        assertEquals("v1", props.get("k1"));
        assertEquals("v2", props.get("k2"));
        assertEquals("123456789", props.get("seatunnel.created.time"));
    }
}
