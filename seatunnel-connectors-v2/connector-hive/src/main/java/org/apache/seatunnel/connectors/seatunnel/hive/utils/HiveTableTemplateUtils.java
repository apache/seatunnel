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

import org.apache.seatunnel.api.table.catalog.TableSchema;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class HiveTableTemplateUtils {

    /** Get default Hive table creation template for non-partitioned tables */
    public static String getDefaultNonPartitionedTemplate() {
        return "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                + "    ${rowtype_fields}\n"
                + ")\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '${table_location}'\n"
                + "TBLPROPERTIES (\n"
                + "    'seatunnel.creation.mode' = 'template',\n"
                + "    'seatunnel.created.time' = '${current_timestamp}'\n"
                + ")";
    }

    /** Get default Hive table creation template for partitioned tables */
    public static String getDefaultPartitionedTemplate() {
        return "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                + "    ${rowtype_fields}\n"
                + ")\n"
                + "PARTITIONED BY (${rowtype_partition_fields})\n"
                + "STORED AS PARQUET\n"
                + "LOCATION '${table_location}'\n"
                + "TBLPROPERTIES (\n"
                + "    'seatunnel.creation.mode' = 'template',\n"
                + "    'seatunnel.created.time' = '${current_timestamp}'\n"
                + ")";
    }

    /** Generate field definitions for table creation */
    public static String generateFieldsDefinition(
            TableSchema tableSchema, List<String> partitionFields) {
        return tableSchema.getColumns().stream()
                .filter(column -> !partitionFields.contains(column.getName()))
                .map(
                        column -> {
                            String hiveType =
                                    HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
                            String comment =
                                    column.getComment() != null
                                            ? " COMMENT '" + column.getComment() + "'"
                                            : "";
                            return String.format(
                                    "    `%s` %s%s", column.getName(), hiveType, comment);
                        })
                .collect(Collectors.joining(",\n"));
    }

    /** Generate partition field definitions for table creation */
    public static String generatePartitionDefinition(
            TableSchema tableSchema, List<String> partitionFields) {
        if (partitionFields == null || partitionFields.isEmpty()) {
            return "";
        }

        return partitionFields.stream()
                .map(
                        partitionField -> {
                            // Try to get type from source schema first
                            String hiveType =
                                    tableSchema.getColumns().stream()
                                            .filter(col -> col.getName().equals(partitionField))
                                            .findFirst()
                                            .map(
                                                    col ->
                                                            HiveTypeConvertor.seatunnelToHiveType(
                                                                    col.getDataType()))
                                            .orElse("string"); // Default to string for new
                            // partition fields

                            return String.format(
                                    "    `%s` %s COMMENT 'Partition field'",
                                    partitionField, hiveType);
                        })
                .collect(Collectors.joining(",\n"));
    }

    /** Replace template variables with actual values */
    public static String replaceTemplateVariables(
            String template,
            String database,
            String table,
            String fieldsDefinition,
            String partitionDefinition,
            String tableLocation) {

        return template.replace("${database}", database)
                .replace("${table}", table)
                .replace("${rowtype_fields}", fieldsDefinition)
                .replace("${rowtype_partition_fields}", partitionDefinition)
                .replace("${table_location}", tableLocation)
                .replace("${current_timestamp}", String.valueOf(System.currentTimeMillis()));
    }

    /** Get default table location */
    public static String getDefaultTableLocation(String database, String table) {
        // e2e 友好：优先使用本地文件系统，避免依赖外部 HDFS；同时满足 StorageFactory 的 file: 识别
        return String.format("file:/tmp/hive/warehouse/%s.db/%s", database, table);
    }

    /**
     * Extract partition fields from template This method tries to parse partition fields from
     * PARTITIONED BY clause
     */
    public static List<String> extractPartitionFieldsFromTemplate(String template) {
        // Simple regex to extract partition fields from PARTITIONED BY clause
        // This is a basic implementation - could be enhanced for more complex cases
        String partitionPattern = "PARTITIONED\\s+BY\\s*\\(([^)]+)\\)";
        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile(
                        partitionPattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(template);

        if (matcher.find()) {
            String partitionClause = matcher.group(1);
            // Extract field names (basic parsing)
            return java.util.Arrays.stream(partitionClause.split(","))
                    .map(field -> field.trim().split("\\s+")[0].replaceAll("`", ""))
                    .collect(Collectors.toList());
        }

        return java.util.Collections.emptyList();
    }

    /** Validate template syntax (basic validation) */
    public static void validateTemplate(String template) {
        if (template == null || template.trim().isEmpty()) {
            throw new IllegalArgumentException("Template cannot be null or empty");
        }

        // Check for required CREATE TABLE statement
        if (!template.toUpperCase().contains("CREATE TABLE")) {
            throw new IllegalArgumentException("Template must contain CREATE TABLE statement");
        }

        // Check for required variables
        if (!template.contains("${database}") || !template.contains("${table}")) {
            throw new IllegalArgumentException(
                    "Template must contain ${database} and ${table} variables");
        }

        log.info("Template validation passed");
    }
}
