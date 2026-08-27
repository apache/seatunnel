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
    }

    /** Extract LOCATION path from template. If it contains ${table_location}, replace it. */
    public static String extractLocationFromTemplate(
            String template, String database, String table) {
        if (template == null) {
            return null;
        }
        String patternStr = "LOCATION\\s+'([^']+)'";
        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile(
                        patternStr, java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(template);
        if (matcher.find()) {
            String raw = matcher.group(1);
            String defaultLocation = getDefaultTableLocation(database, table);
            return raw.replace("${table_location}", defaultLocation);
        }
        return null;
    }

    /**
     * Extract table type from template. Returns EXTERNAL_TABLE if template contains "CREATE
     * EXTERNAL TABLE" (case-insensitive), otherwise MANAGED_TABLE.
     */
    public static String extractTableTypeFromTemplate(String template) {
        if (template == null) {
            return "MANAGED_TABLE";
        }
        String upper = template.toUpperCase();
        if (upper.contains("CREATE EXTERNAL TABLE")) {
            return "EXTERNAL_TABLE";
        }
        return "MANAGED_TABLE";
    }

    /** Extract TBLPROPERTIES key-value pairs from template (best effort). */
    public static java.util.Map<String, String> extractTblPropertiesFromTemplate(String template) {
        java.util.Map<String, String> props = new java.util.HashMap<>();
        if (template == null) {
            return props;
        }
        String patternStr = "TBLPROPERTIES\\s*\\(([^)]*)\\)";
        java.util.regex.Pattern pattern =
                java.util.regex.Pattern.compile(
                        patternStr,
                        java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher matcher = pattern.matcher(template);
        if (matcher.find()) {
            String body = matcher.group(1);
            // Split on commas not inside quotes is complex; here we split on commas and trim
            for (String entry : body.split(",")) {
                String e = entry.trim();
                if (e.isEmpty()) {
                    continue;
                }
                // Patterns like 'key' = 'value' or "key"="value"
                String kvPattern = "['\"]?([^'\"=]+)['\"]?\\s*=\\s*['\"]([^'\"]*)['\"]";
                java.util.regex.Pattern kvp = java.util.regex.Pattern.compile(kvPattern);
                java.util.regex.Matcher km = kvp.matcher(e);
                if (km.find()) {
                    String k = km.group(1).trim();
                    String v = km.group(2).trim();
                    props.put(k, v);
                }
            }
        }
        return props;
    }

    /**
     * Build complete CREATE TABLE SQL from template and schema. This method generates a complete
     * SQL statement that can be executed via JDBC.
     */
    public static String buildCreateTableSQL(
            String template,
            String database,
            String table,
            org.apache.seatunnel.api.table.catalog.TableSchema tableSchema) {

        if (template == null || template.trim().isEmpty()) {
            throw new IllegalArgumentException("Template cannot be null or empty");
        }

        // Extract partition fields
        List<String> partitionFields = extractPartitionFieldsFromTemplate(template);

        // Generate field definitions
        String fieldsDefinition = generateFieldsDefinition(tableSchema, partitionFields);
        String partitionDefinition = generatePartitionDefinition(tableSchema, partitionFields);

        // Get table location
        String tableLocation = extractLocationFromTemplate(template, database, table);
        if (tableLocation == null) {
            tableLocation = getDefaultTableLocation(database, table);
        }

        // Replace template variables
        String sql =
                replaceTemplateVariables(
                        template,
                        database,
                        table,
                        fieldsDefinition,
                        partitionDefinition,
                        tableLocation);

        return sql;
    }
}
