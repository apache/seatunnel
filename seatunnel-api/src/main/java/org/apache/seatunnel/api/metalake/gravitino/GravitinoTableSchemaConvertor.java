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
package org.apache.seatunnel.api.metalake.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.metalake.MetaLakeTableSchemaConvertor;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converter for transforming Gravitino table metadata into SeaTunnel CatalogTable format.
 *
 * <p>Reference documentation:
 *
 * <ul>
 *   <li><a
 *       href="https://gravitino.apache.org/docs/1.1.0/manage-relational-metadata-using-gravitino/#apache-gravitino-table-column-type">Gravitino
 *       Column Types</a>
 *   <li><a
 *       href="https://gravitino.apache.org/docs/1.1.0/table-partitioning-distribution-sort-order-indexes#indexes">Gravitino
 *       Indexes</a>
 * </ul>
 */
public class GravitinoTableSchemaConvertor implements MetaLakeTableSchemaConvertor {

    // 样例json：
    // {"code":0,"table":{"name":"all_type","columns":[{"name":"id","type":"integer","nullable":false,"autoIncrement":false},{"name":"big_number","type":"long","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"small_number","type":"integer","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"tiny_number","type":"short","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"float_value","type":"float","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"double_value","type":"double","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"decimal_value","type":"decimal(10,2)","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"event_date","type":"date","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"user_name","type":"varchar(300)","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"code","type":"varchar(15)","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"description","type":"string","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}},{"name":"event_json","type":"string","nullable":true,"autoIncrement":false,"defaultValue":{"type":"literal","dataType":"null","value":"NULL"}}],"properties":{},"audit":{"lastModifier":"anonymous","lastModifiedTime":"2026-01-25T12:00:04.771957297Z"},"distribution":{"strategy":"none","number":0,"funcArgs":[]},"sortOrders":[],"partitioning":[],"indexes":[{"indexType":"PRIMARY_KEY","name":"all_type_pk","fieldNames":[["id"]]},{"indexType":"UNIQUE_KEY","name":"all_type_big_number_idx","fieldNames":[["big_number"]]}]}}
    // 其中JsonNode metaInfo 是json中的table节点，我只关注columns和indexes
    // columns中我只关注name type nullable
    // https://gravitino.apache.org/docs/1.1.0/manage-relational-metadata-using-gravitino/#apache-gravitino-table-column-type 这是columns的规则
    // https://gravitino.apache.org/docs/1.1.0/table-partitioning-distribution-sort-order-indexes#indexes 这是index的规则
    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile(
                    "decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern VARCHAR_PATTERN =
            Pattern.compile("varchar\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHAR_PATTERN =
            Pattern.compile("char\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FIXED_PATTERN =
            Pattern.compile("fixed\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);

    private static final String JSON_FIELD_COLUMNS = "columns";
    private static final String JSON_FIELD_INDEXES = "indexes";
    private static final String JSON_FIELD_NAME = "name";
    private static final String JSON_FIELD_TYPE = "type";
    private static final String JSON_FIELD_NULLABLE = "nullable";
    private static final String JSON_FIELD_INDEX_TYPE = "indexType";
    private static final String JSON_FIELD_FIELD_NAMES = "fieldNames";

    private static final String INDEX_TYPE_PRIMARY_KEY = "PRIMARY_KEY";
    private static final String INDEX_TYPE_UNIQUE_KEY = "UNIQUE_KEY";

    @Override
    public CatalogTable convertor(JsonNode metaInfo, TablePath tablePath) throws IOException {
        List<Column> columns = new ArrayList<>();
        PrimaryKey primaryKey = null;
        List<ConstraintKey> constraintKeys = new ArrayList<>();

        // Parse columns
        JsonNode columnsNode = metaInfo.get(JSON_FIELD_COLUMNS);
        if (columnsNode != null && columnsNode.isArray()) {
            for (JsonNode columnNode : columnsNode) {
                columns.add(parseColumn(columnNode));
            }
        }

        // Parse indexes
        JsonNode indexesNode = metaInfo.get(JSON_FIELD_INDEXES);
        if (indexesNode != null && indexesNode.isArray()) {
            for (JsonNode indexNode : indexesNode) {
                String indexType = getTextValue(indexNode, JSON_FIELD_INDEX_TYPE);
                if (INDEX_TYPE_PRIMARY_KEY.equalsIgnoreCase(indexType)) {
                    primaryKey = parsePrimaryKey(indexNode);
                } else if (INDEX_TYPE_UNIQUE_KEY.equalsIgnoreCase(indexType)) {
                    constraintKeys.add(parseUniqueKey(indexNode));
                }
            }
        }

        // Build table schema
        TableSchema.Builder schemaBuilder = TableSchema.builder().columns(columns);
        if (primaryKey != null) {
            schemaBuilder.primaryKey(primaryKey);
        }
        if (!constraintKeys.isEmpty()) {
            schemaBuilder.constraintKey(constraintKeys);
        }
        TableSchema tableSchema = schemaBuilder.build();

        // Build table identifier
        // Note: In Gravitino context, TablePath.databaseName maps to catalog name
        String catalogName =
                tablePath.getDatabaseName() != null ? tablePath.getDatabaseName() : "gravitino";
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogName, tablePath.getSchemaName(), tablePath.getTableName());

        // Build catalog table
        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                null,
                catalogName);
    }

    /** Parse a column node from Gravitino JSON. */
    private Column parseColumn(JsonNode columnNode) throws IOException {
        String name = getTextValue(columnNode, JSON_FIELD_NAME);
        String type = getTextValue(columnNode, JSON_FIELD_TYPE);
        boolean nullable =
                columnNode.has(JSON_FIELD_NULLABLE)
                        && columnNode.get(JSON_FIELD_NULLABLE).asBoolean();

        SeaTunnelDataType<?> dataType = convertGravitinoType(type);
        Long columnLength = extractColumnLength(type);
        Integer scale = extractScale(type);

        return PhysicalColumn.builder()
                .name(name)
                .dataType(dataType)
                .columnLength(columnLength)
                .scale(scale)
                .nullable(nullable)
                .build();
    }

    /**
     * Convert Gravitino type string to SeaTunnel DataType.
     *
     * @param gravitinoType the Gravitino type string
     * @return the corresponding SeaTunnel data type
     * @throws IOException if the type is null or unsupported
     * @see <a
     *     href="https://gravitino.apache.org/docs/1.1.0/manage-relational-metadata-using-gravitino/#apache-gravitino-table-column-type">Gravitino
     *     Column Types</a>
     */
    private SeaTunnelDataType<?> convertGravitinoType(String gravitinoType) throws IOException {
        if (gravitinoType == null) {
            throw new IOException("Gravitino type cannot be null");
        }

        String normalizedType = gravitinoType.trim().toLowerCase();

        // Handle complex types with parameters
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(gravitinoType);
        if (decimalMatcher.find()) {
            int precision = Integer.parseInt(decimalMatcher.group(1));
            int scale = Integer.parseInt(decimalMatcher.group(2));
            return new DecimalType(precision, scale);
        }

        // Remove parameters for simple type matching
        String baseType = normalizedType.split("\\(")[0].trim();

        switch (baseType) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "integer":
                return BasicType.INT_TYPE;
            case "long":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "string":
            case "varchar":
            case "char":
                return BasicType.STRING_TYPE;
            case "date":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "time":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "timestamp":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "timestamp_tz":
                return LocalTimeType.OFFSET_DATE_TIME_TYPE;
            case "binary":
            case "fixed":
                // Binary types - use STRING_TYPE as placeholder
                // TODO: Add proper binary type support when needed
                return BasicType.STRING_TYPE;
            case "uuid":
                // UUID - use STRING_TYPE
                return BasicType.STRING_TYPE;
            case "interval_year":
            case "interval_day":
                // Interval types - use STRING_TYPE as placeholder
                // TODO: Add proper interval type support when needed
                return BasicType.STRING_TYPE;
            default:
                // Handle complex types (struct, list, map, union, external, unparsed)
                // For now, return STRING_TYPE as fallback
                return BasicType.STRING_TYPE;
        }
    }

    /** Extract column length from type string (for varchar, char, etc.). */
    private Long extractColumnLength(String type) {
        Matcher varcharMatcher = VARCHAR_PATTERN.matcher(type);
        if (varcharMatcher.find()) {
            return Long.parseLong(varcharMatcher.group(1));
        }
        Matcher charMatcher = CHAR_PATTERN.matcher(type);
        if (charMatcher.find()) {
            return Long.parseLong(charMatcher.group(1));
        }
        Matcher fixedMatcher = FIXED_PATTERN.matcher(type);
        if (fixedMatcher.find()) {
            return Long.parseLong(fixedMatcher.group(1));
        }
        return null;
    }

    /** Extract scale from type string (for decimal). */
    private Integer extractScale(String type) {
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(type);
        if (decimalMatcher.find()) {
            return Integer.parseInt(decimalMatcher.group(2));
        }
        return null;
    }

    /** Parse primary key from index node. */
    private PrimaryKey parsePrimaryKey(JsonNode indexNode) {
        String indexName = getTextValue(indexNode, JSON_FIELD_NAME);
        List<String> columnNames = new ArrayList<>();

        JsonNode fieldNamesNode = indexNode.get(JSON_FIELD_FIELD_NAMES);
        if (fieldNamesNode != null && fieldNamesNode.isArray()) {
            for (JsonNode fieldNameArray : fieldNamesNode) {
                if (fieldNameArray.isArray() && !fieldNameArray.isEmpty()) {
                    columnNames.add(fieldNameArray.get(0).asText());
                }
            }
        }

        return PrimaryKey.of(indexName, columnNames);
    }

    /** Parse unique key from index node. */
    private ConstraintKey parseUniqueKey(JsonNode indexNode) {
        String indexName = getTextValue(indexNode, JSON_FIELD_NAME);
        List<ConstraintKey.ConstraintKeyColumn> columns = new ArrayList<>();

        JsonNode fieldNamesNode = indexNode.get(JSON_FIELD_FIELD_NAMES);
        if (fieldNamesNode != null && fieldNamesNode.isArray()) {
            for (JsonNode fieldNameArray : fieldNamesNode) {
                if (fieldNameArray.isArray() && !fieldNameArray.isEmpty()) {
                    String columnName = fieldNameArray.get(0).asText();
                    columns.add(
                            ConstraintKey.ConstraintKeyColumn.of(
                                    columnName, ConstraintKey.ColumnSortType.ASC));
                }
            }
        }

        return ConstraintKey.of(ConstraintKey.ConstraintType.UNIQUE_KEY, indexName, columns);
    }

    /** Get text value from JSON node field. */
    private String getTextValue(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        return fieldNode != null ? fieldNode.asText() : null;
    }
}
