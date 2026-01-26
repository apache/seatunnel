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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.exception.CommonError;

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

    // Complex type JSON fields
    private static final String JSON_FIELD_ELEMENT_TYPE = "elementType";
    private static final String JSON_FIELD_KEY_TYPE = "keyType";
    private static final String JSON_FIELD_VALUE_TYPE = "valueType";
    private static final String JSON_FIELD_FIELDS = "fields";
    private static final String JSON_FIELD_TYPES = "types";

    private static final String INDEX_TYPE_PRIMARY_KEY = "PRIMARY_KEY";
    private static final String INDEX_TYPE_UNIQUE_KEY = "UNIQUE_KEY";

    @Override
    public TableSchema convertor(JsonNode metaInfo) throws IOException {
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
        return schemaBuilder.build();
    }

    @Override
    public CatalogTable buildCatalogTable(
            String catalogName, TablePath tablePath, TableSchema tableSchema) {
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
    private Column parseColumn(JsonNode columnNode) {
        String name = getTextValue(columnNode, JSON_FIELD_NAME);
        boolean nullable =
                columnNode.has(JSON_FIELD_NULLABLE)
                        && columnNode.get(JSON_FIELD_NULLABLE).asBoolean();

        JsonNode typeNode = columnNode.get(JSON_FIELD_TYPE);
        if (typeNode == null) {
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), "null", name);
        }

        SeaTunnelDataType<?> dataType = convertGravitinoType(name, typeNode);

        String typeStrForLength = typeNode.isTextual() ? typeNode.asText() : null;
        Long columnLength = extractColumnLength(typeStrForLength);
        Integer scale = extractScale(typeStrForLength);

        return PhysicalColumn.builder()
                .name(name)
                .dataType(dataType)
                .columnLength(columnLength)
                .scale(scale)
                .nullable(nullable)
                .build();
    }

    /**
     * Convert Gravitino type to SeaTunnel DataType.
     *
     * <p>Handles both simple types (string) and complex types (JSON object):
     *
     * <ul>
     *   <li>Simple types: "integer", "varchar(100)", "decimal(10,2)", "map<string,int>",
     *       "array<int>"
     *   <li>Complex types:
     *       <ul>
     *         <li>list: {"type": "list", "containsNull": boolean, "elementType": type JSON}
     *         <li>map: {"type": "map", "keyType": type JSON, "valueType": type JSON,
     *             "valueContainsNull": boolean}
     *         <li>struct: {"type": "struct", "fields": [{"name": string, "type": type JSON,
     *             "nullable": boolean, "comment": string}]}
     *         <li>union: {"type": "union", "types": [type JSON, ...]}
     *       </ul>
     * </ul>
     *
     * @param fieldName the field name for error reporting
     * @param typeNode the JSON node representing the type (string or object)
     * @return the corresponding SeaTunnel data type
     */
    private SeaTunnelDataType<?> convertGravitinoType(String fieldName, JsonNode typeNode) {
        // Handle complex type (JSON object)
        if (typeNode.isObject()) {
            JsonNode typeField = typeNode.get(JSON_FIELD_TYPE);
            if (typeField == null || !typeField.isTextual()) {
                throw CommonError.convertToSeaTunnelTypeError(
                        MetaLakeType.GRAVITINO.getType(), typeNode.toString(), fieldName);
            }
            String type = typeField.asText().toLowerCase();
            switch (type) {
                case "list":
                    JsonNode elementType = typeNode.get(JSON_FIELD_ELEMENT_TYPE);
                    if (elementType == null) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "list without elementType",
                                fieldName);
                    }
                    return ArrayType.of(convertGravitinoType(fieldName, elementType));

                case "map":
                    JsonNode keyType = typeNode.get(JSON_FIELD_KEY_TYPE);
                    JsonNode valueType = typeNode.get(JSON_FIELD_VALUE_TYPE);
                    if (keyType == null || valueType == null) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "map without keyType or valueType",
                                fieldName);
                    }
                    return new MapType<>(
                            convertGravitinoType(fieldName, keyType),
                            convertGravitinoType(fieldName, valueType));

                case "struct":
                    JsonNode fields = typeNode.get(JSON_FIELD_FIELDS);
                    if (fields == null || !fields.isArray()) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "struct without valid fields array",
                                fieldName);
                    }
                    // SeaTunnel doesn't have a native struct type, convert to string
                    return BasicType.STRING_TYPE;

                case "union":
                    JsonNode types = typeNode.get(JSON_FIELD_TYPES);
                    if (types == null || !types.isArray() || types.isEmpty()) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "union without valid types array",
                                fieldName);
                    }
                    // Use the first successfully converted type from the union
                    for (JsonNode unionType : types) {
                        if (unionType != null) {
                            try {
                                return convertGravitinoType(fieldName, unionType);
                            } catch (Exception e) {
                                // Continue to next type
                            }
                        }
                    }
                    // Fallback to string
                    return BasicType.STRING_TYPE;
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            MetaLakeType.GRAVITINO.getType(), type, fieldName);
            }
        }

        // Handle simple type (string)
        if (!typeNode.isTextual()) {
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), typeNode.toString(), fieldName);
        }
        String gravitinoType = typeNode.asText();
        String normalizedType = gravitinoType.trim().toLowerCase();
        // Handle decimal type: decimal(precision, scale)
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
            case "byte unsigned":
                return BasicType.BYTE_TYPE;
            case "short":
            case "short unsigned":
                return BasicType.SHORT_TYPE;
            case "integer":
            case "integer unsigned":
                return BasicType.INT_TYPE;
            case "long":
            case "long unsigned":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "string":
            case "varchar":
            case "char":
            case "uuid":
            case "interval_year":
            case "interval_day":
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
                return PrimitiveByteArrayType.INSTANCE;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        MetaLakeType.GRAVITINO.getType(), baseType, fieldName);
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

    /**
     * Helper method to convert type string to SeaTunnel DataType. This is used for recursive calls
     * when parsing nested types in string format.
     */
    private SeaTunnelDataType<?> convertTypeFromString(String fieldName, String typeStr) {
        // Delegate to the main method by creating a text node
        return convertGravitinoType(fieldName, getTextNode(typeStr));
    }

    /** Create a text node from string for type conversion. */
    private JsonNode getTextNode(String value) {
        return new org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode(value);
    }

    /** Get text value from JSON node field. */
    private String getTextValue(JsonNode node, String fieldName) {
        JsonNode fieldNode = node.get(fieldName);
        return fieldNode != null ? fieldNode.asText() : null;
    }
}
