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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.exception.CommonError;

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

    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile(
                    "decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern VARCHAR_PATTERN =
            Pattern.compile("varchar\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHAR_PATTERN =
            Pattern.compile("char\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FIXED_PATTERN =
            Pattern.compile("fixed\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("timestamp(_tz)?\\s*\\(\\s*(\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);

    // JSON field names
    private static final String COLUMNS = "columns";
    private static final String INDEXES = "indexes";
    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String NULLABLE = "nullable";
    private static final String INDEX_TYPE = "indexType";
    private static final String FIELD_NAMES = "fieldNames";

    // Complex type field names
    private static final String ELEMENT_TYPE = "elementType";
    private static final String KEY_TYPE = "keyType";
    private static final String VALUE_TYPE = "valueType";
    private static final String FIELDS = "fields";

    // index type
    private static final String PRIMARY_KEY = "PRIMARY_KEY";
    private static final String UNIQUE_KEY = "UNIQUE_KEY";

    @Override
    public TableSchema convertor(JsonNode metaInfo) {
        List<Column> columns = new ArrayList<>();
        PrimaryKey primaryKey = null;
        List<ConstraintKey> constraintKeys = new ArrayList<>();
        // Parse columns
        JsonNode columnsNode = metaInfo.get(COLUMNS);
        if (columnsNode != null && columnsNode.isArray()) {
            if (columnsNode.isEmpty()) {
                throw CommonError.illegalArgument(
                        "columns", "GravitinoTableSchemaConvertor.convertor");
            }
            for (JsonNode columnNode : columnsNode) {
                columns.add(parseColumn(columnNode));
            }
        }
        // Parse indexes
        JsonNode indexesNode = metaInfo.get(INDEXES);
        if (indexesNode != null && indexesNode.isArray()) {
            for (JsonNode indexNode : indexesNode) {
                String indexType = getTextValue(indexNode, INDEX_TYPE);
                if (PRIMARY_KEY.equalsIgnoreCase(indexType)) {
                    primaryKey = parsePrimaryKey(indexNode);
                } else if (UNIQUE_KEY.equalsIgnoreCase(indexType)) {
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
        TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, tablePath);
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
        String name = getTextValue(columnNode, NAME);
        boolean nullable = columnNode.has(NULLABLE) && columnNode.get(NULLABLE).asBoolean();
        JsonNode typeNode = columnNode.get(TYPE);
        if (typeNode == null) {
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), "null", name);
        }
        SeaTunnelDataType<?> dataType = convertGravitinoType(name, typeNode);
        // Extract column length and scale from type string
        // Returns null if the type doesn't support length/scale specification
        Long columnLength = null;
        Integer scale = null;
        if (typeNode.isTextual()) {
            Pair<Long, Integer> result = extractLengthAndScale(typeNode.asText());
            if (result != null) {
                columnLength = result.getLeft();
                scale = result.getRight();
            }
        }
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
     * @param fieldName the field name for error reporting
     * @param typeNode the JSON node representing the type (string or object)
     * @return the corresponding SeaTunnel data type
     */
    private SeaTunnelDataType<?> convertGravitinoType(String fieldName, JsonNode typeNode) {
        if (typeNode.isObject()) {
            // Handle complex type (JSON object): list, map, struct, external, etc.
            return convertComplexType(fieldName, typeNode);
        } else if (typeNode.isTextual()) {
            // Handle simple type (string): boolean, int, string, etc.
            return convertSimpleType(fieldName, typeNode);
        } else {
            // Invalid type: neither Object nor Textual
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), typeNode.toString(), fieldName);
        }
    }

    /** Convert complex type (JSON object with type field). */
    private SeaTunnelDataType<?> convertComplexType(String fieldName, JsonNode typeNode) {
        JsonNode typeField = typeNode.get(TYPE);
        if (typeField == null || !typeField.isTextual()) {
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), typeNode.toString(), fieldName);
        }
        String type = typeField.asText().toLowerCase();
        switch (type) {
            case "list":
                JsonNode elementType = typeNode.get(ELEMENT_TYPE);
                if (elementType == null) {
                    throw CommonError.convertToSeaTunnelTypeError(
                            MetaLakeType.GRAVITINO.getType(),
                            "list without elementType",
                            fieldName);
                }
                return ArrayType.of(convertGravitinoType(fieldName, elementType));
            case "map":
                JsonNode keyType = typeNode.get(KEY_TYPE);
                JsonNode valueType = typeNode.get(VALUE_TYPE);
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
                JsonNode fields = typeNode.get(FIELDS);
                if (fields == null || !fields.isArray()) {
                    throw CommonError.convertToSeaTunnelTypeError(
                            MetaLakeType.GRAVITINO.getType(),
                            "struct without fields array",
                            fieldName);
                }
                List<String> fieldNames = new ArrayList<>();
                List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();
                for (JsonNode field : fields) {
                    String fName = getTextValue(field, NAME);
                    if (fName == null) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "struct field without name",
                                fieldName);
                    }
                    JsonNode fType = field.get(TYPE);
                    if (fType == null) {
                        throw CommonError.convertToSeaTunnelTypeError(
                                MetaLakeType.GRAVITINO.getType(),
                                "struct field '" + fName + "' without type",
                                fieldName);
                    }
                    fieldNames.add(fName);
                    fieldTypes.add(convertGravitinoType(fieldName + "." + fName, fType));
                }
                return new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType<?>[0]));

            case "external":
                // External types like PostgreSQL jsonb are treated as string
                return BasicType.STRING_TYPE;
            case "union":
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        MetaLakeType.GRAVITINO.getType(), type, fieldName);
        }
    }

    /** Convert simple type (string like "boolean", "integer", "decimal(10,2)", etc.). */
    private SeaTunnelDataType<?> convertSimpleType(String fieldName, JsonNode typeNode) {
        String gravitinoType = typeNode.asText();
        String normalizedType = gravitinoType.trim().toLowerCase();
        // Remove parameters for simple type matching
        String baseType = normalizedType.split("\\(")[0].trim();

        // Handle decimal type: decimal(precision, scale) - only match regex for decimal type
        if ("decimal".equals(baseType)) {
            Matcher decimalMatcher = DECIMAL_PATTERN.matcher(gravitinoType);
            if (decimalMatcher.find()) {
                int precision = Integer.parseInt(decimalMatcher.group(1));
                int scale = Integer.parseInt(decimalMatcher.group(2));
                return new DecimalType(precision, scale);
            }
            // decimal without parameters or invalid format, throw error
            throw CommonError.convertToSeaTunnelTypeError(
                    MetaLakeType.GRAVITINO.getType(), gravitinoType, fieldName);
        }

        // Remove 'unsigned' suffix to simplify type matching
        String cleanType = baseType.replaceAll("unsigned", "").trim();

        switch (cleanType) {
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

    /**
     * Extract column length and scale from type string.
     *
     * <p>Supports extracting:
     *
     * <ul>
     *   <li>Length: varchar(n), char(n), fixed(n), timestamp(n), timestamp_tz(n), time(n)
     *   <li>Scale: decimal(precision,scale) - returns scale, precision can be obtained via
     *       DecimalType
     * </ul>
     *
     * @param type the type string (e.g., "varchar(255)", "decimal(10,2)", "timestamp(6)")
     * @return a Pair where left is length (Long) and right is scale (Integer), or null if neither
     *     exists
     */
    private Pair<Long, Integer> extractLengthAndScale(String type) {
        // Extract base type before the parenthesis
        String baseType = type.split("\\(")[0].trim().toLowerCase();
        // Remove 'unsigned' suffix for type matching
        String cleanType = baseType.replaceAll("unsigned", "").trim();

        switch (cleanType) {
            case "decimal":
                Matcher decimalMatcher = DECIMAL_PATTERN.matcher(type);
                if (decimalMatcher.find()) {
                    return Pair.of(
                            Long.parseLong(decimalMatcher.group(1)),
                            Integer.parseInt(decimalMatcher.group(2)));
                }
                break;
            case "varchar":
                Matcher varcharMatcher = VARCHAR_PATTERN.matcher(type);
                if (varcharMatcher.find()) {
                    return Pair.of(Long.parseLong(varcharMatcher.group(1)), null);
                }
                break;
            case "char":
                Matcher charMatcher = CHAR_PATTERN.matcher(type);
                if (charMatcher.find()) {
                    return Pair.of(Long.parseLong(charMatcher.group(1)), null);
                }
                break;
            case "fixed":
                Matcher fixedMatcher = FIXED_PATTERN.matcher(type);
                if (fixedMatcher.find()) {
                    return Pair.of(Long.parseLong(fixedMatcher.group(1)), null);
                }
                break;
            case "timestamp":
            case "timestamp_tz":
                Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(type);
                if (timestampMatcher.find()) {
                    return Pair.of(Long.parseLong(timestampMatcher.group(2)), null);
                }
                break;
            default:
                // Types not supporting length/scale parameters
                break;
        }
        return null;
    }

    /** Parse primary key from index node. */
    private PrimaryKey parsePrimaryKey(JsonNode indexNode) {
        String indexName = getTextValue(indexNode, NAME);
        List<String> columnNames = new ArrayList<>();
        JsonNode fieldNamesNode = indexNode.get(FIELD_NAMES);
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
        String indexName = getTextValue(indexNode, NAME);
        List<ConstraintKey.ConstraintKeyColumn> columns = new ArrayList<>();
        JsonNode fieldNamesNode = indexNode.get(FIELD_NAMES);
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

    /** Simple immutable pair class to avoid coupling with scala.Tuple2 or Apache Commons Pair. */
    private static class Pair<L, R> {
        private final L left;
        private final R right;

        private Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public static <L, R> Pair<L, R> of(L left, R right) {
            return new Pair<>(left, right);
        }

        public L getLeft() {
            return left;
        }

        public R getRight() {
            return right;
        }
    }
}
