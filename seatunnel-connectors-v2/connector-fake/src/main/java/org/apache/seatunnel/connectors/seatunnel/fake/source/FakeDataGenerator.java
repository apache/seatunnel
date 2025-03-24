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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fake.utils.FakeDataRandomUtils;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.seatunnel.api.table.type.SqlType.TIME;

public class FakeDataGenerator {
    private static final String CURRENT_DATE = "CURRENT_DATE";
    private static final String CURRENT_TIME = "CURRENT_TIME";
    private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    private final ObjectMapper OBJECTMAPPER = new ObjectMapper();

    private final CatalogTable catalogTable;
    private final FakeConfig fakeConfig;
    private final JsonDeserializationSchema jsonDeserializationSchema;
    private final FakeDataRandomUtils fakeDataRandomUtils;
    private String tableId;

    public FakeDataGenerator(FakeConfig fakeConfig) {
        this.catalogTable = fakeConfig.getCatalogTable();
        this.tableId = catalogTable.getTableId().toTablePath().toString();
        this.fakeConfig = fakeConfig;
        this.jsonDeserializationSchema =
                fakeConfig.getFakeRows() == null
                        ? null
                        : new JsonDeserializationSchema(catalogTable, false, false);
        this.fakeDataRandomUtils = new FakeDataRandomUtils(fakeConfig);
    }

    private SeaTunnelRow convertRow(FakeConfig.RowData rowData) {
        try {
            SeaTunnelRow seaTunnelRow =
                    jsonDeserializationSchema.deserialize(rowData.getFieldsJson());
            if (rowData.getKind() != null) {
                seaTunnelRow.setRowKind(RowKind.valueOf(rowData.getKind()));
            }
            seaTunnelRow.setTableId(tableId);
            return seaTunnelRow;
        } catch (IOException e) {
            throw CommonError.jsonOperationError("Fake", rowData.getFieldsJson(), e);
        }
    }

    private SeaTunnelRow randomRow() {
        // Generate random data according to the data type and data colum of the table
        List<Column> physicalColumns = catalogTable.getTableSchema().getColumns();
        List<Object> randomRow = new ArrayList<>(physicalColumns.size());
        for (Column column : physicalColumns) {
            randomRow.add(randomColumnValue(column));
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(randomRow.toArray());
        seaTunnelRow.setTableId(tableId);
        return seaTunnelRow;
    }

    @VisibleForTesting
    public List<SeaTunnelRow> generateFakedRows(int rowNum) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        generateFakedRows(rowNum, rows::add);
        return rows;
    }

    /**
     * @param rowNum The number of pieces of data to be generated by the current task
     * @param consumer The generated data is sent to consumer
     * @return The number of generated data row count
     */
    public long generateFakedRows(int rowNum, Consumer<SeaTunnelRow> consumer) {
        // Use manual configuration data preferentially
        long rowCount = 0;
        if (fakeConfig.getFakeRows() != null) {
            SeaTunnelDataType<?>[] fieldTypes = catalogTable.getSeaTunnelRowType().getFieldTypes();
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            for (FakeConfig.RowData rowData : fakeConfig.getFakeRows()) {
                customField(rowData, fieldTypes, fieldNames);
                consumer.accept(convertRow(rowData));
                rowCount++;
            }
        } else {
            for (int i = 0; i < rowNum; i++) {
                consumer.accept(randomRow());
                rowCount++;
            }
        }
        return rowCount;
    }

    private void customField(
            FakeConfig.RowData rowData, SeaTunnelDataType<?>[] fieldTypes, String[] fieldNames) {
        if (rowData.getFieldsJson() == null) {
            return;
        }

        try {
            JsonNode jsonNode = OBJECTMAPPER.readTree(rowData.getFieldsJson());
            int arity = fieldTypes.length;

            for (int i = 0; i < arity; i++) {
                SeaTunnelDataType<?> fieldType = fieldTypes[i];
                JsonNode field = jsonNode.isArray() ? jsonNode.get(i) : jsonNode.get(fieldNames[i]);

                if (field == null) {
                    continue;
                }

                String newValue = getNewValueForField(fieldType.getSqlType(), field.asText());
                if (newValue != null) {
                    jsonNode = replaceFieldValue(jsonNode, i, fieldNames[i], newValue);
                }
            }

            rowData.setFieldsJson(jsonNode.toString());
        } catch (JsonProcessingException e) {
            throw new FakeConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                    "The data type of the fake data is not supported",
                    e);
        }
    }

    private String getNewValueForField(SqlType sqlType, String fieldValue) {
        switch (sqlType) {
            case TIME:
                return fieldValue.equals(CURRENT_TIME) ? LocalTime.now().toString() : null;
            case DATE:
                return fieldValue.equalsIgnoreCase(CURRENT_DATE)
                        ? LocalDate.now().toString()
                        : null;
            case TIMESTAMP:
                return fieldValue.equalsIgnoreCase(CURRENT_TIMESTAMP)
                        ? LocalDateTime.now().toString()
                        : null;
            case TIMESTAMP_TZ:
                return fieldValue.equalsIgnoreCase(CURRENT_TIMESTAMP)
                        ? OffsetDateTime.now().toString()
                        : null;
            default:
                return null;
        }
    }

    private JsonNode replaceFieldValue(
            JsonNode jsonNode, int index, String fieldName, String newValue) {
        JsonNode newFieldNode = OBJECTMAPPER.convertValue(newValue, JsonNode.class);

        if (jsonNode.isArray()) {
            ((ArrayNode) jsonNode).set(index, newFieldNode);
        } else {
            ((ObjectNode) jsonNode).set(fieldName, newFieldNode);
        }

        return jsonNode;
    }

    @SuppressWarnings("magicnumber")
    private Object randomColumnValue(Column column) {
        SeaTunnelDataType<?> fieldType = column.getDataType();
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                SeaTunnelDataType<?> elementType = arrayType.getElementType();
                int length = fakeConfig.getArraySize();
                Object array = Array.newInstance(elementType.getTypeClass(), length);
                for (int i = 0; i < length; i++) {
                    Object value = randomColumnValue(column.copy(elementType));
                    Array.set(array, i, value);
                }
                return array;
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
                SeaTunnelDataType<?> keyType = mapType.getKeyType();
                SeaTunnelDataType<?> valueType = mapType.getValueType();
                HashMap<Object, Object> objectMap = new HashMap<>();
                int mapSize = fakeConfig.getMapSize();
                for (int i = 0; i < mapSize; i++) {
                    Object key = randomColumnValue(column.copy(keyType));
                    Object value = randomColumnValue(column.copy(valueType));
                    objectMap.put(key, value);
                }
                return objectMap;
            case STRING:
                return value(column, String::toString, fakeDataRandomUtils::randomString);
            case BOOLEAN:
                return value(column, Boolean::parseBoolean, fakeDataRandomUtils::randomBoolean);
            case TINYINT:
                return value(column, Byte::parseByte, fakeDataRandomUtils::randomTinyint);
            case SMALLINT:
                return value(column, Short::parseShort, fakeDataRandomUtils::randomSmallint);
            case INT:
                return value(column, Integer::parseInt, fakeDataRandomUtils::randomInt);
            case BIGINT:
                return value(column, Long::parseLong, fakeDataRandomUtils::randomBigint);
            case FLOAT:
                return value(column, Float::parseFloat, fakeDataRandomUtils::randomFloat);
            case DOUBLE:
                return value(column, Double::parseDouble, fakeDataRandomUtils::randomDouble);
            case DECIMAL:
                return value(column, BigDecimal::new, fakeDataRandomUtils::randomBigDecimal);
            case NULL:
                return null;
            case BYTES:
                return value(column, String::getBytes, fakeDataRandomUtils::randomBytes);
            case DATE:
                return value(
                        column,
                        defaultValue -> {
                            if (defaultValue.equalsIgnoreCase(CURRENT_DATE)) {
                                return LocalDate.now();
                            }
                            DateTimeFormatter dateTimeFormatter =
                                    DateUtils.matchDateFormatter(defaultValue);
                            return LocalDate.parse(
                                    defaultValue,
                                    dateTimeFormatter == null
                                            ? DateTimeFormatter.ISO_LOCAL_DATE
                                            : dateTimeFormatter);
                        },
                        fakeDataRandomUtils::randomLocalDate);
            case TIME:
                return value(
                        column,
                        defaultValue -> {
                            if (defaultValue.equalsIgnoreCase(CURRENT_TIME)) {
                                return LocalTime.now();
                            }
                            return LocalTime.parse(defaultValue, DateTimeFormatter.ISO_LOCAL_TIME);
                        },
                        fakeDataRandomUtils::randomLocalTime);
            case TIMESTAMP:
                return value(
                        column,
                        defaultValue -> {
                            if (defaultValue.equalsIgnoreCase(CURRENT_TIMESTAMP)) {
                                return LocalDateTime.now();
                            }
                            DateTimeFormatter dateTimeFormatter =
                                    DateTimeUtils.matchDateTimeFormatter(defaultValue);
                            return LocalDateTime.parse(
                                    defaultValue,
                                    dateTimeFormatter == null
                                            ? DateTimeFormatter.ISO_LOCAL_DATE_TIME
                                            : dateTimeFormatter);
                        },
                        fakeDataRandomUtils::randomLocalDateTime);
            case TIMESTAMP_TZ:
                return value(
                        column,
                        defaultValue -> {
                            if (defaultValue.equalsIgnoreCase(CURRENT_TIMESTAMP)) {
                                return OffsetDateTime.now();
                            }
                            DateTimeFormatter dateTimeFormatter =
                                    DateTimeUtils.matchDateTimeFormatter(defaultValue);
                            return OffsetDateTime.parse(
                                    defaultValue,
                                    dateTimeFormatter == null
                                            ? DateTimeFormatter.ISO_OFFSET_DATE_TIME
                                            : dateTimeFormatter);
                        },
                        c ->
                                fakeDataRandomUtils
                                        .randomLocalDateTime(c)
                                        .atZone(ZoneId.systemDefault())
                                        .toOffsetDateTime());
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) fieldType).getFieldTypes();
                Object[] objects = new Object[fieldTypes.length];
                for (int i = 0; i < fieldTypes.length; i++) {
                    Object object = randomColumnValue(column.copy(fieldTypes[i]));
                    objects[i] = object;
                }
                return new SeaTunnelRow(objects);
            case BINARY_VECTOR:
                return fakeDataRandomUtils.randomBinaryVector(column);
            case FLOAT_VECTOR:
                return fakeDataRandomUtils.randomFloatVector(column);
            case FLOAT16_VECTOR:
                return fakeDataRandomUtils.randomFloat16Vector(column);
            case BFLOAT16_VECTOR:
                return fakeDataRandomUtils.randomBFloat16Vector(column);
            case SPARSE_FLOAT_VECTOR:
                return fakeDataRandomUtils.randomSparseFloatVector(column);
            default:
                // never got in there
                throw new FakeConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel Fake source connector not support this data type");
        }
    }

    private static <T> T value(
            Column column, Function<String, T> convert, Function<Column, T> generate) {
        if (column.getDefaultValue() != null) {
            return convert.apply(column.getDefaultValue().toString());
        }
        return generate.apply(column);
    }
}
