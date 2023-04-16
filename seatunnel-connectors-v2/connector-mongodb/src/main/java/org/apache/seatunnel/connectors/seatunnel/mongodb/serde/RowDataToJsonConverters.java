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

package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

public class RowDataToJsonConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public RowDataToJsonConverters() {}

    public RowDataToJsonConverter createConverter(SeaTunnelDataType<?> type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private RowDataToJsonConverter createNotNullConverter(SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case NULL:
                return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
            case BOOLEAN:
                return (mapper, reuse, value) ->
                        mapper.getNodeFactory().booleanNode((Boolean) value);
            case TINYINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Byte) value);
            case SMALLINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Short) value);
            case INT:
                return (mapper, reuse, value) ->
                        mapper.getNodeFactory().numberNode((Integer) value);
            case BIGINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Long) value);
            case FLOAT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Float) value);
            case DOUBLE:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Double) value);
            case STRING:
                return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
            case BYTES:
                return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
            case DATE:
                return this.createDateConverter();
            case TIME:
                return this.createTimeConverter();
            case TIMESTAMP:
                return this.createTimestampConverter();
            case DECIMAL:
                return this.createDecimalConverter();
            case ARRAY:
                return this.createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return this.createMapConverter(
                        mapType.toString(), mapType.getKeyType(), mapType.getValueType());
            case ROW:
                return this.createRowConverter((SeaTunnelRowType) type);
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = (BigDecimal) value;
            return mapper.getNodeFactory().numberNode(bd);
        };
    }

    private RowDataToJsonConverter createDateConverter() {
        return (mapper, reuse, value) ->
                mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format((LocalDate) value));
    }

    private RowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) ->
                mapper.getNodeFactory().textNode(TimeFormat.TIME_FORMAT.format((LocalTime) value));
    }

    private RowDataToJsonConverter createTimestampConverter() {
        return (mapper, reuse, value) ->
                mapper.getNodeFactory().textNode(ISO_LOCAL_DATE_TIME.format((LocalDateTime) value));
    }

    private RowDataToJsonConverter createArrayConverter(ArrayType arrayType) {
        final RowDataToJsonConverter elementConverter = createConverter(arrayType.getElementType());
        return (mapper, reuse, value) -> {
            ArrayNode node;
            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createArrayNode();
            } else {
                node = (ArrayNode) reuse;
                node.removeAll();
            }
            Object[] arrayData = (Object[]) value;
            int numElements = arrayData.length;
            for (int i = 0; i < numElements; i++) {
                Object element = arrayData[i];
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createMapConverter(
            String typeSummary, SeaTunnelDataType<?> keyType, SeaTunnelDataType<?> valueType) {
        if (!SqlType.STRING.equals(keyType.getSqlType())) {
            throw new MongodbConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "JSON format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        }

        final RowDataToJsonConverter valueConverter = createConverter(valueType);
        return (mapper, reuse, value) -> {
            ObjectNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
                node.removeAll();
            }

            Map<String, ?> mapData = (Map) value;
            for (Map.Entry<String, ?> entry : mapData.entrySet()) {
                String fieldName = entry.getKey();
                node.set(
                        fieldName,
                        valueConverter.convert(mapper, node.get(fieldName), entry.getValue()));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createRowConverter(SeaTunnelRowType rowType) {
        final RowDataToJsonConverter[] fieldConverters =
                Arrays.stream(rowType.getFieldTypes())
                        .map(
                                new Function<SeaTunnelDataType<?>, Object>() {
                                    @Override
                                    public Object apply(SeaTunnelDataType<?> seaTunnelDataType) {
                                        return createConverter(seaTunnelDataType);
                                    }
                                })
                        .toArray(
                                new IntFunction<RowDataToJsonConverter[]>() {
                                    @Override
                                    public RowDataToJsonConverter[] apply(int value) {
                                        return new RowDataToJsonConverter[value];
                                    }
                                });
        final String[] fieldNames = rowType.getFieldNames();
        final int arity = fieldNames.length;

        return (mapper, reuse, value) -> {
            ObjectNode node;

            // reuse could be a NullNode if last record is null.
            if (reuse == null || reuse.isNull()) {
                node = mapper.createObjectNode();
            } else {
                node = (ObjectNode) reuse;
            }

            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];
                SeaTunnelRow row = (SeaTunnelRow) value;
                node.set(
                        fieldName,
                        fieldConverters[i].convert(mapper, node.get(fieldName), row.getField(i)));
            }

            return node;
        };
    }

    private RowDataToJsonConverter wrapIntoNullableConverter(RowDataToJsonConverter converter) {
        return (mapper, reuse, object) ->
                object == null
                        ? mapper.getNodeFactory().nullNode()
                        : converter.convert(mapper, reuse, object);
    }

    public interface RowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper var1, JsonNode var2, Object var3);
    }
}
