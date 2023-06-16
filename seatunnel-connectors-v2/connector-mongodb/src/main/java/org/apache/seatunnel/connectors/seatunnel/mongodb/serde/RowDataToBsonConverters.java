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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.api.table.type.SqlType.NULL;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.ENCODE_VALUE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.serde.BsonToRowDataConverters.fromBigDecimal;

public class RowDataToBsonConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    public interface RowDataToBsonConverter extends Serializable {
        BsonDocument convert(SeaTunnelRow rowData);
    }

    public static RowDataToBsonConverter createConverter(SeaTunnelDataType<?> type) {
        SerializableFunction<Object, BsonValue> internalRowConverter =
                createNullSafeInternalConverter(type);
        return new RowDataToBsonConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonDocument convert(SeaTunnelRow rowData) {
                return (BsonDocument) internalRowConverter.apply(rowData);
            }
        };
    }

    private static SerializableFunction<Object, BsonValue> createNullSafeInternalConverter(
            SeaTunnelDataType<?> type) {
        return wrapIntoNullSafeInternalConverter(createInternalConverter(type), type);
    }

    private static SerializableFunction<Object, BsonValue> wrapIntoNullSafeInternalConverter(
            SerializableFunction<Object, BsonValue> internalConverter, SeaTunnelDataType<?> type) {
        return new SerializableFunction<Object, BsonValue>() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue apply(Object value) {
                if (value == null || NULL.equals(type.getSqlType())) {
                    throw new MongodbConnectorException(
                            UNSUPPORTED_DATA_TYPE,
                            "The column type is <"
                                    + type
                                    + ">, but a null value is being written into it");
                } else {
                    return internalConverter.apply(value);
                }
            }
        };
    }

    private static SerializableFunction<Object, BsonValue> createInternalConverter(
            SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case NULL:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        return BsonNull.VALUE;
                    }
                };
            case BOOLEAN:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        return new BsonBoolean((boolean) value);
                    }
                };
            case TINYINT:
            case SMALLINT:
            case INT:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        int intValue =
                                value instanceof Byte
                                        ? ((Byte) value) & 0xFF
                                        : value instanceof Short
                                                ? ((Short) value).intValue()
                                                : (int) value;
                        return new BsonInt32(intValue);
                    }
                };
            case BIGINT:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        return new BsonInt64((long) value);
                    }
                };
            case FLOAT:
            case DOUBLE:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        double v =
                                value instanceof Float
                                        ? ((Float) value).doubleValue()
                                        : (double) value;
                        return new BsonDouble(v);
                    }
                };
            case STRING:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        String val = value.toString();
                        // try to parse out the mongodb specific data type from extend-json.
                        if (val.startsWith("{")
                                && val.endsWith("}")
                                && val.contains(ENCODE_VALUE_FIELD)) {
                            try {
                                BsonDocument doc = BsonDocument.parse(val);
                                if (doc.containsKey(ENCODE_VALUE_FIELD)) {
                                    return doc.get(ENCODE_VALUE_FIELD);
                                }
                            } catch (JsonParseException e) {
                                // invalid json format, fallback to store as a bson string.
                                return new BsonString(value.toString());
                            }
                        }
                        return new BsonString(value.toString());
                    }
                };
            case BYTES:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        return new BsonBinary((byte[]) value);
                    }
                };
            case DATE:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        LocalDate localDate = (LocalDate) value;
                        return new BsonDateTime(
                                localDate
                                        .atStartOfDay(ZoneId.systemDefault())
                                        .toInstant()
                                        .toEpochMilli());
                    }
                };
            case TIMESTAMP:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        LocalDateTime localDateTime = (LocalDateTime) value;
                        return new BsonDateTime(
                                localDateTime
                                        .atZone(ZoneId.systemDefault())
                                        .toInstant()
                                        .toEpochMilli());
                    }
                };
            case DECIMAL:
                return new SerializableFunction<Object, BsonValue>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public BsonValue apply(Object value) {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal decimalVal = (BigDecimal) value;
                        return new BsonDecimal128(
                                new Decimal128(
                                        Objects.requireNonNull(
                                                fromBigDecimal(
                                                        decimalVal,
                                                        decimalType.getPrecision(),
                                                        decimalType.getScale()))));
                    }
                };
            case ARRAY:
                return createArrayConverter((ArrayType<?, ?>) type);
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) type;
                return createMapConverter(
                        mapType.toString(), mapType.getKeyType(), mapType.getValueType());
            case ROW:
                return createRowConverter((SeaTunnelRowType) type);
            default:
                throw new MongodbConnectorException(
                        UNSUPPORTED_DATA_TYPE, "Not support to parse type: " + type);
        }
    }

    private static SerializableFunction<Object, BsonValue> createArrayConverter(
            ArrayType<?, ?> arrayType) {
        final SerializableFunction<Object, BsonValue> elementConverter =
                createNullSafeInternalConverter(arrayType.getElementType());

        return new SerializableFunction<Object, BsonValue>() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue apply(Object value) {
                Object[] arrayData = (Object[]) value;
                final List<BsonValue> bsonValues = new ArrayList<>();
                for (Object element : arrayData) {
                    bsonValues.add(elementConverter.apply(element));
                }
                return new BsonArray(bsonValues);
            }
        };
    }

    private static SerializableFunction<Object, BsonValue> createMapConverter(
            String typeSummary, SeaTunnelDataType<?> keyType, SeaTunnelDataType<?> valueType) {
        if (!SqlType.STRING.equals(keyType.getSqlType())) {
            throw new MongodbConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "JSON format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        }

        final SerializableFunction<Object, BsonValue> valueConverter =
                createNullSafeInternalConverter(valueType);

        return new SerializableFunction<Object, BsonValue>() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue apply(Object value) {
                Map<String, ?> mapData = (Map<String, ?>) value;
                final BsonDocument document = new BsonDocument();
                for (Map.Entry<String, ?> entry : mapData.entrySet()) {
                    String fieldName = entry.getKey();
                    document.append(fieldName, valueConverter.apply(entry.getValue()));
                }
                return document;
            }
        };
    }

    private static SerializableFunction<Object, BsonValue> createRowConverter(
            SeaTunnelRowType rowType) {
        final SerializableFunction<Object, BsonValue>[] fieldConverters =
                rowType.getChildren().stream()
                        .map(RowDataToBsonConverters::createNullSafeInternalConverter)
                        .toArray(SerializableFunction[]::new);

        final int fieldCount = rowType.getTotalFields();
        final String[] fieldNames = rowType.getFieldNames();

        return new SerializableFunction<Object, BsonValue>() {
            private static final long serialVersionUID = 1L;

            @Override
            public BsonValue apply(Object value) {
                final SeaTunnelRow rowData = (SeaTunnelRow) value;
                final BsonDocument document = new BsonDocument();
                for (int i = 0; i < fieldCount; i++) {
                    document.append(fieldNames[i], fieldConverters[i].apply(rowData.getField(i)));
                }
                return document;
            }
        };
    }
}
