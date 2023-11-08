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
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DEFAULT_JSON_WRITER_SETTINGS;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.ENCODE_VALUE_FIELD;

public class BsonToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    public interface BsonToRowDataConverter extends Serializable {
        Object convert(BsonValue bsonValue);
    }

    public BsonToRowDataConverter createConverter(SeaTunnelDataType<?> type) {
        SerializableFunction<BsonValue, Object> internalRowConverter =
                createNullSafeInternalConverter(type);
        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue bsonValue) {
                return internalRowConverter.apply(bsonValue);
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createNullSafeInternalConverter(
            SeaTunnelDataType<?> type) {
        return wrapIntoNullSafeInternalConverter(createInternalConverter(type), type);
    }

    private static SerializableFunction<BsonValue, Object> wrapIntoNullSafeInternalConverter(
            SerializableFunction<BsonValue, Object> internalConverter, SeaTunnelDataType<?> type) {
        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (isBsonValueNull(bsonValue) || isBsonDecimalNaN(bsonValue)) {
                    return null;
                }
                return internalConverter.apply(bsonValue);
            }
        };
    }

    private static boolean isBsonValueNull(BsonValue bsonValue) {
        return bsonValue == null
                || bsonValue.isNull()
                || bsonValue.getBsonType() == BsonType.UNDEFINED;
    }

    private static boolean isBsonDecimalNaN(BsonValue bsonValue) {
        return bsonValue.isDecimal128() && bsonValue.asDecimal128().getValue().isNaN();
    }

    private static SerializableFunction<BsonValue, Object> createInternalConverter(
            SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case NULL:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToBoolean(bsonValue);
                    }
                };
            case DOUBLE:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToDouble(bsonValue);
                    }
                };
            case INT:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToInt(bsonValue);
                    }
                };
            case BIGINT:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToLong(bsonValue);
                    }
                };
            case BYTES:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToBinary(bsonValue);
                    }
                };
            case STRING:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToString(bsonValue);
                    }
                };
            case DATE:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToLocalDateTime(bsonValue).toLocalDate();
                    }
                };
            case TIMESTAMP:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        return convertToLocalDateTime(bsonValue);
                    }
                };
            case DECIMAL:
                return new SerializableFunction<BsonValue, Object>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object apply(BsonValue bsonValue) {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal decimalValue = convertToBigDecimal(bsonValue);
                        return fromBigDecimal(
                                decimalValue, decimalType.getPrecision(), decimalType.getScale());
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

    private static LocalDateTime convertToLocalDateTime(BsonValue bsonValue) {
        Instant instant;
        if (bsonValue.isTimestamp()) {
            instant = Instant.ofEpochSecond(bsonValue.asTimestamp().getTime());
        } else if (bsonValue.isDateTime()) {
            instant = Instant.ofEpochMilli(bsonValue.asDateTime().getValue());
        } else {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    "Unable to convert to LocalDateTime from unexpected value '"
                            + bsonValue
                            + "' of type "
                            + bsonValue.getBsonType());
        }
        return Timestamp.from(instant).toLocalDateTime();
    }

    private static SerializableFunction<BsonValue, Object> createRowConverter(
            SeaTunnelRowType type) {
        SeaTunnelDataType<?>[] fieldTypes = type.getFieldTypes();
        final SerializableFunction<BsonValue, Object>[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(BsonToRowDataConverters::createNullSafeInternalConverter)
                        .toArray(SerializableFunction[]::new);
        int fieldCount = type.getTotalFields();

        final String[] fieldNames = type.getFieldNames();

        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT,
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                SeaTunnelRow row = new SeaTunnelRow(fieldCount);
                for (int i = 0; i < fieldCount; i++) {
                    String fieldName = fieldNames[i];
                    BsonValue fieldValue = document.get(fieldName);
                    Object convertedField = fieldConverters[i].apply(fieldValue);
                    row.setField(i, convertedField);
                }
                return row;
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createArrayConverter(
            ArrayType<?, ?> type) {
        final SerializableFunction<BsonValue, Object> elementConverter =
                createNullSafeInternalConverter(type.getElementType());
        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isArray()) {
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT,
                            "Unable to convert to arrayType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                List<BsonValue> in = bsonValue.asArray();
                Object arr = Array.newInstance(type.getElementType().getTypeClass(), in.size());
                for (int i = 0; i < in.size(); i++) {
                    Array.set(arr, i, elementConverter.apply(in.get(i)));
                }
                return arr;
            }
        };
    }

    private static SerializableFunction<BsonValue, Object> createMapConverter(
            String typeSummary, SeaTunnelDataType<?> keyType, SeaTunnelDataType<?> valueType) {
        if (!keyType.getSqlType().equals(SqlType.STRING)) {
            throw new MongodbConnectorException(
                    UNSUPPORTED_OPERATION,
                    "Bson format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        }
        SerializableFunction<BsonValue, Object> valueConverter =
                createNullSafeInternalConverter(valueType);

        return new SerializableFunction<BsonValue, Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT,
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                Map<String, Object> map = new HashMap<>();
                for (String key : document.keySet()) {
                    map.put(key, valueConverter.apply(document.get(key)));
                }
                return map;
            }
        };
    }

    public static BigDecimal fromBigDecimal(BigDecimal bd, int precision, int scale) {
        bd = bd.setScale(scale, RoundingMode.HALF_UP);
        if (bd.precision() > precision) {
            return null;
        }
        return bd;
    }

    private static boolean convertToBoolean(BsonValue bsonValue) {
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unable to convert to boolean from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static double convertToDouble(BsonValue bsonValue) {
        if (bsonValue.isDouble()) {
            return bsonValue.asNumber().doubleValue();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unable to convert to double from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static int convertToInt(BsonValue bsonValue) {
        if (bsonValue.isInt32()) {
            return bsonValue.asNumber().intValue();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unable to convert to integer from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static String convertToString(BsonValue bsonValue) {
        if (bsonValue.isString()) {
            return bsonValue.asString().getValue();
        }
        if (bsonValue.isObjectId()) {
            return bsonValue.asObjectId().getValue().toHexString();
        }
        if (bsonValue.isDocument()) {
            return bsonValue
                    .asDocument()
                    .toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
        }
        return new BsonDocument(ENCODE_VALUE_FIELD, bsonValue).toJson(DEFAULT_JSON_WRITER_SETTINGS);
    }

    private static byte[] convertToBinary(BsonValue bsonValue) {
        if (bsonValue.isBinary()) {
            return bsonValue.asBinary().getData();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unsupported BYTES value type: " + bsonValue.getClass().getSimpleName());
    }

    private static long convertToLong(BsonValue bsonValue) {
        if (bsonValue.isInt64()) {
            return bsonValue.asNumber().longValue();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unable to convert to long from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static BigDecimal convertToBigDecimal(BsonValue bsonValue) {
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return bsonValue.asDecimal128().decimal128Value().bigDecimalValue();
            } else {
                // DecimalData doesn't have the concept of infinity.
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unable to convert infinite bson decimal to Decimal type.");
            }
        }
        throw new MongodbConnectorException(
                ILLEGAL_ARGUMENT,
                "Unable to convert to decimal from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }
}
