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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;

import com.mongodb.client.model.changestream.OperationType;

import javax.annotation.Nonnull;

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
import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DEFAULT_JSON_WRITER_SETTINGS;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ENCODE_VALUE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FULL_DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.extractBsonDocument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class MongoDBConnectorDeserializationSchema
        implements DebeziumDeserializationSchema<SeaTunnelRow> {

    private final SeaTunnelDataType<SeaTunnelRow> resultTypeInfo;

    private final DeserializationRuntimeConverter physicalConverter;

    public MongoDBConnectorDeserializationSchema(
            SeaTunnelDataType<SeaTunnelRow> physicalDataType,
            SeaTunnelDataType<SeaTunnelRow> resultTypeInfo) {
        this.physicalConverter = createConverter(physicalDataType);
        this.resultTypeInfo = resultTypeInfo;
    }

    @Override
    public void deserialize(@Nonnull SourceRecord record, Collector<SeaTunnelRow> out) {
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();

        OperationType op = operationTypeFor(record);
        BsonDocument documentKey =
                checkNotNull(
                        Objects.requireNonNull(
                                extractBsonDocument(value, valueSchema, DOCUMENT_KEY)));
        BsonDocument fullDocument = extractBsonDocument(value, valueSchema, FULL_DOCUMENT);

        switch (op) {
            case INSERT:
                SeaTunnelRow insert = extractRowData(fullDocument);
                insert.setRowKind(RowKind.INSERT);
                emit(record, insert, out);
                break;
            case DELETE:
                SeaTunnelRow delete = extractRowData(documentKey);
                delete.setRowKind(RowKind.DELETE);
                emit(record, delete, out);
                break;
            case UPDATE:
                if (fullDocument == null) {
                    break;
                }
                SeaTunnelRow updateAfter = extractRowData(fullDocument);
                updateAfter.setRowKind(RowKind.UPDATE_AFTER);
                emit(record, updateAfter, out);
                break;
            case REPLACE:
                SeaTunnelRow replaceAfter = extractRowData(fullDocument);
                replaceAfter.setRowKind(RowKind.UPDATE_AFTER);
                emit(record, replaceAfter, out);
                break;
            case INVALIDATE:
            case DROP:
            case DROP_DATABASE:
            case RENAME:
            case OTHER:
            default:
                break;
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return resultTypeInfo;
    }

    private @Nonnull OperationType operationTypeFor(@Nonnull SourceRecord record) {
        Struct value = (Struct) record.value();
        return OperationType.fromString(value.getString("operationType"));
    }

    // TODO:The dynamic schema will be completed based on this method later.
    private void emit(
            SourceRecord inRecord,
            SeaTunnelRow physicalRow,
            @Nonnull Collector<SeaTunnelRow> collector) {
        collector.collect(physicalRow);
    }

    private SeaTunnelRow extractRowData(BsonDocument document) {
        checkNotNull(document);
        return (SeaTunnelRow) physicalConverter.convert(document);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    @FunctionalInterface
    public interface DeserializationRuntimeConverter extends Serializable {
        Object convert(BsonValue bsonValue);
    }

    public DeserializationRuntimeConverter createConverter(SeaTunnelDataType<?> type) {
        SerializableFunction<BsonValue, Object> internalRowConverter =
                createNullSafeInternalConverter(type);
        return new DeserializationRuntimeConverter() {
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

    private static boolean isBsonDecimalNaN(@Nonnull BsonValue bsonValue) {
        return bsonValue.isDecimal128() && bsonValue.asDecimal128().getValue().isNaN();
    }

    private static SerializableFunction<BsonValue, Object> createInternalConverter(
            @Nonnull SeaTunnelDataType<?> type) {
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

    @SuppressWarnings("unchecked")
    private static SerializableFunction<BsonValue, Object> createRowConverter(
            SeaTunnelRowType type) {
        SeaTunnelDataType<?>[] fieldTypes = type.getFieldTypes();
        final SerializableFunction<BsonValue, Object>[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(MongoDBConnectorDeserializationSchema::createNullSafeInternalConverter)
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

    private static @Nonnull SerializableFunction<BsonValue, Object> createArrayConverter(
            @Nonnull ArrayType<?, ?> type) {
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

    private static @Nonnull SerializableFunction<BsonValue, Object> createMapConverter(
            String typeSummary,
            @Nonnull SeaTunnelDataType<?> keyType,
            SeaTunnelDataType<?> valueType) {
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

    private static boolean convertToBoolean(@Nonnull BsonValue bsonValue) {
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

    private static double convertToDouble(@Nonnull BsonValue bsonValue) {
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

    private static int convertToInt(@Nonnull BsonValue bsonValue) {
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

    private static String convertToString(@Nonnull BsonValue bsonValue) {
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

    private static byte[] convertToBinary(@Nonnull BsonValue bsonValue) {
        if (bsonValue.isBinary()) {
            return bsonValue.asBinary().getData();
        }
        throw new MongodbConnectorException(
                UNSUPPORTED_DATA_TYPE,
                "Unsupported BYTES value type: " + bsonValue.getClass().getSimpleName());
    }

    private static long convertToLong(@Nonnull BsonValue bsonValue) {
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

    private static BigDecimal convertToBigDecimal(@Nonnull BsonValue bsonValue) {
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
