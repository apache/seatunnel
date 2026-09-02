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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.serialize;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts standalone MongoDB-driver BSON values into rows described by a SeaTunnel schema.
 *
 * <p>This implementation intentionally lives in the Amazon DocumentDB module: compatibility with
 * the wire protocol does not create a runtime dependency on connector-mongodb internals.
 */
public class DocumentDBItemDeserializer {

    private static final String CONNECTOR_NAME = "AmazonDocumentDB";
    private static final JsonWriterSettings RELAXED_JSON_SETTINGS =
            JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    private final SeaTunnelRowType rowType;

    public DocumentDBItemDeserializer(SeaTunnelRowType rowType) {
        this.rowType = rowType;
    }

    /**
     * Converts a complete BSON document, leaving fields absent from the document as {@code null}.
     */
    public SeaTunnelRow deserialize(BsonDocument document) {
        return convertRow("root", rowType, document);
    }

    /**
     * Dispatches conversion by the declared SeaTunnel type and retains low-level failure causes.
     */
    private Object convert(String field, SeaTunnelDataType<?> type, BsonValue value) {
        if (isNull(value)) {
            return null;
        }

        try {
            switch (type.getSqlType()) {
                case NULL:
                    return null;
                case BOOLEAN:
                    return value.asBoolean().getValue();
                case TINYINT:
                    return (byte) checkedInteger(value, Byte.MIN_VALUE, Byte.MAX_VALUE);
                case SMALLINT:
                    return (short) checkedInteger(value, Short.MIN_VALUE, Short.MAX_VALUE);
                case INT:
                    return checkedInteger(value, Integer.MIN_VALUE, Integer.MAX_VALUE);
                case BIGINT:
                    return checkedLong(value);
                case FLOAT:
                    return (float) value.asNumber().doubleValue();
                case DOUBLE:
                    return value.asNumber().doubleValue();
                case DECIMAL:
                    return convertDecimal((DecimalType) type, value);
                case STRING:
                    return convertString(value);
                case DATE:
                    return convertDateTime(value).toLocalDate();
                case TIME:
                    return convertDateTime(value).toLocalTime();
                case TIMESTAMP:
                    return convertDateTime(value);
                case BYTES:
                    return value.asBinary().getData();
                case ARRAY:
                    return convertArray(field, (ArrayType<?, ?>) type, value);
                case MAP:
                    return convertMap(field, (MapType<?, ?>) type, value);
                case ROW:
                    return convertRow(field, (SeaTunnelRowType) type, value.asDocument());
                default:
                    throw conversionError(field, type);
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (RuntimeException e) {
            SeaTunnelRuntimeException error = conversionError(field, type);
            error.initCause(e);
            throw error;
        }
    }

    private static boolean isNull(BsonValue value) {
        return value == null
                || value.isNull()
                || value.getBsonType() == BsonType.UNDEFINED
                || (value.isDecimal128() && value.asDecimal128().getValue().isNaN());
    }

    private static int checkedInteger(BsonValue value, int minimum, int maximum) {
        long number = value.asNumber().longValue();
        if (number < minimum || number > maximum) {
            throw new IllegalArgumentException("Integer value is out of range");
        }
        return (int) number;
    }

    private static long checkedLong(BsonValue value) {
        if (value.isInt32() || value.isInt64()) {
            return value.asNumber().longValue();
        }
        if (value.isDouble()) {
            double number = value.asNumber().doubleValue();
            if (number < Long.MIN_VALUE || number > Long.MAX_VALUE) {
                throw new IllegalArgumentException("Long value is out of range");
            }
            return value.asNumber().longValue();
        }
        throw new IllegalArgumentException("Value is not a supported long");
    }

    /**
     * Applies the configured scale and rejects precision overflow instead of silently emitting
     * {@code null}, which would make malformed source data indistinguishable from BSON null.
     */
    private static BigDecimal convertDecimal(DecimalType type, BsonValue value) {
        Decimal128 decimal128 = value.asDecimal128().decimal128Value();
        if (!decimal128.isFinite()) {
            throw new IllegalArgumentException("Infinite Decimal128 values are not supported");
        }
        BigDecimal decimal =
                decimal128.bigDecimalValue().setScale(type.getScale(), RoundingMode.HALF_UP);
        if (decimal.precision() > type.getPrecision()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Decimal precision %d exceeds configured precision %d",
                            decimal.precision(), type.getPrecision()));
        }
        return decimal;
    }

    private static String convertString(BsonValue value) {
        if (value.isString()) {
            return value.asString().getValue();
        }
        if (value.isObjectId()) {
            return value.asObjectId().getValue().toHexString();
        }
        if (value.isDocument()) {
            return value.asDocument().toJson(RELAXED_JSON_SETTINGS);
        }
        return value.toString();
    }

    private static LocalDateTime convertDateTime(BsonValue value) {
        Instant instant;
        if (value.isDateTime()) {
            instant = Instant.ofEpochMilli(value.asDateTime().getValue());
        } else if (value.isTimestamp()) {
            instant = Instant.ofEpochSecond(value.asTimestamp().getTime());
        } else {
            throw new IllegalArgumentException("Value is not a BSON date or timestamp");
        }
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    /** Converts array elements recursively so nested rows, maps, and arrays use the same rules. */
    private Object convertArray(String field, ArrayType<?, ?> type, BsonValue value) {
        List<BsonValue> source = value.asArray();
        Object target = Array.newInstance(type.getElementType().getTypeClass(), source.size());
        for (int i = 0; i < source.size(); i++) {
            Array.set(target, i, convert(field, type.getElementType(), source.get(i)));
        }
        return target;
    }

    /** Converts BSON documents to maps; BSON field names require a string map key type. */
    private Map<String, Object> convertMap(String field, MapType<?, ?> type, BsonValue value) {
        if (type.getKeyType().getSqlType() != SqlType.STRING) {
            throw conversionError(field, type);
        }
        Map<String, Object> target = new HashMap<>();
        BsonDocument document = value.asDocument();
        for (String key : document.keySet()) {
            target.put(key, convert(field, type.getValueType(), document.get(key)));
        }
        return target;
    }

    /** Maps document fields by schema name, preserving schema order in the resulting row. */
    private SeaTunnelRow convertRow(String field, SeaTunnelRowType type, BsonDocument document) {
        SeaTunnelRow row = new SeaTunnelRow(type.getTotalFields());
        for (int i = 0; i < type.getTotalFields(); i++) {
            String fieldName = type.getFieldName(i);
            row.setField(i, convert(fieldName, type.getFieldType(i), document.get(fieldName)));
        }
        return row;
    }

    private static SeaTunnelRuntimeException conversionError(
            String field, SeaTunnelDataType<?> type) {
        return CommonError.convertToSeaTunnelTypeError(
                CONNECTOR_NAME, type.getSqlType().toString(), field);
    }
}
