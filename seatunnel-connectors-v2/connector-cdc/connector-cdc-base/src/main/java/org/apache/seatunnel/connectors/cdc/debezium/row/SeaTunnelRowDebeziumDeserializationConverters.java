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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;
import org.apache.seatunnel.connectors.cdc.debezium.utils.TemporalConversions;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;

/**
 * Deserialization schema from Debezium object to {@link SeaTunnelRow}
 */
public class SeaTunnelRowDebeziumDeserializationConverters implements Serializable {
    private static final long serialVersionUID = -897499476343410567L;
    protected final DebeziumDeserializationConverter[] physicalConverters;
    protected final MetadataConverter[] metadataConverters;
    protected final String[] fieldNames;

    public SeaTunnelRowDebeziumDeserializationConverters(
        SeaTunnelRowType physicalDataType,
        MetadataConverter[] metadataConverters,
        ZoneId serverTimeZone,
        DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        this.metadataConverters = metadataConverters;

        this.physicalConverters =
            Arrays.stream(physicalDataType.getFieldTypes())
                .map(type -> createConverter(type, serverTimeZone, userDefinedConverterFactory))
                .toArray(DebeziumDeserializationConverter[]::new);
        this.fieldNames = physicalDataType.getFieldNames();
    }

    public SeaTunnelRow convert(SourceRecord record, Struct struct, Schema schema) throws Exception {
        int arity = physicalConverters.length + metadataConverters.length;
        SeaTunnelRow row = new SeaTunnelRow(arity);
        // physical column
        for (int i = 0; i < physicalConverters.length; i++) {
            String fieldName = fieldNames[i];
            Object fieldValue = struct.get(fieldName);
            Field field = schema.field(fieldName);
            if (field == null) {
                row.setField(i, null);
            } else {
                Schema fieldSchema = field.schema();
                Object convertedField = SeaTunnelRowDebeziumDeserializationConverters.convertField(physicalConverters[i], fieldValue, fieldSchema);
                row.setField(i, convertedField);
            }
        }
        // metadata column
        for (int i = 0; i < metadataConverters.length; i++) {
            row.setField(i + physicalConverters.length, metadataConverters[i].read(record));
        }
        return row;
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Creates a runtime converter which is null safe.
     */
    private static DebeziumDeserializationConverter createConverter(SeaTunnelDataType<?> type,
                                                             ZoneId serverTimeZone,
                                                             DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        return wrapIntoNullableConverter(createNotNullConverter(type, serverTimeZone, userDefinedConverterFactory));
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260).
    // --------------------------------------------------------------------------------

    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    private static DebeziumDeserializationConverter createNotNullConverter(SeaTunnelDataType<?> type,
                                                                    ZoneId serverTimeZone,
                                                                    DebeziumDeserializationConverterFactory userDefinedConverterFactory) {

        // user defined converter has a higher resolve order
        Optional<DebeziumDeserializationConverter> converter =
            userDefinedConverterFactory.createUserDefinedConverter(type, serverTimeZone);
        if (converter.isPresent()) {
            return converter.get();
        }

        // if no matched user defined converter, fallback to the default converter
        switch (type.getSqlType()) {
            case NULL:
                return new DebeziumDeserializationConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(Object dbzObj, Schema schema) throws Exception {
                        return null;
                    }
                };
            case BOOLEAN:
                return wrapNumericConverter(convertToBoolean());
            case TINYINT:
                return wrapNumericConverter(convertToByte());
            case SMALLINT:
                return wrapNumericConverter(convertToShort());
            case INT:
                return wrapNumericConverter(convertToInt());
            case BIGINT:
                return wrapNumericConverter(convertToLong());
            case DATE:
                return convertToDate();
            case TIME:
                return convertToTime();
            case TIMESTAMP:
                return convertToTimestamp(serverTimeZone);
            case FLOAT:
                return wrapNumericConverter(convertToFloat());
            case DOUBLE:
                return wrapNumericConverter(convertToDouble());
            case STRING:
                return convertToString();
            case BYTES:
                return convertToBinary();
            case DECIMAL:
                return wrapNumericConverter(createDecimalConverter());
            case ROW:
                return createRowConverter((SeaTunnelRowType) type, serverTimeZone, userDefinedConverterFactory);
            case ARRAY:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DebeziumDeserializationConverter convertToBoolean() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Boolean) {
                    return dbzObj;
                } else if (dbzObj instanceof Byte) {
                    return (byte) dbzObj != 0;
                } else if (dbzObj instanceof Short) {
                    return (short) dbzObj != 0;
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).shortValue() != 0;
                } else {
                    return Boolean.parseBoolean(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToByte() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Byte) {
                    return dbzObj;
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).byteValue();
                } else {
                    return Byte.parseByte(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToShort() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Byte) {
                    return dbzObj;
                } else if (dbzObj instanceof Short) {
                    return dbzObj;
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).shortValue();
                } else {
                    return Short.parseShort(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToInt() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return dbzObj;
                } else if (dbzObj instanceof Long) {
                    return ((Long) dbzObj).intValue();
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).intValue();
                } else {
                    return Integer.parseInt(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToLong() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Integer) {
                    return dbzObj;
                } else if (dbzObj instanceof Long) {
                    return dbzObj;
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).longValue();
                } else {
                    return Long.parseLong(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToDouble() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return dbzObj;
                } else if (dbzObj instanceof Double) {
                    return dbzObj;
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).doubleValue();
                } else {
                    return Double.parseDouble(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToFloat() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Float) {
                    return dbzObj;
                } else if (dbzObj instanceof Double) {
                    return ((Double) dbzObj).floatValue();
                } else if (dbzObj instanceof BigDecimal) {
                    return ((BigDecimal) dbzObj).floatValue();
                } else {
                    return Float.parseFloat(dbzObj.toString());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter convertToDate() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return TemporalConversions.toLocalDate(dbzObj);
            }
        };
    }

    private static DebeziumDeserializationConverter convertToTime() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("MagicNumber")
            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case MicroTime.SCHEMA_NAME:
                            return LocalTime.ofNanoOfDay((long) dbzObj * 1000L);
                        case NanoTime.SCHEMA_NAME:
                            return LocalTime.ofNanoOfDay((long) dbzObj);
                        default:
                    }
                } else if (dbzObj instanceof Integer) {
                    return LocalTime.ofNanoOfDay((long) dbzObj * 1000_000L);
                }
                // get number of milliseconds of the day
                return TemporalConversions.toLocalTime(dbzObj);
            }
        };
    }

    private static DebeziumDeserializationConverter convertToTimestamp(ZoneId serverTimeZone) {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("MagicNumber")
            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof Long) {
                    switch (schema.name()) {
                        case Timestamp.SCHEMA_NAME:
                            return toLocalDateTime((Long) dbzObj, 0);
                        case MicroTimestamp.SCHEMA_NAME:
                            long micro = (long) dbzObj;
                            return toLocalDateTime(micro / 1000, (int) (micro % 1000 * 1000));
                        case NanoTimestamp.SCHEMA_NAME:
                            long nano = (long) dbzObj;
                            return toLocalDateTime(nano / 1000_000, (int) (nano % 1000_000));
                        default:
                    }
                }
                return TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
            }
        };
    }

    @SuppressWarnings("MagicNumber")
    public static LocalDateTime toLocalDateTime(long millisecond, int nanoOfMillisecond) {
        // 86400000 = 24 * 60 * 60 * 1000
        int date = (int) (millisecond / 86400000);
        int time = (int) (millisecond % 86400000);
        if (time < 0) {
            --date;
            time += 86400000;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return LocalDateTime.of(localDate, localTime);
    }

    private static DebeziumDeserializationConverter convertToLocalTimeZoneTimestamp(ZoneId serverTimeZone) {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                if (dbzObj instanceof String) {
                    String str = (String) dbzObj;
                    // TIMESTAMP type is encoded in string type
                    Instant instant = Instant.parse(str);
                    return LocalDateTime.ofInstant(instant, serverTimeZone);
                }
                throw new IllegalArgumentException(
                    "Unable to convert to LocalDateTime from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
            }
        };
    }

    private static DebeziumDeserializationConverter convertToString() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) {
                return dbzObj.toString();
            }
        };
    }

    private static DebeziumDeserializationConverter convertToBinary() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                if (dbzObj instanceof byte[]) {
                    return dbzObj;
                } else if (dbzObj instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                        "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
                }
            }
        };
    }

    private static DebeziumDeserializationConverter createDecimalConverter() {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                BigDecimal bigDecimal;
                if (dbzObj instanceof byte[]) {
                    // decimal.handling.mode=precise
                    bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
                } else if (dbzObj instanceof String) {
                    // decimal.handling.mode=string
                    bigDecimal = new BigDecimal((String) dbzObj);
                } else if (dbzObj instanceof Double) {
                    // decimal.handling.mode=double
                    bigDecimal = BigDecimal.valueOf((Double) dbzObj);
                } else if (dbzObj instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) dbzObj;
                } else {
                    // fallback to string
                    bigDecimal = new BigDecimal(dbzObj.toString());
                }

                return bigDecimal;
            }
        };
    }

    private static DebeziumDeserializationConverter createRowConverter(SeaTunnelRowType rowType,
                                                                ZoneId serverTimeZone,
                                                                DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        final DebeziumDeserializationConverter[] fieldConverters =
            Arrays.stream(rowType.getFieldTypes())
                .map(type -> createConverter(type, serverTimeZone, userDefinedConverterFactory))
                .toArray(DebeziumDeserializationConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames();

        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                Struct struct = (Struct) dbzObj;
                int arity = fieldNames.length;
                SeaTunnelRow row = new SeaTunnelRow(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    Object fieldValue = struct.get(fieldName);
                    Field field = schema.field(fieldName);
                    if (field == null) {
                        row.setField(i, null);
                    } else {
                        Schema fieldSchema = field.schema();
                        Object convertedField = SeaTunnelRowDebeziumDeserializationConverters.convertField(fieldConverters[i], fieldValue, fieldSchema);
                        row.setField(i, convertedField);
                    }
                }
                return row;
            }
        };
    }

    private static Object convertField(
        DebeziumDeserializationConverter fieldConverter, Object fieldValue, Schema fieldSchema)
        throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue, fieldSchema);
        }
    }

    private static DebeziumDeserializationConverter wrapIntoNullableConverter(DebeziumDeserializationConverter converter) {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                if (dbzObj == null) {
                    return null;
                }
                return converter.convert(dbzObj, schema);
            }
        };
    }

    private static DebeziumDeserializationConverter wrapNumericConverter(DebeziumDeserializationConverter converter) {
        return new DebeziumDeserializationConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object dbzObj, Schema schema) throws Exception {
                if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                    SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
                    return converter.convert(decimal.getDecimalValue().orElse(BigDecimal.ZERO), schema);
                }
                return converter.convert(dbzObj, schema);
            }
        };
    }
}
