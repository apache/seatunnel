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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.convert.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from {@link SeaTunnelRow} to Avro {@link GenericRecord}. */
public class RowDataToAvroConverters implements Serializable {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Seatunnel internal data structures to
     * corresponding Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {
        Object convert(Schema schema, Object object);
    }

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Seatunnel internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(SeaTunnelDataType<?> dataType) {
        final RowDataToAvroConverter converter;
        switch (dataType.getSqlType()) {
            case TINYINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Byte) object).intValue();
                            }
                        };
                break;
            case SMALLINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Short) object).intValue();
                            }
                        };
                break;
            case BOOLEAN: // boolean
            case INT: // int
            case BIGINT: // long
            case FLOAT: // float
            case DOUBLE: // double
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return object;
                            }
                        };
                break;
            case TIME: // int
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
                            }
                        };
                break;
            case DATE: // int
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((int) ((LocalDate) object).toEpochDay());
                            }
                        };
                break;
            case STRING:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return new Utf8(object.toString());
                            }
                        };
                break;
            case BYTES:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap((byte[]) object);
                            }
                        };
                break;
            case TIMESTAMP:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((LocalDateTime) object)
                                        .toInstant(java.time.ZoneOffset.UTC)
                                        .toEpochMilli();
                            }
                        };
                break;
            case DECIMAL:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap(
                                        ((BigDecimal) object).unscaledValue().toByteArray());
                            }
                        };
                break;
            case ARRAY:
                converter = createArrayConverter((ArrayType<?, ?>) dataType);
                break;
            case ROW:
                converter = createRowConverter((SeaTunnelRowType) dataType);
                break;
            case MAP:
                converter = createMapConverter(dataType);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }

        // wrap into nullable converter
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                if (object == null) {
                    return null;
                }

                // get actual schema if it is a nullable schema
                Schema actualSchema;
                if (schema.getType() == Schema.Type.UNION) {
                    List<Schema> types = schema.getTypes();
                    int size = types.size();
                    if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(0);
                    } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(1);
                    } else {
                        throw new IllegalArgumentException(
                                "The Avro schema is not a nullable type: " + schema);
                    }
                } else {
                    actualSchema = schema;
                }
                return converter.convert(actualSchema, object);
            }
        };
    }

    private static RowDataToAvroConverter createRowConverter(SeaTunnelRowType rowType) {
        final RowDataToAvroConverter[] fieldConverters =
                Arrays.stream(rowType.getFieldTypes())
                        .map(RowDataToAvroConverters::createConverter)
                        .toArray(RowDataToAvroConverter[]::new);
        final SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final SeaTunnelRow row = (SeaTunnelRow) object;
                final List<Schema.Field> fields = schema.getFields();
                final GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < fieldTypes.length; ++i) {
                    final Schema.Field schemaField = fields.get(i);
                    try {
                        Object avroObject =
                                fieldConverters[i].convert(schemaField.schema(), row.getField(i));
                        record.put(i, avroObject);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format(
                                        "Fail to serialize at field: %s.", schemaField.name()),
                                t);
                    }
                }
                return record;
            }
        };
    }

    private static RowDataToAvroConverter createArrayConverter(ArrayType<?, ?> arrayType) {
        final RowDataToAvroConverter elementConverter = createConverter(arrayType.getElementType());

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema elementSchema = schema.getElementType();
                Object[] arrayData = (Object[]) object;
                List<Object> list = new ArrayList<>();
                for (Object arrayDatum : arrayData) {
                    list.add(elementConverter.convert(elementSchema, arrayDatum));
                }
                return list;
            }
        };
    }

    private static RowDataToAvroConverter createMapConverter(SeaTunnelDataType<?> type) {
        SeaTunnelDataType<?> valueType = extractValueTypeToAvroMap(type);

        final RowDataToAvroConverter valueConverter = createConverter(valueType);

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema valueSchema = schema.getValueType();
                final Map<String, Object> mapData = (Map) object;

                final Map<Object, Object> map = new HashMap<>(mapData.size());

                mapData.forEach(
                        (s, o) -> {
                            map.put(s, valueConverter.convert(valueSchema, o));
                        });

                return map;
            }
        };
    }
}
