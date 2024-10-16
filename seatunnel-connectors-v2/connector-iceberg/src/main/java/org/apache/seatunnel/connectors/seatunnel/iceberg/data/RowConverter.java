/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.schema.SchemaChangeWrapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

public class RowConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DateTimeFormatter OFFSET_TS_FMT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .appendOffset("+HHmm", "Z")
                    .toFormatter();

    private final Schema tableSchema;
    private final NameMapping nameMapping;
    private final SinkConfig config;
    private final Map<Integer, Map<String, Types.NestedField>> structNames = Maps.newHashMap();

    public RowConverter(Table table, SinkConfig config) {
        this.tableSchema = table.schema();
        this.nameMapping = createNameMapping(table);
        this.config = config;
    }

    private NameMapping createNameMapping(Table table) {
        String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        return nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    }

    public Record convert(Object row, SeaTunnelDataType<?> rowType) {
        return convertStructValue(row, rowType, tableSchema.asStruct(), -1, null);
    }

    public Record convert(Object row, SeaTunnelDataType<?> rowType, SchemaChangeWrapper wrapper) {
        return convertStructValue(row, rowType, tableSchema.asStruct(), -1, wrapper);
    }

    protected GenericRecord convertStructValue(
            Object value,
            SeaTunnelDataType<?> fromType,
            Types.StructType schema,
            int parentFieldId,
            SchemaChangeWrapper wrapper) {
        switch (fromType.getSqlType()) {
            case ROW:
                return convertToStruct(
                        (SeaTunnelRow) value,
                        (SeaTunnelRowType) fromType,
                        schema,
                        parentFieldId,
                        wrapper);
            default:
                throw new IllegalArgumentException(
                        "Cannot convert to struct: " + fromType.getSqlType().name());
        }
    }

    /** Convert RowType */
    private GenericRecord convertToStruct(
            SeaTunnelRow row,
            SeaTunnelRowType fromType,
            Types.StructType schema,
            int structFieldId,
            SchemaChangeWrapper wrapper) {
        GenericRecord result = GenericRecord.create(schema);
        String[] filedNames = fromType.getFieldNames();
        for (int i = 0; i < filedNames.length; i++) {
            String recordField = filedNames[i];
            Type afterType = SchemaUtils.toIcebergType(fromType.getFieldType(i));
            Types.NestedField tableField = lookupStructField(recordField, schema, structFieldId);
            // add column
            if (Objects.isNull(tableField)) {
                if (config.isTableSchemaEvolutionEnabled() && Objects.nonNull(wrapper)) {
                    // add the column if schema evolution is on
                    String parentFieldName =
                            structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
                    wrapper.addColumn(parentFieldName, recordField, afterType);
                }
                continue;
            }
            // update column type,;
            boolean hasSchemaUpdates = false;
            if (config.isTableSchemaEvolutionEnabled() && Objects.nonNull(wrapper)) {
                // update the type if needed and schema evolution is on
                Type.PrimitiveType evolveDataType =
                        SchemaUtils.needsDataTypeUpdate(tableField.type(), afterType);
                if (Objects.nonNull(evolveDataType)) {
                    String fieldName = tableSchema.findColumnName(tableField.fieldId());
                    wrapper.modifyColumn(fieldName, evolveDataType);
                    hasSchemaUpdates = true;
                }
            }
            if (!hasSchemaUpdates) {
                result.setField(
                        tableField.name(),
                        convertValue(
                                row.getField(i),
                                fromType.getFieldType(i),
                                tableField.type(),
                                tableField.fieldId(),
                                wrapper));
            }
        }
        return result;
    }

    public Object convertValue(
            Object value,
            SeaTunnelDataType<?> fromType,
            Type type,
            int fieldId,
            SchemaChangeWrapper wrapper) {
        if (value == null) {
            return null;
        }
        switch (type.typeId()) {
            case STRUCT:
                return convertStructValue(value, fromType, type.asStructType(), fieldId, wrapper);
            case LIST:
                return convertListValue(value, fromType, type.asListType(), wrapper);
            case MAP:
                return convertMapValue(value, fromType, type.asMapType(), wrapper);
            case INTEGER:
                return convertInt(value);
            case LONG:
                return convertLong(value);
            case FLOAT:
                return convertFloat(value);
            case DOUBLE:
                return convertDouble(value);
            case DECIMAL:
                return convertDecimal(value, (Types.DecimalType) type);
            case BOOLEAN:
                return convertBoolean(value);
            case STRING:
                return convertString(value);
            case UUID:
                return convertUUID(value);
            case BINARY:
            case FIXED:
                return convertBase64Binary(value);
            case DATE:
                return convertDateValue(value);
            case TIME:
                return convertTimeValue(value);
            case TIMESTAMP:
                return convertTimestampValue(value, (Types.TimestampType) type);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
    }

    private Types.NestedField lookupStructField(
            String fieldName, Types.StructType schema, int structFieldId) {
        if (nameMapping == null) {
            return config.isCaseSensitive()
                    ? schema.caseInsensitiveField(fieldName)
                    : schema.field(fieldName);
        }

        return structNames
                .computeIfAbsent(structFieldId, notUsed -> createStructNameMap(schema))
                .get(fieldName);
    }

    private Map<String, Types.NestedField> createStructNameMap(Types.StructType schema) {
        Map<String, Types.NestedField> map = Maps.newHashMap();
        schema.fields()
                .forEach(
                        col -> {
                            MappedField mappedField = nameMapping.find(col.fieldId());
                            if (mappedField != null && !mappedField.names().isEmpty()) {
                                mappedField.names().forEach(name -> map.put(name, col));
                            } else {
                                map.put(col.name(), col);
                            }
                        });
        return map;
    }

    protected List<Object> convertListValue(
            Object value,
            SeaTunnelDataType<?> fromType,
            Types.ListType type,
            SchemaChangeWrapper wrapper) {
        Preconditions.checkArgument(value.getClass().isArray());
        Object[] list = (Object[]) value;
        return Arrays.stream(list)
                .map(
                        element -> {
                            int fieldId = type.fields().get(0).fieldId();
                            return convertValue(
                                    element, fromType, type.elementType(), fieldId, wrapper);
                        })
                .collect(toList());
    }

    protected Map<Object, Object> convertMapValue(
            Object value,
            SeaTunnelDataType<?> fromType,
            Types.MapType type,
            SchemaChangeWrapper wrapper) {
        Preconditions.checkArgument(value instanceof Map);
        Map<?, ?> map = (Map<?, ?>) value;
        Map<Object, Object> result = Maps.newHashMap();
        map.forEach(
                (k, v) -> {
                    int keyFieldId = type.fields().get(0).fieldId();
                    int valueFieldId = type.fields().get(1).fieldId();
                    result.put(
                            convertValue(k, fromType, type.keyType(), keyFieldId, wrapper),
                            convertValue(v, fromType, type.valueType(), valueFieldId, wrapper));
                });
        return result;
    }

    protected int convertInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
    }

    protected long convertLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
    }

    protected float convertFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        throw new IllegalArgumentException(
                "Cannot convert to float: " + value.getClass().getName());
    }

    protected double convertDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException(
                "Cannot convert to double: " + value.getClass().getName());
    }

    protected BigDecimal convertDecimal(Object value, Types.DecimalType type) {
        BigDecimal bigDecimal;
        if (value instanceof BigDecimal) {
            bigDecimal = (BigDecimal) value;
        } else if (value instanceof Number) {
            Number num = (Number) value;
            Double dbl = num.doubleValue();
            if (dbl.equals(Math.floor(dbl))) {
                bigDecimal = BigDecimal.valueOf(num.longValue());
            } else {
                bigDecimal = BigDecimal.valueOf(dbl);
            }
        } else if (value instanceof String) {
            bigDecimal = new BigDecimal((String) value);
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert to BigDecimal: " + value.getClass().getName());
        }
        return bigDecimal.setScale(type.scale(), RoundingMode.HALF_UP);
    }

    protected boolean convertBoolean(Object value) {
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException(
                "Cannot convert to boolean: " + value.getClass().getName());
    }

    protected String convertString(Object value) {
        try {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            } else if (value instanceof Map || value instanceof List) {
                return MAPPER.writeValueAsString(value);
            } else {
                return MAPPER.writeValueAsString(value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected UUID convertUUID(Object value) {
        if (value instanceof String) {
            return UUID.fromString((String) value);
        } else if (value instanceof UUID) {
            return (UUID) value;
        }
        throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
    }

    protected ByteBuffer convertBase64Binary(Object value) {
        if (value instanceof String) {
            return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
        } else if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        }
        throw new IllegalArgumentException(
                "Cannot convert to binary: " + value.getClass().getName());
    }

    protected LocalDate convertDateValue(Object value) {
        if (value instanceof Number) {
            int days = ((Number) value).intValue();
            return DateTimeUtil.dateFromDays(days);
        } else if (value instanceof String) {
            return LocalDate.parse((String) value);
        } else if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof Date) {
            int days = (int) (((Date) value).getTime() / 1000 / 60 / 60 / 24);
            return DateTimeUtil.dateFromDays(days);
        }
        throw new RuntimeException("Cannot convert date: " + value);
    }

    protected LocalTime convertTimeValue(Object value) {
        if (value instanceof Number) {
            long millis = ((Number) value).longValue();
            return DateTimeUtil.timeFromMicros(millis * 1000);
        } else if (value instanceof String) {
            return LocalTime.parse((String) value);
        } else if (value instanceof LocalTime) {
            return (LocalTime) value;
        } else if (value instanceof Date) {
            long millis = ((Date) value).getTime();
            return DateTimeUtil.timeFromMicros(millis * 1000);
        }
        throw new RuntimeException("Cannot convert time: " + value);
    }

    protected Temporal convertTimestampValue(Object value, Types.TimestampType type) {
        if (type.shouldAdjustToUTC()) {
            return convertOffsetDateTime(value);
        }
        return convertLocalDateTime(value);
    }

    private OffsetDateTime convertOffsetDateTime(Object value) {
        if (value instanceof Number) {
            long millis = ((Number) value).longValue();
            return DateTimeUtil.timestamptzFromMicros(millis * 1000);
        } else if (value instanceof String) {
            return parseOffsetDateTime((String) value);
        } else if (value instanceof OffsetDateTime) {
            return (OffsetDateTime) value;
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
        } else if (value instanceof Date) {
            return DateTimeUtil.timestamptzFromMicros(((Date) value).getTime() * 1000);
        }
        throw new RuntimeException(
                "Cannot convert timestamptz: " + value + ", type: " + value.getClass());
    }

    private OffsetDateTime parseOffsetDateTime(String str) {
        String tsStr = ensureTimestampFormat(str);
        try {
            return OFFSET_TS_FMT.parse(tsStr, OffsetDateTime::from);
        } catch (DateTimeParseException e) {
            return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atOffset(ZoneOffset.UTC);
        }
    }

    private LocalDateTime convertLocalDateTime(Object value) {
        if (value instanceof Number) {
            long millis = ((Number) value).longValue();
            return DateTimeUtil.timestampFromMicros(millis * 1000);
        } else if (value instanceof String) {
            return parseLocalDateTime((String) value);
        } else if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        } else if (value instanceof Date) {
            return DateTimeUtil.timestampFromMicros(((Date) value).getTime() * 1000);
        }
        throw new RuntimeException(
                "Cannot convert timestamp: " + value + ", type: " + value.getClass());
    }

    private LocalDateTime parseLocalDateTime(String str) {
        String tsStr = ensureTimestampFormat(str);
        try {
            return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException e) {
            return OFFSET_TS_FMT.parse(tsStr, OffsetDateTime::from).toLocalDateTime();
        }
    }

    private String ensureTimestampFormat(String str) {
        String result = str;
        if (result.charAt(10) == ' ') {
            result = result.substring(0, 10) + 'T' + result.substring(11);
        }
        if (result.length() > 22 && result.charAt(19) == '+' && result.charAt(22) == ':') {
            result = result.substring(0, 19) + result.substring(19).replace(":", "");
        }
        return result;
    }
}
