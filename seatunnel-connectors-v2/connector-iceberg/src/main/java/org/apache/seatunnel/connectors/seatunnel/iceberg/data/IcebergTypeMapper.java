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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergTypeMapper {

    /** Format version Iceberg uses when a table is created without {@code format-version}. */
    public static final int DEFAULT_FORMAT_VERSION = 2;

    private static final int NANOSECOND_TIMESTAMP_FORMAT_VERSION = 3;

    /** Column scale reported for {@code timestamp_ns} and {@code timestamptz_ns} columns. */
    public static final int NANOSECOND_SCALE = 9;

    public static SeaTunnelDataType<?> mapping(String field, @NonNull Type icebergType) {
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP:
                Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                if (timestampType.shouldAdjustToUTC()) {
                    // withZone() → LTZ → TIMESTAMP_TZ
                    return LocalTimeType.OFFSET_DATE_TIME_TYPE;
                }
                // withoutZone() → NTZ → TIMESTAMP
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP_NANO:
                // format version 3: java.time keeps the nanoseconds, the precision is carried as
                // column scale 9 by IcebergCatalog
                Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) icebergType;
                if (timestampNanoType.shouldAdjustToUTC()) {
                    return LocalTimeType.OFFSET_DATE_TIME_TYPE;
                }
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case UNKNOWN:
                // format version 3: an unknown column only ever holds null
                return BasicType.VOID_TYPE;
            case STRING:
                return BasicType.STRING_TYPE;
            case FIXED:
            case BINARY:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) icebergType;
                return new DecimalType(decimalType.precision(), decimalType.scale());
            case STRUCT:
                return mappingStructType((Types.StructType) icebergType);
            case LIST:
                return mappingListType(field, (Types.ListType) icebergType);
            case MAP:
                return mappingMapType(field, (Types.MapType) icebergType);
            default:
                // includes the format version 3 types variant, geometry and geography, which have
                // no SeaTunnel counterpart yet
                throw CommonError.convertToSeaTunnelTypeError(
                        "Iceberg", icebergType.toString(), field);
        }
    }

    private static SeaTunnelRowType mappingStructType(Types.StructType structType) {
        List<Types.NestedField> fields = structType.fields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>(fields.size());
        for (Types.NestedField field : fields) {
            fieldNames.add(field.name());
            fieldTypes.add(mapping(field.name(), field.type()));
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private static ArrayType mappingListType(String field, Types.ListType listType) {
        switch (listType.elementType().typeId()) {
            case BOOLEAN:
                return ArrayType.BOOLEAN_ARRAY_TYPE;
            case INTEGER:
                return ArrayType.INT_ARRAY_TYPE;
            case LONG:
                return ArrayType.LONG_ARRAY_TYPE;
            case FLOAT:
                return ArrayType.FLOAT_ARRAY_TYPE;
            case DOUBLE:
                return ArrayType.DOUBLE_ARRAY_TYPE;
            case STRING:
                return ArrayType.STRING_ARRAY_TYPE;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        "Iceberg", listType.toString(), field);
        }
    }

    private static MapType mappingMapType(String field, Types.MapType mapType) {
        return new MapType(mapping(field, mapType.keyType()), mapping(field, mapType.valueType()));
    }

    public static Type toIcebergType(SeaTunnelDataType dataType) {
        return toIcebergType(dataType, new AtomicInteger(1));
    }

    public static Type toIcebergType(Column column, AtomicInteger nextId) {
        return toIcebergType(column, nextId, DEFAULT_FORMAT_VERSION);
    }

    /**
     * Converts a column for a table created with the given format version. Timestamp columns with
     * nanosecond precision (scale 9) become {@code timestamp_ns} / {@code timestamptz_ns} on format
     * version 3. Other scales and older format versions keep the microsecond timestamp types, whose
     * values are not limited to the years 1677 to 2262.
     */
    public static Type toIcebergType(Column column, AtomicInteger nextId, int formatVersion) {
        if (column.getSinkType() != null) {
            return Types.fromPrimitiveString(column.getSinkType());
        }
        if (formatVersion >= NANOSECOND_TIMESTAMP_FORMAT_VERSION && isNanosecondTimestamp(column)) {
            return column.getDataType().getSqlType() == SqlType.TIMESTAMP_TZ
                    ? Types.TimestampNanoType.withZone()
                    : Types.TimestampNanoType.withoutZone();
        }
        return toIcebergType(column.getDataType(), nextId);
    }

    private static boolean isNanosecondTimestamp(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        return (sqlType == SqlType.TIMESTAMP || sqlType == SqlType.TIMESTAMP_TZ)
                && column.getScale() != null
                && column.getScale() >= NANOSECOND_SCALE;
    }

    public static Type toIcebergType(SeaTunnelDataType dataType, AtomicInteger nextId) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return Types.BooleanType.get();
            case BYTES:
                return Types.BinaryType.get();
            case SMALLINT:
            case TINYINT:
            case INT:
                return Types.IntegerType.get();
            case BIGINT:
                return Types.LongType.get();
            case FLOAT:
                return Types.FloatType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                // converter elementType
                Type elementType = toIcebergType(arrayType.getElementType(), nextId);
                return Types.ListType.ofOptional(nextId.getAndIncrement(), elementType);
            case MAP:
                org.apache.seatunnel.api.table.type.MapType mapType =
                        (org.apache.seatunnel.api.table.type.MapType) dataType;
                Type keyType = toIcebergType(mapType.getKeyType(), nextId);
                Type valueType = toIcebergType(mapType.getValueType(), nextId);
                return Types.MapType.ofOptional(
                        nextId.getAndIncrement(), nextId.getAndIncrement(), keyType, valueType);
            case ROW:
                SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) dataType;
                List<Types.NestedField> structFields = new ArrayList<>();
                for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                    String field = seaTunnelRowType.getFieldName(i);
                    SeaTunnelDataType fieldType = seaTunnelRowType.getFieldType(i);
                    structFields.add(
                            Types.NestedField.of(
                                    nextId.getAndIncrement(),
                                    true,
                                    field,
                                    toIcebergType(fieldType, nextId)));
                }
                return Types.StructType.of(structFields);
            case DATE:
                return Types.DateType.get();
            case TIME:
                return Types.TimeType.get();
            case TIMESTAMP:
                // NTZ → Iceberg withoutZone()
                return Types.TimestampType.withoutZone();
            case TIMESTAMP_TZ:
                // LTZ → Iceberg withZone()
                return Types.TimestampType.withZone();
            case STRING:
            default:
                return Types.StringType.get();
        }
    }
}
