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

package org.apache.seatunnel.connectors.seatunnel.lance.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/** Converts an Arrow schema / rows read from a Lance dataset into SeaTunnel types and rows. */
public class ArrowRowConverter {

    private ArrowRowConverter() {}

    /** Derives the SeaTunnel row type from a Lance dataset's Arrow schema. */
    public static SeaTunnelRowType toSeaTunnelRowType(Schema schema) {
        List<Field> fields = schema.getFields();
        String[] fieldNames = new String[fields.size()];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            fieldNames[i] = field.getName();
            fieldTypes[i] = toSeaTunnelType(field);
        }
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    /** Maps one Arrow field to a SeaTunnel data type, rejecting unsupported ones. */
    public static SeaTunnelDataType<?> toSeaTunnelType(Field field) {
        return toSeaTunnelType(field.getName(), field.getType(), field.getChildren());
    }

    /** Maps one Arrow field type to a SeaTunnel data type, rejecting unsupported ones. */
    public static SeaTunnelDataType<?> toSeaTunnelType(
            String fieldName, ArrowType arrowType, List<Field> children) {
        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() <= 32) {
                return BasicType.INT_TYPE;
            }
            return BasicType.LONG_TYPE;
        }
        if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
            if (floatType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return BasicType.FLOAT_TYPE;
            }
            return BasicType.DOUBLE_TYPE;
        }
        if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return BasicType.STRING_TYPE;
        }
        if (arrowType instanceof ArrowType.Bool) {
            return BasicType.BOOLEAN_TYPE;
        }
        if (arrowType instanceof ArrowType.Date) {
            return LocalTimeType.LOCAL_DATE_TYPE;
        }
        if (arrowType instanceof ArrowType.Time) {
            return LocalTimeType.LOCAL_TIME_TYPE;
        }
        if (arrowType instanceof ArrowType.Timestamp) {
            return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        }
        if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
        }
        if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary) {
            return PrimitiveByteArrayType.INSTANCE;
        }
        if (arrowType instanceof ArrowType.Null) {
            return BasicType.VOID_TYPE;
        }
        if (arrowType instanceof ArrowType.List) {
            if (children == null || children.isEmpty()) {
                throw new LanceConnectorException(
                        LanceConnectorErrorCode.UNSUPPORTED_FIELD_TYPE,
                        String.format(
                                "Field '%s' is a list without an element field; "
                                        + "cannot derive the array element type.",
                                fieldName));
            }
            return ArrayType.of(toSeaTunnelType(children.get(0)));
        }
        throw CommonError.unsupportedDataType("Lance", arrowType.toString(), fieldName);
    }

    /** Extracts the Java value of one cell using the vector's getObject, normalized per type. */
    public static Object readField(FieldVector vector, int rowIndex, SeaTunnelDataType<?> type) {
        Object value = vector.getObject(rowIndex);
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case STRING:
                return value instanceof Text ? value.toString() : value;
            case INT:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return value;
            case DATE:
                return toLocalDate(value);
            case TIME:
                return toLocalTime(value, vector);
            case TIMESTAMP:
                return toLocalDateTime(value, vector);
            case DECIMAL:
                return value;
            case BYTES:
                return value;
            case NULL:
                return null;
            case ARRAY:
                return toArray(value, ((ArrayType<?, ?>) type).getElementType());
            default:
                throw CommonError.unsupportedDataType(
                        "Lance", type.getSqlType().toString(), vector.getField().getName());
        }
    }

    private static LocalDate toLocalDate(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        if (value instanceof Number) {
            return LocalDate.ofEpochDay(((Number) value).longValue());
        }
        return LocalDate.parse(value.toString());
    }

    private static java.time.LocalTime toLocalTime(Object value, FieldVector vector) {
        if (value instanceof java.time.LocalTime) {
            return (java.time.LocalTime) value;
        }
        if (value instanceof LocalDateTime) {
            // TimeMilliVector.getObject returns an epoch-based LocalDateTime whose time-of-day
            // is the stored value; the time portion is what a TIME column means.
            return ((LocalDateTime) value).toLocalTime();
        }
        if (value instanceof Number) {
            return java.time.LocalTime.ofNanoOfDay(
                    scaleTimeOfDay(
                            ((Number) value).longValue(), unitOf(vector, ArrowType.Time.class)));
        }
        return java.time.LocalTime.parse(value.toString());
    }

    private static LocalDateTime toLocalDateTime(Object value, FieldVector vector) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Number) {
            // Timezone-aware timestamp vectors return the raw epoch value; convert it back
            // through the timestamp's declared timezone.
            long epoch = ((Number) value).longValue();
            String timezone = null;
            if (vector.getField().getType() instanceof ArrowType.Timestamp) {
                timezone = ((ArrowType.Timestamp) vector.getField().getType()).getTimezone();
            }
            ZoneId zone =
                    (timezone == null || timezone.isEmpty())
                            // Arrow normalizes timezone-naive timestamps to UTC.
                            ? ZoneOffset.UTC
                            : ZoneId.of(timezone);
            return LocalDateTime.ofInstant(
                    toInstant(epoch, unitOf(vector, ArrowType.Timestamp.class)), zone);
        }
        return LocalDateTime.parse(value.toString());
    }

    private static Instant toInstant(long epoch, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return Instant.ofEpochSecond(epoch);
            case MILLISECOND:
                return Instant.ofEpochMilli(epoch);
            case MICROSECOND:
                return Instant.ofEpochSecond(
                        Math.floorDiv(epoch, 1_000_000L),
                        Math.floorMod(epoch, 1_000_000L) * 1_000L);
            case NANOSECOND:
                return Instant.ofEpochSecond(
                        Math.floorDiv(epoch, 1_000_000_000L), Math.floorMod(epoch, 1_000_000_000L));
            default:
                throw CommonError.unsupportedDataType("Lance", unit.toString(), "timestamp");
        }
    }

    private static long scaleTimeOfDay(long timeOfDay, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return timeOfDay * 1_000_000_000L;
            case MILLISECOND:
                return timeOfDay * 1_000_000L;
            case MICROSECOND:
                return timeOfDay * 1_000L;
            case NANOSECOND:
                return timeOfDay;
            default:
                throw CommonError.unsupportedDataType("Lance", unit.toString(), "time");
        }
    }

    private static TimeUnit unitOf(FieldVector vector, Class<? extends ArrowType> expected) {
        ArrowType arrowType = vector.getField().getType();
        if (!expected.isInstance(arrowType)) {
            throw CommonError.unsupportedDataType(
                    "Lance", arrowType.toString(), vector.getField().getName());
        }
        if (arrowType instanceof ArrowType.Time) {
            return ((ArrowType.Time) arrowType).getUnit();
        }
        return ((ArrowType.Timestamp) arrowType).getUnit();
    }

    private static Object toArray(Object value, SeaTunnelDataType<?> elementType) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                    "Expected a java.util.List for ARRAY column, but got: " + value.getClass());
        }
        List<?> list = (List<?>) value;
        int size = list.size();
        switch (elementType.getSqlType()) {
            case INT:
                Integer[] ints = new Integer[size];
                for (int i = 0; i < size; i++) {
                    ints[i] = list.get(i) == null ? null : ((Number) list.get(i)).intValue();
                }
                return ints;
            case BIGINT:
                Long[] longs = new Long[size];
                for (int i = 0; i < size; i++) {
                    longs[i] = list.get(i) == null ? null : ((Number) list.get(i)).longValue();
                }
                return longs;
            case FLOAT:
                Float[] floats = new Float[size];
                for (int i = 0; i < size; i++) {
                    floats[i] = list.get(i) == null ? null : ((Number) list.get(i)).floatValue();
                }
                return floats;
            case DOUBLE:
                Double[] doubles = new Double[size];
                for (int i = 0; i < size; i++) {
                    doubles[i] = list.get(i) == null ? null : ((Number) list.get(i)).doubleValue();
                }
                return doubles;
            case BOOLEAN:
                return list.toArray(new Boolean[0]);
            case STRING:
                String[] strings = new String[size];
                for (int i = 0; i < size; i++) {
                    strings[i] = list.get(i) == null ? null : list.get(i).toString();
                }
                return strings;
            default:
                throw CommonError.unsupportedDataType(
                        "Lance", elementType.getSqlType().toString(), "array element");
        }
    }

    /**
     * Converts one batch of an Arrow reader into SeaTunnel rows.
     *
     * @param root the vector schema root positioned at the current batch
     * @param rowType the SeaTunnel row type derived from the dataset schema
     * @param offset index offset of this fragment within the dataset (informational; row order
     *     within a fragment is preserved, fragments are assigned and read independently)
     * @param collected rows are appended to this list
     */
    public static void convertBatch(
            VectorSchemaRoot root,
            SeaTunnelRowType rowType,
            long offset,
            List<SeaTunnelRow> collected) {
        List<FieldVector> vectors = root.getFieldVectors();
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            Object[] fields = new Object[vectors.size()];
            for (int col = 0; col < vectors.size(); col++) {
                fields[col] = readField(vectors.get(col), row, rowType.getFieldType(col));
            }
            collected.add(new SeaTunnelRow(fields));
        }
    }

    /** Builds a full row list from a vector schema root (single batch, offset 0). */
    public static List<SeaTunnelRow> convertAll(VectorSchemaRoot root, SeaTunnelRowType rowType) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        convertBatch(root, rowType, 0, rows);
        return rows;
    }
}
