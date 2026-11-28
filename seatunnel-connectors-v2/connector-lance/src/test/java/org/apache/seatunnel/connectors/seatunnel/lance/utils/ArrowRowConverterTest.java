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
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Pure in-memory Arrow tests for the schema/row conversion; no lance JNI involved. */
public class ArrowRowConverterTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    @Test
    void testSchemaMappingCoversAllSupportedTypes() {
        Field listField =
                new Field(
                        "c_array",
                        FieldType.nullable(new ArrowType.List()),
                        Collections.singletonList(field("element", new ArrowType.Int(32, true))));
        Schema schema =
                new Schema(
                        Arrays.asList(
                                field("c_int", new ArrowType.Int(32, true)),
                                field("c_smallint", new ArrowType.Int(16, true)),
                                field("c_bigint", new ArrowType.Int(64, true)),
                                field(
                                        "c_float",
                                        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                                field(
                                        "c_double",
                                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                                field("c_string", new ArrowType.Utf8()),
                                field("c_large_string", new ArrowType.LargeUtf8()),
                                field("c_boolean", new ArrowType.Bool()),
                                field("c_date", new ArrowType.Date(DateUnit.DAY)),
                                field("c_time", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                                field(
                                        "c_timestamp",
                                        new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                                field("c_decimal", new ArrowType.Decimal(30, 8, 128)),
                                field("c_bytes", new ArrowType.Binary()),
                                field("c_null", new ArrowType.Null()),
                                listField));

        SeaTunnelRowType rowType = ArrowRowConverter.toSeaTunnelRowType(schema);

        Assertions.assertEquals(15, rowType.getTotalFields());
        Assertions.assertEquals("c_int", rowType.getFieldName(0));
        Assertions.assertEquals(BasicType.INT_TYPE, rowType.getFieldType(0));
        Assertions.assertEquals(BasicType.INT_TYPE, rowType.getFieldType(1));
        Assertions.assertEquals(BasicType.LONG_TYPE, rowType.getFieldType(2));
        Assertions.assertEquals(BasicType.FLOAT_TYPE, rowType.getFieldType(3));
        Assertions.assertEquals(BasicType.DOUBLE_TYPE, rowType.getFieldType(4));
        Assertions.assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(5));
        Assertions.assertEquals(BasicType.STRING_TYPE, rowType.getFieldType(6));
        Assertions.assertEquals(BasicType.BOOLEAN_TYPE, rowType.getFieldType(7));
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TYPE, rowType.getFieldType(8));
        Assertions.assertEquals(LocalTimeType.LOCAL_TIME_TYPE, rowType.getFieldType(9));
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, rowType.getFieldType(10));
        Assertions.assertEquals(new DecimalType(30, 8), rowType.getFieldType(11));
        Assertions.assertEquals(PrimitiveByteArrayType.INSTANCE, rowType.getFieldType(12));
        Assertions.assertEquals(BasicType.VOID_TYPE, rowType.getFieldType(13));
        Assertions.assertEquals(ArrayType.of(BasicType.INT_TYPE), rowType.getFieldType(14));
        Assertions.assertEquals("c_array", rowType.getFieldName(14));
    }

    @Test
    void testStructFieldRejected() {
        Field structField =
                new Field(
                        "c_struct",
                        FieldType.nullable(ArrowType.Struct.INSTANCE),
                        Collections.singletonList(field("inner", new ArrowType.Utf8())));
        SeaTunnelRuntimeException error =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> ArrowRowConverter.toSeaTunnelType(structField));
        Assertions.assertTrue(error.getMessage().contains("c_struct"), error.getMessage());
    }

    @Test
    void testVectorFieldRejected() {
        // Fixed-size binary is the Arrow type Lance uses for vector columns; it has no
        // faithful SeaTunnel counterpart and must be rejected naming the column.
        Field vectorField = field("c_vector", new ArrowType.FixedSizeBinary(16));
        SeaTunnelRuntimeException error =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () -> ArrowRowConverter.toSeaTunnelType(vectorField));
        Assertions.assertTrue(error.getMessage().contains("c_vector"), error.getMessage());
    }

    @Test
    void testListWithoutElementFieldRejected() {
        Field emptyList =
                new Field(
                        "c_array",
                        FieldType.nullable(new ArrowType.List()),
                        Collections.emptyList());
        LanceConnectorException error =
                Assertions.assertThrows(
                        LanceConnectorException.class,
                        () -> ArrowRowConverter.toSeaTunnelType(emptyList));
        Assertions.assertTrue(error.getMessage().contains("c_array"), error.getMessage());
    }

    @Test
    void testReadFieldScalarNormalization() {
        IntVector intVector =
                new IntVector("c_int", FieldType.nullable(new ArrowType.Int(32, true)), allocator);
        intVector.allocateNew(1);
        intVector.setSafe(0, 42);
        intVector.setValueCount(1);

        VarCharVector varcharVector =
                new VarCharVector("c_string", FieldType.nullable(new ArrowType.Utf8()), allocator);
        varcharVector.allocateNew(1);
        varcharVector.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
        varcharVector.setValueCount(1);

        BitVector bitVector =
                new BitVector("c_boolean", FieldType.nullable(new ArrowType.Bool()), allocator);
        bitVector.allocateNew(1);
        bitVector.setSafe(0, 1);
        bitVector.setValueCount(1);

        try {
            Assertions.assertEquals(
                    42, ArrowRowConverter.readField(intVector, 0, BasicType.INT_TYPE));
            Assertions.assertEquals(
                    "hello", ArrowRowConverter.readField(varcharVector, 0, BasicType.STRING_TYPE));
            Assertions.assertEquals(
                    Boolean.TRUE,
                    ArrowRowConverter.readField(bitVector, 0, BasicType.BOOLEAN_TYPE));
        } finally {
            intVector.close();
            varcharVector.close();
            bitVector.close();
        }
    }

    @Test
    void testReadFieldDateFromEpochDays() {
        DateDayVector dateVector =
                new DateDayVector(
                        "c_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), allocator);
        dateVector.allocateNew(1);
        dateVector.setSafe(0, 19000);
        dateVector.setValueCount(1);

        try {
            Assertions.assertEquals(
                    LocalDate.ofEpochDay(19000),
                    ArrowRowConverter.readField(dateVector, 0, LocalTimeType.LOCAL_DATE_TYPE));
        } finally {
            dateVector.close();
        }
    }

    @Test
    void testReadFieldTimeFromMillis() {
        TimeMilliVector timeVector =
                new TimeMilliVector(
                        "c_time",
                        FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                        allocator);
        timeVector.allocateNew(1);
        timeVector.setSafe(
                0, (int) LocalTime.of(1, 2, 3).get(java.time.temporal.ChronoField.MILLI_OF_DAY));
        timeVector.setValueCount(1);

        try {
            Assertions.assertEquals(
                    LocalTime.of(1, 2, 3),
                    ArrowRowConverter.readField(timeVector, 0, LocalTimeType.LOCAL_TIME_TYPE));
        } finally {
            timeVector.close();
        }
    }

    @Test
    void testReadFieldTimestampPassthrough() {
        TimeStampMicroVector timestampVector =
                new TimeStampMicroVector(
                        "c_timestamp",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                        allocator);
        timestampVector.allocateNew(1);
        LocalDateTime expected = LocalDateTime.of(2026, 11, 13, 10, 30, 15);
        timestampVector.setSafe(0, expected.toEpochSecond(ZoneOffset.UTC) * 1_000_000L);
        timestampVector.setValueCount(1);

        try {
            Object value =
                    ArrowRowConverter.readField(
                            timestampVector, 0, LocalTimeType.LOCAL_DATE_TIME_TYPE);
            Assertions.assertTrue(value instanceof LocalDateTime, String.valueOf(value));
            Assertions.assertEquals(expected.toLocalTime(), ((LocalDateTime) value).toLocalTime());
        } finally {
            timestampVector.close();
        }
    }

    @Test
    void testReadFieldTimezoneAwareTimestampFromEpoch() {
        // Timezone-aware timestamp vectors surface the raw epoch value from getObject.
        TimeStampMicroTZVector timestampVector =
                new TimeStampMicroTZVector(
                        "c_timestamp",
                        FieldType.nullable(
                                new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai")),
                        allocator);
        timestampVector.allocateNew(1);
        long epochMicro = 1_782_139_041_000_000L;
        timestampVector.setSafe(0, epochMicro);
        timestampVector.setValueCount(1);

        try {
            Object value =
                    ArrowRowConverter.readField(
                            timestampVector, 0, LocalTimeType.LOCAL_DATE_TIME_TYPE);
            Assertions.assertTrue(value instanceof LocalDateTime, String.valueOf(value));
            Assertions.assertEquals(
                    LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(1_782_139_041_000L), ZoneId.of("Asia/Shanghai")),
                    value);
        } finally {
            timestampVector.close();
        }
    }

    @Test
    void testReadFieldTimeFromEpochMicro() {
        // TimeMicroVector.getObject returns the raw micros-of-day value.
        TimeMicroVector timeVector =
                new TimeMicroVector(
                        "c_time",
                        FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                        allocator);
        timeVector.allocateNew(1);
        timeVector.setSafe(0, LocalTime.of(1, 2, 3).getLong(ChronoField.MICRO_OF_DAY));
        timeVector.setValueCount(1);

        try {
            Assertions.assertEquals(
                    LocalTime.of(1, 2, 3),
                    ArrowRowConverter.readField(timeVector, 0, LocalTimeType.LOCAL_TIME_TYPE));
        } finally {
            timeVector.close();
        }
    }

    @Test
    void testReadFieldArrayNormalization() {
        ListVector listVector = ListVector.empty("c_array", allocator);
        UnionListWriter writer = listVector.getWriter();
        writer.allocate();
        writer.setPosition(0);
        writer.startList();
        writer.writeInt(1);
        writer.writeInt(2);
        writer.endList();
        listVector.setValueCount(1);

        try {
            Assertions.assertArrayEquals(
                    new Integer[] {1, 2},
                    (Integer[])
                            ArrowRowConverter.readField(
                                    listVector, 0, ArrayType.of(BasicType.INT_TYPE)));
        } finally {
            listVector.close();
        }
    }

    @Test
    void testConvertBatchSetsOffsetsAndValues() {
        IntVector intVector =
                new IntVector("c_int", FieldType.nullable(new ArrowType.Int(32, true)), allocator);
        intVector.allocateNew(2);
        intVector.setSafe(0, 1);
        intVector.setSafe(1, 2);
        intVector.setValueCount(2);

        VarCharVector varcharVector =
                new VarCharVector("c_string", FieldType.nullable(new ArrowType.Utf8()), allocator);
        varcharVector.allocateNew(2);
        varcharVector.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
        varcharVector.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
        varcharVector.setValueCount(2);

        VectorSchemaRoot root = VectorSchemaRoot.of(intVector, varcharVector);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_int", "c_string"},
                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        try {
            List<SeaTunnelRow> rows = ArrowRowConverter.convertAll(root, rowType);

            Assertions.assertEquals(2, rows.size());
            Assertions.assertEquals(1, rows.get(0).getField(0));
            Assertions.assertEquals("a", rows.get(0).getField(1));
            Assertions.assertEquals(2, rows.get(1).getField(0));
            Assertions.assertEquals("b", rows.get(1).getField(1));

            // The offset parameter is informational only; both calls convert the same rows.
            List<SeaTunnelRow> offsetRows = new ArrayList<>();
            ArrowRowConverter.convertBatch(root, rowType, 100, offsetRows);
            Assertions.assertEquals(2, offsetRows.size());
            Assertions.assertEquals(1, offsetRows.get(0).getField(0));
            Assertions.assertEquals(2, offsetRows.get(1).getField(0));
        } finally {
            root.close();
        }
    }
}
