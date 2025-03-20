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

package org.apache.seatunnel.connectors.seatunnel.common.source.arrow;

import org.apache.seatunnel.shade.com.google.common.base.Stopwatch;
import org.apache.seatunnel.shade.io.netty.util.CharsetUtil;
import org.apache.seatunnel.shade.org.apache.arrow.memory.ArrowBuf;
import org.apache.seatunnel.shade.org.apache.arrow.memory.BufferAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.BigIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.BitVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.DateDayVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.DateMilliVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.DecimalVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float4Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.Float8Vector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.IntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.LargeVarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.SmallIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TimeMicroVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.TinyIntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarBinaryVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.ListVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.seatunnel.shade.org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.TimeUnit;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.pojo.Field;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.pojo.Schema;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader.ArrowToSeatunnelRowReader;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ArrowToSeatunnelRowReaderTest {

    private static VectorSchemaRoot root;
    private static RootAllocator rootAllocator;
    private static final List<SeaTunnelDataTypeHolder> seaTunnelDataTypeHolder = new ArrayList<>();

    private static final LocalDateTime localDateTime =
            LocalDateTime.parse(
                    "2025-02-15 02:21:23", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    private static final List<String> stringData = new ArrayList<>();
    private static final List<Byte> byteData = new ArrayList<>();
    private static final List<Short> shortData = new ArrayList<>();
    private static final List<Integer> intData = new ArrayList<>();
    private static final List<Long> longData = new ArrayList<>();
    private static final float floatData = 1.23f;
    private static final double doubleData = 1.23456789d;
    private static final BigDecimal decimalData = new BigDecimal("1234567.89");
    private static final List<List<Integer>> arrayData1 = new ArrayList<>();
    private static final List<List<LocalDateTime>> arrayData2 = new ArrayList<>();
    private static final List<Map<String, LocalDateTime>> mapData = new ArrayList<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
        root = buildVectorSchemaRoot(rootAllocator, 10, true);
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("boolean", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("byte", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("short", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("int", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("long", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("float", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("double", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("string1", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("decimal", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("timestamp1", 1));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("string2", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("string3", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("timestamp2", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("time", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("date1", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("date2", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("array1", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("array2", 0));
        seaTunnelDataTypeHolder.add(new SeaTunnelDataTypeHolder("map", 0));
    }

    private static VectorSchemaRoot buildVectorSchemaRoot(
            RootAllocator rootAllocator, int count, boolean allType) {
        List<FieldVector> vectors = new ArrayList<>();
        ZoneId zoneId = ZoneId.systemDefault();
        vectors.add(new BitVector("boolean", rootAllocator));
        vectors.add(new TinyIntVector("byte", rootAllocator));
        vectors.add(new SmallIntVector("short", rootAllocator));
        vectors.add(new IntVector("int", rootAllocator));
        vectors.add(new BigIntVector("long", rootAllocator));
        vectors.add(new Float4Vector("float", rootAllocator));
        vectors.add(new Float8Vector("double", rootAllocator));
        // varchar
        vectors.add(new VarCharVector("string1", rootAllocator));
        vectors.add(
                new DecimalVector(
                        Field.nullable("decimal", new ArrowType.Decimal(10, 2, 128)),
                        rootAllocator));
        // timestamp without timezone
        vectors.add(new TimeStampMicroVector("timestamp1", rootAllocator));
        if (allType) {
            // byte[]
            vectors.add(new VarBinaryVector("string2", rootAllocator));
            // text
            vectors.add(new LargeVarCharVector("string3", rootAllocator));
            // timestamp with timezone
            vectors.add(
                    new TimeStampMilliTZVector(
                            Field.nullable(
                                    "timestamp2",
                                    new ArrowType.Timestamp(
                                            TimeUnit.MILLISECOND, ZoneId.systemDefault().getId())),
                            rootAllocator));
            vectors.add(new TimeMicroVector("time", rootAllocator));
            vectors.add(new DateMilliVector("date1", rootAllocator));
            vectors.add(new DateDayVector("date2", rootAllocator));
            // array int
            vectors.add(ListVector.empty("array1", rootAllocator));
            // array int
            vectors.add(ListVector.empty("array2", rootAllocator));
            // map
        }
        // allocate storage
        vectors.forEach(FieldVector::allocateNew);
        long epochMilli = localDateTime.atZone(zoneId).toInstant().toEpochMilli();

        byte byteStart = 'a';

        // setVectorValue
        vectors.forEach(
                vector -> {
                    for (int i = 0; i < count; i++) {
                        String stringValue = "test" + i;
                        if (vector instanceof BitVector) {
                            ((BitVector) vector).setSafe(i, i % 2 == 0 ? 0 : 1);
                        } else if (vector instanceof TinyIntVector) {
                            int i1 = byteStart + i;
                            byteData.add((byte) i1);
                            ((TinyIntVector) vector).setSafe(i, i1);
                        } else if (vector instanceof SmallIntVector) {
                            shortData.add((short) i);
                            ((SmallIntVector) vector).setSafe(i, i);
                        } else if (vector instanceof IntVector) {
                            intData.add(i);
                            ((IntVector) vector).setSafe(i, i);
                        } else if (vector instanceof BigIntVector) {
                            longData.add((long) i);
                            ((BigIntVector) vector).setSafe(i, i);
                        } else if (vector instanceof Float4Vector) {
                            ((Float4Vector) vector).setSafe(i, floatData);
                        } else if (vector instanceof Float8Vector) {
                            ((Float8Vector) vector).setSafe(i, doubleData);
                        } else if (vector instanceof DecimalVector) {
                            ((DecimalVector) vector).setSafe(i, decimalData);
                        } else if (vector instanceof VarCharVector) {
                            stringData.add(stringValue);
                            ((VarCharVector) vector)
                                    .setSafe(i, (stringValue).getBytes(StandardCharsets.UTF_8));
                        } else if (vector instanceof TimeStampMicroVector) {
                            ((TimeStampMicroVector) vector).setSafe(i, epochMilli * 1000);
                        } else if (vector instanceof VarBinaryVector) {
                            ((VarBinaryVector) vector)
                                    .setSafe(i, (stringValue).getBytes(StandardCharsets.UTF_8));
                        } else if (vector instanceof LargeVarCharVector) {
                            ((LargeVarCharVector) vector)
                                    .setSafe(i, (stringValue).getBytes(StandardCharsets.UTF_8));
                        } else if (vector instanceof TimeStampMilliTZVector) {
                            ((TimeStampMilliTZVector) vector).setSafe(i, epochMilli);
                        } else if (vector instanceof TimeMicroVector) {
                            ((TimeMicroVector) vector).setSafe(i, epochMilli);
                        } else if (vector instanceof DateMilliVector) {
                            ((DateMilliVector) vector).setSafe(i, epochMilli);
                        } else if (vector instanceof DateDayVector) {
                            ((DateDayVector) vector)
                                    .setSafe(i, (int) localDateTime.toLocalDate().toEpochDay());
                        }
                    }
                });

        // setListVectorValue
        vectors.stream()
                .filter(vector -> vector instanceof ListVector)
                .forEach(
                        vector -> {
                            ListVector listVector = (ListVector) vector;
                            String name = listVector.getField().getName();
                            UnionListWriter writer = listVector.getWriter();
                            for (int i = 0; i < count; i++) {
                                writer.startList();
                                writer.setPosition(i);
                                if ("array1".equals(name)) {
                                    List<Integer> intList = new ArrayList<>();
                                    for (int j = 0; j < 5; j++) {
                                        int i1 = j + i;
                                        writer.writeInt(i1);
                                        intList.add(i1);
                                    }
                                    writer.setValueCount(5);
                                    writer.endList();
                                    arrayData1.add(intList);
                                }
                                if ("array2".equals(name)) {
                                    List<LocalDateTime> dateTimeList = new ArrayList<>();
                                    for (int j = 0; j < 5; j++) {
                                        writer.writeTimeStampMilliTZ(epochMilli);
                                        dateTimeList.add(localDateTime);
                                    }
                                    writer.setValueCount(5);
                                    writer.endList();
                                    arrayData2.add(dateTimeList);
                                }
                            }
                        });
        // setMapVectorValue

        // setValueCount
        vectors.forEach(vector -> vector.setValueCount(count));
        List<Field> fields =
                vectors.stream().map(FieldVector::getField).collect(Collectors.toList());
        Schema schema = new Schema(fields);
        return new VectorSchemaRoot(schema, vectors, count);
    }

    private static void writeKeyAndValue(
            UnionMapWriter writer, Object value, int rowIndex, BufferAllocator allocator) {
        writer.setPosition(rowIndex);
        if (value instanceof String) {
            byte[] bytes = ((String) value).getBytes(CharsetUtil.UTF_8);
            ArrowBuf buffer = allocator.buffer(bytes.length);
            buffer.writeBytes(bytes);
            VarCharHolder holder = new VarCharHolder();
            holder.start = 0;
            holder.buffer = buffer;
            holder.end = bytes.length;
            writer.write(holder);
        } else if (value instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) value;
            TimeMilliHolder holder = new TimeMilliHolder();
            holder.value = (int) dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            writer.write(holder);
        }
    }

    @Test
    public void testSeatunnelRow() throws Exception {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer =
                        new ArrowStreamWriter(
                                root, /*DictionaryProvider=*/ null, Channels.newChannel(out))) {
            writer.writeBatch();
            out.flush();
            List<SeaTunnelRow> rows = new ArrayList<>();
            try (ArrowToSeatunnelRowReader reader =
                    new ArrowToSeatunnelRowReader(out.toByteArray(), getSeatunnelRowType(true))
                            .readArrow()) {
                while (reader.hasNext()) {
                    rows.add(reader.next());
                }
                Assertions.assertEquals(10, rows.size());
            }
            // check boolean
            List<Object> actualBooleanData =
                    rows.stream().map(s -> s.getField(0)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList(Boolean.FALSE, Boolean.TRUE), actualBooleanData);
            // check byte
            List<Object> actualByteData =
                    rows.stream().map(s -> s.getField(1)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(byteData, actualByteData);
            // check short
            List<Object> actualShortData =
                    rows.stream().map(s -> s.getField(2)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(shortData, actualShortData);
            // check int
            List<Object> actualIntData =
                    rows.stream().map(s -> s.getField(3)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(intData, actualIntData);
            // check long
            List<Object> actualLongData =
                    rows.stream().map(s -> s.getField(4)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(longData, actualLongData);
            // check float
            List<Object> actualFloatData =
                    rows.stream().map(s -> s.getField(5)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(floatData), actualFloatData);
            // check double
            List<Object> actualDoubleData =
                    rows.stream().map(s -> s.getField(6)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(doubleData), actualDoubleData);
            // check string1
            List<Object> actualStringData =
                    rows.stream().map(s -> s.getField(7)).collect(Collectors.toList());
            Assertions.assertEquals(stringData, actualStringData);
            // check decimal
            List<Object> actualDecimalData =
                    rows.stream().map(s -> s.getField(8)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(decimalData), actualDecimalData);
            // check timestamp without tz
            List<Object> actualTimestamp1Data =
                    rows.stream().map(s -> s.getField(9)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(localDateTime), actualTimestamp1Data);
            // check string2
            List<Object> actualString2Data =
                    rows.stream().map(s -> s.getField(10)).collect(Collectors.toList());
            Assertions.assertEquals(stringData, actualString2Data);
            // check string3
            List<Object> actualString3Data =
                    rows.stream().map(s -> s.getField(11)).collect(Collectors.toList());
            Assertions.assertEquals(stringData, actualString3Data);

            // check timestamp with tz
            List<Object> actualTimestamp2Data =
                    rows.stream().map(s -> s.getField(12)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(Collections.singletonList(localDateTime), actualTimestamp2Data);

            // check time
            List<Object> actualTimeDate =
                    rows.stream().map(s -> s.getField(13)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(
                    Collections.singletonList(localDateTime.toLocalTime()), actualTimeDate);
            // check date1
            List<Object> actualDate1Data =
                    rows.stream().map(s -> s.getField(14)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(
                    Collections.singletonList(localDateTime.toLocalDate()), actualDate1Data);
            // check date2
            List<Object> actualDate2Data =
                    rows.stream().map(s -> s.getField(15)).distinct().collect(Collectors.toList());
            Assertions.assertEquals(
                    Collections.singletonList(localDateTime.toLocalDate()), actualDate2Data);
            // check array int
            List<Object> actualArrayIntData =
                    rows.stream().map(s -> s.getField(16)).collect(Collectors.toList());
            Assertions.assertIterableEquals(arrayData1, actualArrayIntData);
            // check array timestamp
            List<Object> actualArrayTimestampData =
                    rows.stream().map(s -> s.getField(17)).collect(Collectors.toList());
            Assertions.assertIterableEquals(arrayData2, actualArrayTimestampData);
            // todo check map
            // The java api has problems building MapVectors,and there are no examples on the
            // official website
            // @see https://github.com/apache/arrow/issues/44664
        }
    }

    @Test
    public void testConvertArrowSpeed() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        int count = 1000000;
        try (RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
                VectorSchemaRoot vectorSchemaRoot =
                        buildVectorSchemaRoot(rootAllocator, count, false);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer =
                        new ArrowStreamWriter(
                                vectorSchemaRoot,
                                /*DictionaryProvider=*/ null,
                                Channels.newChannel(out))) {
            stopwatch.stop();
            System.out.printf(
                    "build %s rows vectorSchemaRoot cost %s ms \n",
                    count, stopwatch.elapsed(java.util.concurrent.TimeUnit.MILLISECONDS));
            writer.writeBatch();
            out.flush();
            List<SeaTunnelRow> rows = new ArrayList<>();
            stopwatch.reset().start();
            SeaTunnelRowType seatunnelRowType = getSeatunnelRowType(false);
            try (ArrowToSeatunnelRowReader reader =
                    new ArrowToSeatunnelRowReader(out.toByteArray(), seatunnelRowType)
                            .readArrow()) {
                while (reader.hasNext()) {
                    rows.add(reader.next());
                }
                stopwatch.stop();
                System.out.printf(
                        "read %s rows cost %s ms ",
                        rows.size(), stopwatch.elapsed(java.util.concurrent.TimeUnit.MILLISECONDS));
                Assertions.assertEquals(count, rows.size());
            }
        }
    }

    private SeaTunnelRowType getSeatunnelRowType(boolean allType) {
        String[] fieldNames =
                seaTunnelDataTypeHolder.stream()
                        .filter(h -> allType ? h.getFlag() >= 0 : h.getFlag() == 1)
                        .map(SeaTunnelDataTypeHolder::getFiledName)
                        .toArray(String[]::new);
        SeaTunnelDataType[] seaTunnelDataTypes =
                seaTunnelDataTypeHolder.stream()
                        .filter(h -> allType ? h.getFlag() >= 0 : h.getFlag() == 1)
                        .map(SeaTunnelDataTypeHolder::getSeatunnelDataType)
                        .toArray(SeaTunnelDataType[]::new);
        return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        try {
            if (root != null) {
                root.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to close arrow stream reader.", e);
        }
    }
}
