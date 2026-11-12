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

package org.apache.seatunnel.connectors.seatunnel.common.source.arrow.reader;

import org.apache.seatunnel.shade.org.apache.arrow.memory.BufferAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.memory.RootAllocator;
import org.apache.seatunnel.shade.org.apache.arrow.vector.FieldVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.IntVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.MapVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.StructVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamWriter;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Verifies that a null cell in a complex column reads back as null.
 *
 * <p>The list converters guard the generics convert overload with an isNull check, but
 * StructConverter and MapConverter did not, so a nullable ROW column aborted the read with a
 * NullPointerException and a nullable MAP column silently became an empty map. Both reach the
 * production path through ArrowToSeatunnelRowReader, which is what the Doris and StarRocks sources
 * read with.
 *
 * <p>Every stream here holds a single record batch, so a failure points at the converter dispatch
 * and not at a batch boundary. Each fixture also carries a plain "id" column, which must keep its
 * value on the null row to prove a null complex cell does not blank the rest of the row.
 */
public class ArrowToSeatunnelRowReaderNullCellTest {

    /**
     * A null ROW cell must read as null rather than throwing.
     *
     * <p>Before the guard, StructConverter called entrySet() on the null result of getObject() and
     * the whole read failed, so one null struct cell cost the entire split rather than one value.
     */
    @Test
    public void testNullStructCellReadsAsNull() throws Exception {
        List<SeaTunnelRow> rows = readAll(writeStructStream(), structRowType());

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(
                Collections.singletonMap("age", 30),
                rows.get(0).getField(1),
                "the populated struct cell must still convert through the generics overload");
        Assertions.assertNull(
                rows.get(1).getField(1), "a null struct cell must read as null, not throw");
        Assertions.assertEquals(
                2, rows.get(1).getField(0), "a null struct must not blank the scalars in its row");
    }

    /**
     * A null MAP cell must read as null rather than as an empty map.
     *
     * <p>MapConverter positions the reader and loops on next(), which yields nothing for a null
     * cell, so the method returned an empty map.
     */
    @Test
    public void testNullMapCellReadsAsNull() throws Exception {
        List<SeaTunnelRow> rows = readAll(writeMapStream(), mapRowType());

        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(
                Collections.singletonMap(1, 10),
                rows.get(0).getField(1),
                "the populated map cell must still convert through the generics overload");
        Assertions.assertNull(
                rows.get(1).getField(1), "a null map cell must read as null, not as an empty map");
        Assertions.assertEquals(
                2, rows.get(1).getField(0), "a null map must not blank the scalars in its row");
    }

    /**
     * A cell holding a genuinely empty map must still read as an empty map.
     *
     * <p>This is the other half of the null guard: the guard must separate absent from empty rather
     * than collapse both to null, so a row that really holds no entries keeps reading as an empty
     * map while only a null cell reads as null.
     */
    @Test
    public void testEmptyMapCellStaysAnEmptyMap() throws Exception {
        List<SeaTunnelRow> rows = readAll(writeMapStream(), mapRowType());

        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(
                Collections.emptyMap(),
                rows.get(2).getField(1),
                "an empty map cell must stay an empty map and stay distinct from a null cell");
        Assertions.assertEquals(
                3, rows.get(2).getField(0), "an empty map must not blank the scalars in its row");
    }

    /**
     * Writes a single-batch stream with an "id" column and a "profile" struct column whose second
     * row is null.
     */
    private byte[] writeStructStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                IntVector idVector = new IntVector("id", allocator);
                StructVector profileVector = StructVector.empty("profile", allocator)) {
            fillIds(idVector, 2);

            NullableStructWriter profileWriter = profileVector.getWriter();
            profileWriter.setPosition(0);
            profileWriter.start();
            profileWriter.integer("age").writeInt(30);
            profileWriter.end();
            profileVector.setNull(1);
            profileVector.setValueCount(2);

            writeBatch(out, 2, idVector, profileVector);
        }
        return out.toByteArray();
    }

    /**
     * Writes a single-batch stream with an "id" column and a "labels" map column holding one
     * populated row, one null row and one empty-map row, in that order.
     */
    private byte[] writeMapStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                IntVector idVector = new IntVector("id", allocator);
                MapVector labelsVector = MapVector.empty("labels", allocator, false)) {
            fillIds(idVector, 3);

            UnionMapWriter labelsWriter = labelsVector.getWriter();
            labelsWriter.setPosition(0);
            labelsWriter.startMap();
            labelsWriter.startEntry();
            labelsWriter.key().integer().writeInt(1);
            labelsWriter.value().integer().writeInt(10);
            labelsWriter.endEntry();
            labelsWriter.endMap();
            labelsVector.setNull(1);
            labelsWriter.setPosition(2);
            labelsWriter.startMap();
            labelsWriter.endMap();
            labelsVector.setValueCount(3);

            writeBatch(out, 3, idVector, labelsVector);
        }
        return out.toByteArray();
    }

    /** Populates the shared "id" column with 1-based row numbers, so every row has a sentinel. */
    private void fillIds(IntVector idVector, int rowCount) {
        idVector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            idVector.set(i, i + 1);
        }
        idVector.setValueCount(rowCount);
    }

    /** Writes the given vectors as one record batch. */
    private void writeBatch(ByteArrayOutputStream out, int rowCount, FieldVector... vectors)
            throws IOException {
        try (VectorSchemaRoot writerRoot = VectorSchemaRoot.of(vectors)) {
            writerRoot.setRowCount(rowCount);
            try (ArrowStreamWriter writer =
                    new ArrowStreamWriter(writerRoot, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
    }

    /** Drains the reader through the production byte-array constructor. */
    private List<SeaTunnelRow> readAll(byte[] payload, SeaTunnelRowType rowType) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        try (ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(payload, rowType).readArrow()) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
        return rows;
    }

    /** Schema of an int column plus a ROW column, which dispatches to StructConverter. */
    private SeaTunnelRowType structRowType() {
        SeaTunnelRowType profileType =
                new SeaTunnelRowType(
                        new String[] {"age"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return new SeaTunnelRowType(
                new String[] {"id", "profile"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, profileType});
    }

    /** Schema of an int column plus a MAP column, which dispatches to MapConverter. */
    private SeaTunnelRowType mapRowType() {
        MapType<Integer, Integer> labelsType =
                new MapType<>(BasicType.INT_TYPE, BasicType.INT_TYPE);
        return new SeaTunnelRowType(
                new String[] {"id", "labels"},
                new SeaTunnelDataType[] {BasicType.INT_TYPE, labelsType});
    }
}
