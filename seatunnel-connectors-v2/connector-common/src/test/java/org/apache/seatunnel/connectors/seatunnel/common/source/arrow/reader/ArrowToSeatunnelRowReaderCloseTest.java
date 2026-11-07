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
import org.apache.seatunnel.shade.org.apache.arrow.vector.VarCharVector;
import org.apache.seatunnel.shade.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.seatunnel.shade.org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.seatunnel.shade.org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.seatunnel.shade.org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.seatunnel.shade.org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.seatunnel.shade.org.apache.arrow.vector.types.pojo.DictionaryEncoding;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Verifies that Arrow reader cleanup releases the allocator even when stream reader close fails,
 * and that the production read path can drain real Arrow streams without tripping the allocator
 * leak check from issue #9863.
 */
public class ArrowToSeatunnelRowReaderCloseTest {

    /**
     * Covers the normal close path and preserves the required reader-before-allocator order.
     *
     * <p>This protects the Arrow allocator from being closed before the stream reader has released
     * vector buffers.
     */
    @Test
    public void testCloseReleasesAllocatorWhenStreamReaderCloseSucceeds() throws Exception {
        ArrowStreamReader arrowStreamReader = Mockito.mock(ArrowStreamReader.class);
        RootAllocator rootAllocator = Mockito.mock(RootAllocator.class);
        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(
                        arrowStreamReader, rootAllocator, createSeatunnelRowType());

        reader.close();

        InOrder inOrder = Mockito.inOrder(arrowStreamReader, rootAllocator);
        inOrder.verify(arrowStreamReader).close();
        inOrder.verify(rootAllocator).close();
    }

    /**
     * Covers the regression where an ArrowStreamReader close failure skipped allocator cleanup.
     *
     * <p>The reader close failure is still propagated, but the allocator must always receive its
     * close call.
     */
    @Test
    public void testCloseReleasesAllocatorWhenStreamReaderCloseFails() throws Exception {
        IOException closeFailure = new IOException("close failure");
        ArrowStreamReader arrowStreamReader = Mockito.mock(ArrowStreamReader.class);
        Mockito.doThrow(closeFailure).when(arrowStreamReader).close();
        RootAllocator rootAllocator = Mockito.mock(RootAllocator.class);
        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(
                        arrowStreamReader, rootAllocator, createSeatunnelRowType());

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, reader::close);

        Assertions.assertSame(closeFailure, exception.getCause());
        InOrder inOrder = Mockito.inOrder(arrowStreamReader, rootAllocator);
        inOrder.verify(arrowStreamReader).close();
        inOrder.verify(rootAllocator).close();
    }

    /**
     * Covers unchecked reader close failures so the allocator failure is never silently dropped.
     *
     * <p>An unchecked exception from ArrowStreamReader.close() must become the primary close
     * failure and any allocator close failure must be attached as a suppressed exception, otherwise
     * the allocator diagnostic for a real leak is lost during error handling.
     */
    @Test
    public void testCloseAggregatesUncheckedReaderFailureWithAllocatorFailure() throws Exception {
        RuntimeException readerFailure = new IllegalStateException("reader close failure");
        IllegalStateException allocatorFailure = new IllegalStateException("Memory was leaked");
        ArrowStreamReader arrowStreamReader = Mockito.mock(ArrowStreamReader.class);
        Mockito.doThrow(readerFailure).when(arrowStreamReader).close();
        RootAllocator rootAllocator = Mockito.mock(RootAllocator.class);
        Mockito.doThrow(allocatorFailure).when(rootAllocator).close();
        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(
                        arrowStreamReader, rootAllocator, createSeatunnelRowType());

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, reader::close);

        Assertions.assertSame(readerFailure, exception);
        Assertions.assertArrayEquals(new Throwable[] {allocatorFailure}, exception.getSuppressed());
        InOrder inOrder = Mockito.inOrder(arrowStreamReader, rootAllocator);
        inOrder.verify(arrowStreamReader).close();
        inOrder.verify(rootAllocator).close();
    }

    /**
     * Round-trip regression for the plain read path with a real allocator.
     *
     * <p>The reader is built through the production byte-array constructor, so readArrow() runs the
     * same load, convert, and close sequence Doris and StarRocks sources use. Any close-order
     * regression that leaves allocations behind surfaces here as an IllegalStateException from
     * RootAllocator.close().
     */
    @Test
    public void testReadArrowDrainsRealStreamAndClosesWithoutLeak() throws Exception {
        byte[] payload = writePlainIntStream();

        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(payload, createSeatunnelRowType());

        Assertions.assertDoesNotThrow(reader::readArrow);
        Assertions.assertEquals(3, reader.getReadRowCount());
        Assertions.assertEquals(Arrays.asList(1, 2, 3), drainIds(reader));
    }

    /**
     * Round-trip regression for the exact issue #9863 failure signature.
     *
     * <p>Dictionary batches are allocated by ArrowStreamReader outside the VectorSchemaRoot, so
     * closing the allocator before the stream reader (the pre-fix order) fails with "Memory was
     * leaked by query" even for a fully drained stream. This test writes a dictionary-encoded
     * column and must fail if the reader-before-allocator close order is ever reverted. The encoded
     * column is intentionally absent from the SeaTunnel schema; the reader skips unknown fields,
     * keeping the assertion focused on resource release.
     */
    @Test
    public void testReadArrowReleasesDictionaryAllocationsOnClose() throws Exception {
        byte[] payload = writeDictionaryEncodedStream();

        ArrowToSeatunnelRowReader reader =
                new ArrowToSeatunnelRowReader(payload, createSeatunnelRowType());

        Assertions.assertDoesNotThrow(reader::readArrow);
        Assertions.assertEquals(3, reader.getReadRowCount());
        Assertions.assertEquals(Arrays.asList(1, 2, 3), drainIds(reader));
    }

    /**
     * Writes a single-batch Arrow stream holding one int column named "id" with values 1, 2, 3.
     *
     * <p>The writer side uses its own allocator; leaked writer allocations would fail the test when
     * the try-with-resources closes that allocator.
     */
    private byte[] writePlainIntStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator writerAllocator = new RootAllocator(Integer.MAX_VALUE);
                IntVector idVector = new IntVector("id", writerAllocator)) {
            fillIds(idVector);
            try (VectorSchemaRoot writerRoot = VectorSchemaRoot.of(idVector)) {
                writerRoot.setRowCount(3);
                try (ArrowStreamWriter writer =
                        new ArrowStreamWriter(writerRoot, null, Channels.newChannel(out))) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
            }
        }
        return out.toByteArray();
    }

    /**
     * Writes a single-batch Arrow stream with a plain "id" column plus a dictionary-encoded "tag"
     * column, forcing the reader to hold dictionary allocations outside the root.
     */
    private byte[] writeDictionaryEncodedStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (BufferAllocator writerAllocator = new RootAllocator(Integer.MAX_VALUE);
                VarCharVector dictionaryValues =
                        new VarCharVector("tag_dictionary", writerAllocator);
                VarCharVector rawTags = new VarCharVector("tag", writerAllocator);
                IntVector idVector = new IntVector("id", writerAllocator)) {
            dictionaryValues.allocateNew(2);
            dictionaryValues.set(0, "apple".getBytes(StandardCharsets.UTF_8));
            dictionaryValues.set(1, "banana".getBytes(StandardCharsets.UTF_8));
            dictionaryValues.setValueCount(2);
            Dictionary dictionary =
                    new Dictionary(
                            dictionaryValues,
                            new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)));

            rawTags.allocateNew(3);
            rawTags.set(0, "apple".getBytes(StandardCharsets.UTF_8));
            rawTags.set(1, "banana".getBytes(StandardCharsets.UTF_8));
            rawTags.set(2, "apple".getBytes(StandardCharsets.UTF_8));
            rawTags.setValueCount(3);

            fillIds(idVector);

            try (FieldVector encodedTags =
                    (FieldVector) DictionaryEncoder.encode(rawTags, dictionary)) {
                DictionaryProvider.MapDictionaryProvider provider =
                        new DictionaryProvider.MapDictionaryProvider(dictionary);
                try (VectorSchemaRoot writerRoot = VectorSchemaRoot.of(idVector, encodedTags)) {
                    writerRoot.setRowCount(3);
                    try (ArrowStreamWriter writer =
                            new ArrowStreamWriter(writerRoot, provider, Channels.newChannel(out))) {
                        writer.start();
                        writer.writeBatch();
                        writer.end();
                    }
                }
            }
        }
        return out.toByteArray();
    }

    /** Populates the shared "id" test column with values 1, 2, 3. */
    private void fillIds(IntVector idVector) {
        idVector.allocateNew(3);
        idVector.set(0, 1);
        idVector.set(1, 2);
        idVector.set(2, 3);
        idVector.setValueCount(3);
    }

    /** Drains all rows from the reader and returns the "id" column values in read order. */
    private List<Integer> drainIds(ArrowToSeatunnelRowReader reader) {
        List<Integer> ids = new ArrayList<>();
        while (reader.hasNext()) {
            ids.add((Integer) reader.next().getField(0));
        }
        return ids;
    }

    /**
     * Builds the minimal SeaTunnel schema needed to initialize the reader resource owner.
     *
     * <p>Only the "id" column is declared on purpose: the dictionary-encoded "tag" column must stay
     * out of the schema so the round-trip test exercises the unknown-field skip path while still
     * forcing dictionary allocations on the reader side.
     */
    private SeaTunnelRowType createSeatunnelRowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
    }
}
