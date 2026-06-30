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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BigtableSourceReader}.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>Checkpoint state includes the in-flight split (Issue 1 fix)
 *   <li>Streaming read path — rows emitted via forEach, not buffered (Issue 2 fix)
 *   <li>rowkey_column config drives row-key field mapping (Issue 3 fix)
 * </ul>
 */
class BigtableSourceReaderTest {

    private BigtableClient mockClient;
    private BigtableDataClient mockDataClient;
    private SourceReader.Context mockContext;
    private SeaTunnelRowType rowType;
    private BigtableParameters parameters;

    @BeforeEach
    void setUp() {
        mockClient = mock(BigtableClient.class);
        mockDataClient = mock(BigtableDataClient.class);
        when(mockClient.getDataClient()).thenReturn(mockDataClient);

        mockContext = mock(SourceReader.Context.class);

        rowType =
                new SeaTunnelRowType(
                        new String[] {"rowkey", "cf:name"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        parameters = BigtableParameters.builder().projectId("p").instanceId("i").table("t").build();
    }

    /** Matches production lifecycle: open() initializes the client before pollNext(). */
    private BigtableSourceReader createOpenedReader(
            BigtableParameters params, SeaTunnelRowType type) throws Exception {
        BigtableSourceReader reader =
                new BigtableSourceReader(params, mockContext, type, mockClient);
        reader.open();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private void mockReadStream(Runnable duringForEach) {
        ServerStream<Row> fakeStream = mock(ServerStream.class);
        Mockito.doAnswer(
                        invocation -> {
                            if (duringForEach != null) {
                                duringForEach.run();
                            }
                            return null;
                        })
                .when(fakeStream)
                .forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);
    }

    // -------------------------------------------------------------------------
    // Issue 1: snapshotState must include the currently-being-read split
    // -------------------------------------------------------------------------

    /**
     * When a split is being read (between addSplits and end of readSplit), snapshotState must
     * include it so that a failover can re-enqueue it.
     */
    @Test
    void testSnapshotStateIncludesInFlightSplit() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "a", "z");
        final List<BigtableSourceSplit>[] capturedState = new List[1];

        BigtableSourceReader reader = createOpenedReader(parameters, rowType);
        reader.addSplits(Collections.singletonList(split));

        mockReadStream(() -> capturedState[0] = reader.snapshotState(1L));

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        reader.pollNext(collector);

        assertTrue(
                capturedState[0].stream().anyMatch(s -> s.splitId().equals(split.splitId())),
                "snapshotState taken during readSplit() must include the in-flight split");
    }

    /**
     * After readSplit() completes, currentSplit is cleared. A snapshot taken after that must NOT
     * re-include the already-finished split.
     */
    @Test
    void testSnapshotStateAfterReadDoesNotDuplicateSplit() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");

        BigtableSourceReader reader = createOpenedReader(parameters, rowType);
        reader.addSplits(Collections.singletonList(split));

        mockReadStream(null);

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        reader.pollNext(collector);

        List<BigtableSourceSplit> state = reader.snapshotState(2L);
        assertTrue(state.isEmpty(), "State after completed read must be empty");
    }

    // -------------------------------------------------------------------------
    // Issue 2: rows must be emitted via streaming forEach, not buffered
    // -------------------------------------------------------------------------

    /**
     * Verifies that each row is emitted individually via output.collect() inside the forEach
     * lambda, rather than being buffered first.
     */
    @SuppressWarnings("unchecked")
    @Test
    void testRowsEmittedStreamingNotBuffered() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");

        Row fakeRow = mock(Row.class);
        RowCell cell = mock(RowCell.class);
        when(cell.getFamily()).thenReturn("cf");
        when(cell.getQualifier()).thenReturn(ByteString.copyFromUtf8("name"));
        when(cell.getValue()).thenReturn(ByteString.copyFromUtf8("alice"));
        when(fakeRow.getCells()).thenReturn(Collections.singletonList(cell));
        when(fakeRow.getKey()).thenReturn(ByteString.copyFromUtf8("row-1"));

        ServerStream<Row> fakeStream = mock(ServerStream.class);
        Mockito.doAnswer(
                        invocation -> {
                            Consumer<Row> action = invocation.getArgument(0);
                            action.accept(fakeRow);
                            return null;
                        })
                .when(fakeStream)
                .forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);

        BigtableSourceReader reader = createOpenedReader(parameters, rowType);
        reader.addSplits(Collections.singletonList(split));

        Object lock = new Object();
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(lock);

        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals("alice", captor.getValue().getField(1));
    }

    // -------------------------------------------------------------------------
    // Issue 3: rowkey_column config drives field mapping
    // -------------------------------------------------------------------------

    /**
     * When rowkey_column is configured, the named field should receive the row key value, not the
     * default literal "rowkey".
     */
    @SuppressWarnings("unchecked")
    @Test
    void testRowkeyColumnConfigMapsCorrectField() throws Exception {
        SeaTunnelRowType customRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "cf:value"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        BigtableParameters paramsWithRowkeyCol =
                BigtableParameters.builder()
                        .projectId("p")
                        .instanceId("i")
                        .table("t")
                        .rowkeyColumns(Collections.singletonList("id"))
                        .build();

        Row fakeRow = mock(Row.class);
        RowCell cell = mock(RowCell.class);
        when(cell.getFamily()).thenReturn("cf");
        when(cell.getQualifier()).thenReturn(ByteString.copyFromUtf8("value"));
        when(cell.getValue()).thenReturn(ByteString.copyFromUtf8("hello"));
        when(fakeRow.getCells()).thenReturn(Collections.singletonList(cell));
        when(fakeRow.getKey()).thenReturn(ByteString.copyFromUtf8("my-key"));

        ServerStream<Row> fakeStream = mock(ServerStream.class);
        Mockito.doAnswer(
                        invocation -> {
                            Consumer<Row> action = invocation.getArgument(0);
                            action.accept(fakeRow);
                            return null;
                        })
                .when(fakeStream)
                .forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);

        BigtableSourceReader reader = createOpenedReader(paramsWithRowkeyCol, customRowType);
        reader.addSplits(Collections.singletonList(new BigtableSourceSplit(0, "", "")));

        Object lock = new Object();
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(lock);

        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals("my-key", captor.getValue().getField(0));
        assertEquals("hello", captor.getValue().getField(1));
    }
}
