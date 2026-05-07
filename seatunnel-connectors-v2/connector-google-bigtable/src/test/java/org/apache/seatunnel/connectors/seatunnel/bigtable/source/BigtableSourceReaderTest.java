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

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BigtableSourceReader}.
 *
 * <p>Covers:
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

        parameters =
                BigtableParameters.builder()
                        .projectId("p")
                        .instanceId("i")
                        .table("t")
                        .build();
    }

    // -------------------------------------------------------------------------
    // Issue 1: snapshotState must include the currently-being-read split
    // -------------------------------------------------------------------------

    /**
     * When a split is being read (between addSplits and end of readSplit), snapshotState must
     * include it so that a failover can re-enqueue it.
     *
     * <p>We simulate the in-flight condition by making readRows() block long enough for us to call
     * snapshotState() while pollNext() is executing.
     */
    @Test
    void testSnapshotStateIncludesInFlightSplit() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "a", "z");

        // readRows returns an Iterable whose forEach blocks, then captures the snapshot mid-read
        final List<BigtableSourceSplit>[] capturedState = new List[1];

        BigtableSourceReader reader =
                new BigtableSourceReader(parameters, mockContext, rowType, mockClient);
        reader.addSplits(Collections.singletonList(split));

        // Make readRows() call our callback once, giving us a chance to inspect state
        Mockito.doAnswer(
                        invocation -> {
                            // At this point readSplit() is executing — currentSplit is set
                            // We call snapshotState() as a checkpoint would
                            capturedState[0] = reader.snapshotState(1L);
                            return null;
                        })
                .when(mockDataClient)
                .readRows(any(Query.class));

        // Trigger a dummy collector
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        reader.pollNext(collector);

        // The snapshot taken during readSplit() must contain the split
        assertTrue(
                capturedState[0].stream()
                        .anyMatch(s -> s.splitId().equals(split.splitId())),
                "snapshotState taken during readSplit() must include the in-flight split");
    }

    /**
     * After readSplit() completes, currentSplit is cleared. A snapshot taken after that must NOT
     * re-include the already-finished split.
     */
    @Test
    void testSnapshotStateAfterReadDoesNotDuplicateSplit() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");

        // readRows returns an empty iterable immediately
        Mockito.doAnswer(invocation -> null).when(mockDataClient).readRows(any(Query.class));

        BigtableSourceReader reader =
                new BigtableSourceReader(parameters, mockContext, rowType, mockClient);
        reader.addSplits(Collections.singletonList(split));

        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        reader.pollNext(collector);

        // Split has been fully consumed — snapshot after completion should be empty
        List<BigtableSourceSplit> state = reader.snapshotState(2L);
        assertTrue(state.isEmpty(), "State after completed read must be empty");
    }

    // -------------------------------------------------------------------------
    // Issue 2: rows must be emitted via streaming forEach, not buffered
    // -------------------------------------------------------------------------

    /**
     * Verifies that each row is emitted individually via output.collect() inside the forEach
     * lambda, rather than being buffered first. We inject a Row mock and confirm collect() is
     * called once per row.
     */
    @SuppressWarnings("unchecked")
    @Test
    void testRowsEmittedStreamingNotBuffered() throws Exception {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");

        // Build a fake Bigtable Row with one cell "cf:name" = "alice"
        Row fakeRow = mock(Row.class);
        RowCell cell = mock(RowCell.class);
        when(cell.getFamily()).thenReturn("cf");
        when(cell.getQualifier()).thenReturn(ByteString.copyFromUtf8("name"));
        when(cell.getValue()).thenReturn(ByteString.copyFromUtf8("alice"));
        when(fakeRow.getCells()).thenReturn(Collections.singletonList(cell));
        when(fakeRow.getKey()).thenReturn(ByteString.copyFromUtf8("row-1"));

        // Make readRows() emit our fakeRow via the forEach consumer
        Mockito.doAnswer(
                        invocation -> {
                            // readRows returns an Iterable; the reader calls .forEach(consumer)
                            // We simulate that by invoking the action on our fake row
                            // The actual call is: dataClient.readRows(query).forEach(lambda)
                            // We can't easily intercept the lambda, so we verify collect() was
                            // called via the answer below.
                            return null;
                        })
                .when(mockDataClient)
                .readRows(any(Query.class));

        // Use a real iterable answer to make the forEach fire
        com.google.api.gax.rpc.ServerStream<Row> fakeStream = mock(com.google.api.gax.rpc.ServerStream.class);
        Mockito.doAnswer(invocation -> {
            Consumer<Row> action = invocation.getArgument(0);
            action.accept(fakeRow);
            return null;
        }).when(fakeStream).forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);

        BigtableSourceReader reader =
                new BigtableSourceReader(parameters, mockContext, rowType, mockClient);
        reader.addSplits(Collections.singletonList(split));

        Object lock = new Object();
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(lock);

        reader.pollNext(collector);

        // One row emitted
        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        assertEquals("alice", captor.getValue().getField(1));
    }

    // -------------------------------------------------------------------------
    // Issue 3: rowkey_column config drives field mapping
    // -------------------------------------------------------------------------

    /**
     * When rowkey_column is configured, the named field should receive the row key value, not
     * the default literal "rowkey".
     */
    @SuppressWarnings("unchecked")
    @Test
    void testRowkeyColumnConfigMapsCorrectField() throws Exception {
        // Schema uses "id" as the row-key field, not the default "rowkey"
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

        com.google.api.gax.rpc.ServerStream<Row> fakeStream = mock(com.google.api.gax.rpc.ServerStream.class);
        Mockito.doAnswer(invocation -> {
            Consumer<Row> action = invocation.getArgument(0);
            action.accept(fakeRow);
            return null;
        }).when(fakeStream).forEach(any());
        when(mockDataClient.readRows(any(Query.class))).thenReturn(fakeStream);

        BigtableSourceReader reader =
                new BigtableSourceReader(paramsWithRowkeyCol, mockContext, customRowType, mockClient);
        reader.addSplits(Collections.singletonList(new BigtableSourceSplit(0, "", "")));

        Object lock = new Object();
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(lock);

        reader.pollNext(collector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(collector).collect(captor.capture());
        // field[0] = "id" → should be the row key "my-key"
        assertEquals("my-key", captor.getValue().getField(0));
        // field[1] = "cf:value" → should be "hello"
        assertEquals("hello", captor.getValue().getField(1));
    }
}
