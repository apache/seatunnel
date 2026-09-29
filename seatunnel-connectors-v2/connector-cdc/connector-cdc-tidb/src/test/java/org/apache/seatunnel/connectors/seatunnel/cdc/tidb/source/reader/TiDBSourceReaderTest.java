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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import org.junit.jupiter.api.Test;
import org.tikv.cdc.CDCClient;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TiDBSourceReaderTest {

    private static final long TABLE_ID = 42L;
    private static final long ROW_HANDLE = 7L;
    private static final long START_TS = 100L;
    private static final long COMMIT_TS = 200L;
    private static final long RESOLVED_TS = 300L;

    @Test
    void shouldAdvanceSplitWithTheSlowestRegionResolvedTimestamp() throws Exception {
        TiDBSourceConfig config =
                TiDBSourceConfig.builder().startupMode(StartupMode.LATEST).batchSize(1).build();
        TiDBSourceReader reader =
                new TiDBSourceReader(
                        mock(SourceReader.Context.class), config, mock(CatalogTable.class));
        TiDBSourceSplit split =
                new TiDBSourceSplit(
                        "database", "table", mock(Coprocessor.KeyRange.class), 10L, null, true);
        CDCClient cdcClient = mock(CDCClient.class);
        when(cdcClient.get()).thenReturn(null);
        when(cdcClient.getMinResolvedTs()).thenReturn(100L);
        when(cdcClient.getMaxResolvedTs()).thenReturn(200L);
        cdcClients(reader).put(split, cdcClient);

        reader.captureStreamingEvents(split, mock(Collector.class));

        assertEquals(100L, split.getResolvedTs());
    }

    @Test
    void flushRowsShouldHoldCommitUntilMatchingPrewriteArrives() throws Exception {
        TiDBSourceReader reader = new TiDBSourceReader(null, null, null);
        Cdcpb.Event.Row commit = row(Cdcpb.Event.LogType.COMMIT, START_TS, COMMIT_TS);

        assertTrue(handleRow(reader, commit));

        long safeResolvedTs = flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS);

        assertEquals(COMMIT_TS - 1, safeResolvedTs);
        assertEquals(1, commits(reader).size());
        assertTrue(committedEvents(reader).isEmpty());

        Cdcpb.Event.Row prewrite = row(Cdcpb.Event.LogType.PREWRITE, START_TS, 0L);

        assertTrue(handleRow(reader, prewrite));

        safeResolvedTs = flushRowsAndGetSafeResolvedTs(reader, RESOLVED_TS);

        assertEquals(RESOLVED_TS, safeResolvedTs);
        assertTrue(commits(reader).isEmpty());
        assertTrue(preWrites(reader).isEmpty());
        BlockingQueue<Cdcpb.Event.Row> committedEvents = committedEvents(reader);
        assertFalse(committedEvents.isEmpty());
        assertSame(prewrite, committedEvents.poll());
        assertTrue(committedEvents.isEmpty());
    }

    private static Cdcpb.Event.Row row(Cdcpb.Event.LogType type, long startTs, long commitTs) {
        return Cdcpb.Event.Row.newBuilder()
                .setType(type)
                .setStartTs(startTs)
                .setCommitTs(commitTs)
                .setKey(RowKey.toRowKey(TABLE_ID, ROW_HANDLE).toByteString())
                .build();
    }

    private static boolean handleRow(TiDBSourceReader reader, Cdcpb.Event.Row row)
            throws Exception {
        Method handleRow =
                TiDBSourceReader.class.getDeclaredMethod("handleRow", Cdcpb.Event.Row.class);
        handleRow.setAccessible(true);
        return (Boolean) handleRow.invoke(reader, row);
    }

    private static long flushRowsAndGetSafeResolvedTs(TiDBSourceReader reader, long resolvedTs)
            throws Exception {
        Method flushRowsAndGetSafeResolvedTs =
                TiDBSourceReader.class.getDeclaredMethod(
                        "flushRowsAndGetSafeResolvedTs", long.class);
        flushRowsAndGetSafeResolvedTs.setAccessible(true);
        return (Long) flushRowsAndGetSafeResolvedTs.invoke(reader, resolvedTs);
    }

    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits(TiDBSourceReader reader)
            throws Exception {
        return rowBuffer(reader, "commits");
    }

    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> preWrites(TiDBSourceReader reader)
            throws Exception {
        return rowBuffer(reader, "preWrites");
    }

    @SuppressWarnings("unchecked")
    private static TreeMap<RowKeyWithTs, Cdcpb.Event.Row> rowBuffer(
            TiDBSourceReader reader, String fieldName) throws Exception {
        Field field = TiDBSourceReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (TreeMap<RowKeyWithTs, Cdcpb.Event.Row>) field.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static BlockingQueue<Cdcpb.Event.Row> committedEvents(TiDBSourceReader reader)
            throws Exception {
        Field field = TiDBSourceReader.class.getDeclaredField("committedEvents");
        field.setAccessible(true);
        return (BlockingQueue<Cdcpb.Event.Row>) field.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static Map<TiDBSourceSplit, CDCClient> cdcClients(TiDBSourceReader reader)
            throws ReflectiveOperationException {
        Field cacheField = TiDBSourceReader.class.getDeclaredField("cacheCDCClient");
        cacheField.setAccessible(true);
        return (Map<TiDBSourceSplit, CDCClient>) cacheField.get(reader);
    }
}
