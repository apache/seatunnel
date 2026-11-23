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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkDataPartitioner;
import org.apache.seatunnel.api.sink.SupportSinkDataPartition;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class MultiTableWriteRoutingTest {

    @Test
    void shouldDelegateRoutingBySourceTable() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("target-first", 0));
        sinks.put(TablePath.of("database", "second"), sink("target-second", 1));
        SinkDataPartitioner<SeaTunnelRow> routing =
                SupportSinkDataPartition.resolve(multiTable(sinks, 1), 2).get();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("database.first");
        assertEquals(0, routing.select(row));
        row.setTableId("database.second");
        assertEquals(1, routing.select(row));
        row.setTableId("database.missing");
        assertThrows(IllegalArgumentException.class, () -> routing.select(row));
    }

    @Test
    void shouldFallbackToSingleRouteForMissingTableIds() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "only"), sink("target-only", 1));
        SinkDataPartitioner<SeaTunnelRow> routing =
                SupportSinkDataPartition.resolve(multiTable(sinks, 1), 2).get();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});

        row.setTableId("database.only");
        assertEquals(1, routing.select(row));

        row.setTableId("");
        assertEquals(1, routing.select(row));
        assertEquals("", row.getTableId());

        row.setTableId(null);
        assertEquals(1, routing.select(row));
        assertNull(row.getTableId());

        row.setTableId("database.unknown");
        assertEquals(1, routing.select(row));
        assertEquals("database.unknown", row.getTableId());
    }

    @Test
    void shouldKeepSkippedTableHandlingAndRejectUnsafeSingleRouteFallback() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "skipped"), sink("target-skipped", 0));
        sinks.put(TablePath.of("database", "active"), sink("target-active", 1));
        MultiTableFailedTable skippedTable =
                MultiTableFailureHelper.buildFailedTable(
                        "database.skipped",
                        MultiTableFailurePhase.SINK_INIT,
                        "paimon",
                        new RuntimeException("table initialization failed"));
        SinkDataPartitioner<SeaTunnelRow> routing =
                SupportSinkDataPartition.resolve(multiTable(sinks, 1, skippedTable), 2).get();

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("database.active");
        assertEquals(1, routing.select(row));

        row.setTableId("database.skipped");
        assertEquals(0, routing.select(row));

        row.setTableId("");
        assertThrows(IllegalArgumentException.class, () -> routing.select(row));
        row.setTableId(null);
        assertThrows(IllegalArgumentException.class, () -> routing.select(row));
        row.setTableId("database.unknown");
        assertThrows(IllegalArgumentException.class, () -> routing.select(row));
    }

    @Test
    void shouldRejectReplicaAndDuplicatePhysicalTarget() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(TablePath.of("database", "first"), sink("same-target", 0));
        assertThrows(
                UnsupportedOperationException.class,
                () -> SupportSinkDataPartition.resolve(multiTable(sinks, 2), 2));
        sinks.put(TablePath.of("database", "second"), sink("same-target", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> SupportSinkDataPartition.resolve(multiTable(sinks, 1), 2));
    }

    @Test
    void shouldKeepUnroutedSinksAndRejectMixedRouting() {
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        SeaTunnelSink plain = mock(SeaTunnelSink.class);
        sinks.put(TablePath.of("database", "first"), plain);
        assertTrue(!SupportSinkDataPartition.resolve(multiTable(sinks, 2), 2).isPresent());
        sinks.put(TablePath.of("database", "second"), sink("target", 0));
        UnsupportedOperationException failure =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> SupportSinkDataPartition.resolve(multiTable(sinks, 1), 2));
        assertTrue(failure.getMessage().contains("Cannot mix routed and unrouted sinks"));
    }

    private MultiTableSink multiTable(Map<TablePath, SeaTunnelSink> sinks, int replicas) {
        return multiTable(sinks, replicas, null);
    }

    private MultiTableSink multiTable(
            Map<TablePath, SeaTunnelSink> sinks, int replicas, MultiTableFailedTable failedTable) {
        Map<String, Object> options = new HashMap<>();
        options.put("multi_table_sink_replica", replicas);
        if (failedTable != null) {
            options.put(
                    MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(),
                    "CONTINUE_OTHER_TABLES");
        }
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        if (failedTable != null) {
            config =
                    MultiTableFailureHelper.withFailedTables(
                            config, Collections.singletonList(failedTable));
        }
        return new MultiTableSink(
                new MultiTableFactoryContext(config, getClass().getClassLoader(), sinks));
    }

    private SeaTunnelSink sink(String target, int owner) {
        SeaTunnelSink sink =
                mock(
                        SeaTunnelSink.class,
                        withSettings().extraInterfaces(SupportSinkDataPartition.class));
        SinkDataPartitioner<SeaTunnelRow> routing =
                new SinkDataPartitioner<SeaTunnelRow>() {
                    @Override
                    public int select(SeaTunnelRow row) {
                        return owner;
                    }

                    @Override
                    public Optional<String> targetIdentifier() {
                        return Optional.of(target);
                    }
                };
        when(((SupportSinkDataPartition<SeaTunnelRow>) sink).getSinkDataPartitioner(anyInt()))
                .thenReturn(Optional.of(routing));
        return sink;
    }
}
