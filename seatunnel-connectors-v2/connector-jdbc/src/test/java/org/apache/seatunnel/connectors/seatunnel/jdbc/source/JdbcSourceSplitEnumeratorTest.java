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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class JdbcSourceSplitEnumeratorTest {

    @Test
    void testRunSignalsNoMoreSplitsOnce() throws Exception {
        int parallelism = 1;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        List<Integer> assignTargets = new ArrayList<>();
        Set<Integer> noMoreSplitsReaders = new HashSet<>();
        AtomicInteger noMoreSplitsCallCount = new AtomicInteger();

        SourceSplitEnumerator.Context<JdbcSourceSplit> context =
                new SourceSplitEnumerator.Context<JdbcSourceSplit>() {
                    @Override
                    public int currentParallelism() {
                        return parallelism;
                    }

                    @Override
                    public Set<Integer> registeredReaders() {
                        return Collections.singleton(0);
                    }

                    @Override
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {
                        assignTargets.add(subtaskId);
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {
                        noMoreSplitsCallCount.incrementAndGet();
                        noMoreSplitsReaders.add(subtask);
                    }

                    @Override
                    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        JdbcSourceConfig sourceConfig =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build();

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, null);

        enumerator.open();
        enumerator.run();

        Assertions.assertEquals(Collections.singletonList(0), assignTargets);
        Assertions.assertEquals(Collections.singleton(0), noMoreSplitsReaders);
        Assertions.assertEquals(1, noMoreSplitsCallCount.get());

        // NoMoreSplitsEvent is only sent once at the end of run().
        enumerator.addSplitsBack(Collections.emptyList(), 0);
        enumerator.registerReader(0);

        Assertions.assertEquals(1, noMoreSplitsCallCount.get());
    }

    @Test
    void testRunSignalsNoMoreSplitsForAllRegisteredReadersWithHighParallelism() throws Exception {
        int parallelism = 8;

        Set<Integer> registeredReaders = new HashSet<>();
        for (int i = 0; i < parallelism; i++) {
            registeredReaders.add(i);
        }

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            TablePath tablePath = TablePath.of("db", "schema", "table_" + i);
            tables.put(tablePath, createJdbcSourceTable(tablePath));
        }

        Map<String, Integer> assignedSplitOwners = new HashMap<>();
        Set<Integer> noMoreSplitsReaders = ConcurrentHashMap.newKeySet();
        AtomicInteger noMoreSplitsCallCount = new AtomicInteger();

        SourceSplitEnumerator.Context<JdbcSourceSplit> context =
                new SourceSplitEnumerator.Context<JdbcSourceSplit>() {
                    @Override
                    public int currentParallelism() {
                        return parallelism;
                    }

                    @Override
                    public Set<Integer> registeredReaders() {
                        return new HashSet<>(registeredReaders);
                    }

                    @Override
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {
                        for (JdbcSourceSplit split : splits) {
                            assignedSplitOwners.put(split.splitId(), subtaskId);
                        }
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {
                        noMoreSplitsCallCount.incrementAndGet();
                        noMoreSplitsReaders.add(subtask);
                    }

                    @Override
                    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        JdbcSourceConfig sourceConfig =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build();

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, null);

        enumerator.open();
        enumerator.run();

        Assertions.assertEquals(tables.size(), assignedSplitOwners.size());
        assignedSplitOwners.forEach(
                (splitId, owner) -> {
                    int expectedOwner = (splitId.hashCode() & Integer.MAX_VALUE) % parallelism;
                    Assertions.assertEquals(expectedOwner, owner);
                });

        Assertions.assertEquals(registeredReaders, noMoreSplitsReaders);
        Assertions.assertEquals(parallelism, noMoreSplitsCallCount.get());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testRegisterReaderReassignsReturnedSplitsWithoutResignaling() throws Exception {
        int parallelism = 1;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        Set<Integer> registeredReaders = new HashSet<>(Collections.singleton(0));
        List<List<String>> assignedSplitBatches = new ArrayList<>();
        AtomicInteger noMoreSplitsCallCount = new AtomicInteger();

        SourceSplitEnumerator.Context<JdbcSourceSplit> context =
                new SourceSplitEnumerator.Context<JdbcSourceSplit>() {
                    @Override
                    public int currentParallelism() {
                        return parallelism;
                    }

                    @Override
                    public Set<Integer> registeredReaders() {
                        return new HashSet<>(registeredReaders);
                    }

                    @Override
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {
                        assignedSplitBatches.add(
                                splits.stream()
                                        .map(JdbcSourceSplit::splitId)
                                        .collect(java.util.stream.Collectors.toList()));
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {
                        noMoreSplitsCallCount.incrementAndGet();
                    }

                    @Override
                    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

                    @Override
                    public MetricsContext getMetricsContext() {
                        return null;
                    }

                    @Override
                    public EventListener getEventListener() {
                        return null;
                    }
                };

        JdbcSourceConfig sourceConfig =
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build();

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, null);

        enumerator.open();
        enumerator.run();

        registeredReaders.clear();

        JdbcSourceSplit returnedSplit = createSplit(tablePath, "returned-split");
        enumerator.addSplitsBack(Collections.singletonList(returnedSplit), 0);

        registeredReaders.add(0);
        enumerator.registerReader(0);

        Assertions.assertEquals(2, assignedSplitBatches.size());
        Assertions.assertEquals(
                Collections.singletonList("returned-split"), assignedSplitBatches.get(1));
        Assertions.assertEquals(1, noMoreSplitsCallCount.get());
    }

    private JdbcSourceTable createJdbcSourceTable(TablePath tablePath) {
        TableIdentifier tableId = TableIdentifier.of("default", tablePath);
        TableSchema tableSchema = TableSchema.builder().columns(Collections.emptyList()).build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, Collections.emptyMap(), Collections.emptyList(), "");
        return JdbcSourceTable.builder().tablePath(tablePath).catalogTable(catalogTable).build();
    }

    private JdbcSourceSplit createSplit(TablePath tablePath, String splitId) {
        return new JdbcSourceSplit(tablePath, splitId, "SELECT 1", "id", null, 0, 1);
    }
}
