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
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class JdbcSourceSplitEnumeratorTest {

    @Test
    void testRunSignalsNoMoreSplitsOnce() throws Exception {
        int parallelism = 1;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        List<Integer> assignTargets = new ArrayList<>();
        Set<Integer> noMoreSplitsReaders = new HashSet<>();

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
                        if (!splits.isEmpty()) {
                            assignTargets.add(subtaskId);
                        }
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {
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
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .build();

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, null);

        enumerator.open();
        enumerator.run();

        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Assertions.assertEquals(Collections.singleton(0), noMoreSplitsReaders);
        Assertions.assertEquals(Collections.singleton(0), new HashSet<>(assignTargets));
    }

    @Test
    void testAddSplitsBackAndRegisterReaderSignalNoMoreSplits() throws Exception {
        int parallelism = 1;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        Set<Integer> registeredReaders = new HashSet<>();
        List<JdbcSourceSplit> assignedSplits = new ArrayList<>();
        Set<Integer> noMoreSplitsReaders = new HashSet<>();

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
                        assignedSplits.addAll(splits);
                    }

                    @Override
                    public void signalNoMoreSplits(int subtask) {
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
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .build();

        // Simulate that initial enumeration has been completed in a previous attempt and there are
        // no remaining pending tables or splits in the enumerator state. The split to be
        // reprocessed will be added via addSplitsBack, as happens in a real failover.
        List<TablePath> pendingTables = new ArrayList<>();
        Map<Integer, List<JdbcSourceSplit>> pendingSplits = new HashMap<>();
        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-0", null, null, null, null, null);
        JdbcSourceState state = new JdbcSourceState(pendingTables, pendingSplits);

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, state);

        enumerator.open();

        // Simulate failover recovery: add splits back from a failed reader before any reader
        // registers.
        List<JdbcSourceSplit> splitsBack = new ArrayList<>();
        splitsBack.add(split);
        enumerator.addSplitsBack(splitsBack, 0);

        Assertions.assertTrue(assignedSplits.isEmpty());
        Assertions.assertTrue(noMoreSplitsReaders.isEmpty());

        // Now a new reader 0 registers, it should receive the split and then NoMoreSplitsEvent
        registeredReaders.add(0);
        enumerator.registerReader(0);

        Assertions.assertEquals(Collections.singletonList(split), assignedSplits);
        Assertions.assertEquals(Collections.singleton(0), noMoreSplitsReaders);
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testNoMoreSplitsWhenOnlyUnregisteredReadersHavePendingSplits() throws Exception {
        int parallelism = 3;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        // Only subtask 0 is considered registered from the enumerator's perspective.
        Set<Integer> registeredReaders = new HashSet<>(Collections.singleton(0));
        Set<Integer> noMoreSplitsReaders = new HashSet<>();

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
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {}

                    @Override
                    public void signalNoMoreSplits(int subtask) {
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
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .build();

        // Simulate a failover-recovery-like state where there are no pending tables
        // and a split needs to be re-added for an unregistered reader (subtask 1).
        JdbcSourceState state =
                new JdbcSourceState(
                        new ArrayList<>(), new HashMap<Integer, List<JdbcSourceSplit>>());

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, state);

        enumerator.open();

        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-1", null, null, null, null, null);

        // Add a split back for subtask 1, which is not in registeredReaders.
        enumerator.addSplitsBack(Collections.singletonList(split), 1);

        // Since there are no pending splits for the only registered reader (0),
        // the enumerator should still signal NoMoreSplitsEvent to reader 0.
        Assertions.assertEquals(Collections.singleton(0), noMoreSplitsReaders);
        // currentUnassignedSplitSize still reflects that there are unassigned splits
        // in the enumerator (for unregistered readers), so it is expected to be 1.
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testLateRegisteredReaderReceivesNoMoreSplits() throws Exception {
        int parallelism = 2;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        Set<Integer> registeredReaders = new HashSet<>(Collections.singleton(0));
        Set<Integer> noMoreSplitsReaders = new HashSet<>();

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
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {}

                    @Override
                    public void signalNoMoreSplits(int subtask) {
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
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .build();

        JdbcSourceState state =
                new JdbcSourceState(
                        new ArrayList<>(), new HashMap<Integer, List<JdbcSourceSplit>>());

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, state);

        enumerator.open();
        enumerator.run();

        Assertions.assertEquals(Collections.singleton(0), noMoreSplitsReaders);

        registeredReaders.add(1);
        enumerator.registerReader(1);

        Set<Integer> expected = new HashSet<>();
        expected.add(0);
        expected.add(1);
        Assertions.assertEquals(expected, noMoreSplitsReaders);
    }

    @Test
    void testNoMoreSplitsSignalIsSentAtMostOnce() throws Exception {
        int parallelism = 1;
        TablePath tablePath = TablePath.of("db", "schema", "table");

        Map<TablePath, JdbcSourceTable> tables = new HashMap<>();
        tables.put(tablePath, createJdbcSourceTable(tablePath));

        Set<Integer> registeredReaders = new HashSet<>(Collections.singleton(0));
        AtomicBoolean noMoreSplitsCalled = new AtomicBoolean(false);

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
                    public void assignSplit(int subtaskId, List<JdbcSourceSplit> splits) {}

                    @Override
                    public void signalNoMoreSplits(int subtask) {
                        if (!noMoreSplitsCalled.compareAndSet(false, true)) {
                            Assertions.fail("NoMoreSplitsEvent should be sent at most once.");
                        }
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
                                        .url("jdbc:mysql://localhost:3306/test")
                                        .driverName("com.mysql.cj.jdbc.Driver")
                                        .build())
                        .build();

        JdbcSourceSplitEnumerator enumerator =
                new JdbcSourceSplitEnumerator(context, sourceConfig, tables, null);

        enumerator.open();
        enumerator.run();

        // maybeSignalNoMoreSplits is invoked from run(), addSplitsBack and registerReader.
        // Here we call addSplitsBack / registerReader again, and ensure that noMoreSplits
        // is not sent multiple times.
        enumerator.addSplitsBack(Collections.emptyList(), 0);
        enumerator.registerReader(0);

        Assertions.assertTrue(noMoreSplitsCalled.get());
    }

    private JdbcSourceTable createJdbcSourceTable(TablePath tablePath) {
        TableIdentifier tableId = TableIdentifier.of("default", tablePath);
        TableSchema tableSchema = TableSchema.builder().columns(Collections.emptyList()).build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, Collections.emptyMap(), Collections.emptyList(), "");
        return JdbcSourceTable.builder().tablePath(tablePath).catalogTable(catalogTable).build();
    }
}
