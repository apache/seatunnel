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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StarRocksSourceSplitEnumeratorTest {

    @Test
    public void shouldBalanceSplitsEvenlyAcrossReaders() throws Exception {
        Map<Integer, List<StarRocksSourceSplit>> assignments = new HashMap<>();
        SourceSplitEnumerator.Context<StarRocksSourceSplit> context = createContext(4, assignments);
        StartRocksSourceSplitEnumerator enumerator =
                new StartRocksSourceSplitEnumerator(
                        context, new SourceConfig(ReadonlyConfig.fromMap(configMap())));

        Method addPendingSplit =
                StartRocksSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", java.util.Collection.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, buildSplits(10));

        StarRocksSourceState state = enumerator.snapshotState(1L);
        Map<Integer, List<StarRocksSourceSplit>> pendingSplits = state.getPendingSplit();

        Assertions.assertEquals(4, pendingSplits.size());
        Assertions.assertEquals(3, pendingSplits.get(0).size());
        Assertions.assertEquals(3, pendingSplits.get(1).size());
        Assertions.assertEquals(2, pendingSplits.get(2).size());
        Assertions.assertEquals(2, pendingSplits.get(3).size());
    }

    @Test
    public void shouldBalanceSingleSplitTablesAcrossReadersInRun() {
        Map<Integer, List<StarRocksSourceSplit>> assignments = new HashMap<>();
        SourceSplitEnumerator.Context<StarRocksSourceSplit> context = createContext(3, assignments);
        StartRocksSourceSplitEnumerator enumerator =
                spy(
                        new StartRocksSourceSplitEnumerator(
                                context,
                                new SourceConfig(ReadonlyConfig.fromMap(configMap())),
                                new StarRocksSourceState(
                                        new HashMap<>(),
                                        new ConcurrentLinkedQueue<>(
                                                new ArrayList<String>() {
                                                    {
                                                        add("table_0");
                                                        add("table_1");
                                                        add("table_2");
                                                    }
                                                }),
                                        0)));

        doReturn(Collections.singletonList(buildSplit("split-0")))
                .when(enumerator)
                .getStarRocksSourceSplit("table_0");
        doReturn(Collections.singletonList(buildSplit("split-1")))
                .when(enumerator)
                .getStarRocksSourceSplit("table_1");
        doReturn(Collections.singletonList(buildSplit("split-2")))
                .when(enumerator)
                .getStarRocksSourceSplit("table_2");

        enumerator.run();

        Assertions.assertEquals(1, getAssignedSplitCount(assignments, 0));
        Assertions.assertEquals(1, getAssignedSplitCount(assignments, 1));
        Assertions.assertEquals(1, getAssignedSplitCount(assignments, 2));
    }

    @Test
    public void shouldReassignReturnedSplitsToOriginalReader() {
        Map<Integer, List<StarRocksSourceSplit>> assignments = new HashMap<>();
        SourceSplitEnumerator.Context<StarRocksSourceSplit> context = createContext(3, assignments);
        StartRocksSourceSplitEnumerator enumerator =
                new StartRocksSourceSplitEnumerator(
                        context, new SourceConfig(ReadonlyConfig.fromMap(configMap())));

        enumerator.addSplitsBack(buildSplits(3), 2);

        Assertions.assertEquals(0, getAssignedSplitCount(assignments, 0));
        Assertions.assertEquals(0, getAssignedSplitCount(assignments, 1));
        Assertions.assertEquals(3, getAssignedSplitCount(assignments, 2));

        StarRocksSourceState state = enumerator.snapshotState(1L);
        Assertions.assertTrue(state.getPendingSplit().isEmpty());
    }

    @Test
    public void shouldContinueRoundRobinAfterRestore() {
        Map<Integer, List<StarRocksSourceSplit>> assignments = new HashMap<>();
        SourceSplitEnumerator.Context<StarRocksSourceSplit> context = createContext(3, assignments);
        String remainingTable = "table_after_restore";
        StartRocksSourceSplitEnumerator enumerator =
                spy(
                        new StartRocksSourceSplitEnumerator(
                                context,
                                new SourceConfig(ReadonlyConfig.fromMap(configMap())),
                                new StarRocksSourceState(
                                        new HashMap<>(),
                                        new ConcurrentLinkedQueue<>(
                                                Collections.singletonList(remainingTable)),
                                        1)));

        doReturn(Collections.singletonList(buildSplit("split-after-restore")))
                .when(enumerator)
                .getStarRocksSourceSplit(remainingTable);

        enumerator.run();

        Assertions.assertEquals(0, getAssignedSplitCount(assignments, 0));
        Assertions.assertEquals(1, getAssignedSplitCount(assignments, 1));
        Assertions.assertEquals(0, getAssignedSplitCount(assignments, 2));
    }

    private Map<String, Object> configMap() {
        Map<String, Object> config = new HashMap<>();
        config.put("nodeUrls", Collections.singletonList("localhost:8030"));
        config.put("username", "root");
        config.put("password", "");
        config.put("database", "test_db");
        config.put("table", "test_table");
        config.put("table_list", Collections.emptyList());
        return config;
    }

    private List<StarRocksSourceSplit> buildSplits(int size) {
        List<StarRocksSourceSplit> splits = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            splits.add(buildSplit("split-" + i));
        }
        return splits;
    }

    private StarRocksSourceSplit buildSplit(String splitId) {
        return new StarRocksSourceSplit(null, splitId);
    }

    private SourceSplitEnumerator.Context<StarRocksSourceSplit> createContext(
            int parallelism, Map<Integer, List<StarRocksSourceSplit>> assignments) {
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<StarRocksSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(parallelism);
        when(context.registeredReaders()).thenReturn(createReaders(parallelism));
        doAnswer(
                        invocation -> {
                            int subtaskId = invocation.getArgument(0);
                            List<StarRocksSourceSplit> splits = invocation.getArgument(1);
                            assignments
                                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                                    .addAll(splits);
                            return null;
                        })
                .when(context)
                .assignSplit(anyInt(), anyList());
        return context;
    }

    private Set<Integer> createReaders(int parallelism) {
        Set<Integer> readers = new java.util.LinkedHashSet<>();
        for (int i = 0; i < parallelism; i++) {
            readers.add(i);
        }
        return readers;
    }

    private int getAssignedSplitCount(
            Map<Integer, List<StarRocksSourceSplit>> assignments, int subtaskId) {
        return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
    }
}
