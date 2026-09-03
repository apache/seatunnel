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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.structure.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeGraphSourceSplitEnumeratorTest {

    @Test
    void parallelismOneProducesSingleLabelListSplit() {
        CapturingContext context = new CapturingContext(1);
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(
                        context, config(), 1024L, null, failingClientFactory());

        enumerator.open();
        enumerator.run();

        List<HugeGraphSourceSplit> assigned = context.assignedTo(0);
        assertEquals(1, assigned.size());
        assertFalse(assigned.get(0).isShardMode());
        assertEquals("label-list", assigned.get(0).splitId());
        assertTrue(context.noMoreSplits.contains(0));
    }

    @Test
    void readAllProducesOneLabelListSplitPerLabel() {
        // Read-all mode ignores parallelism-based sharding: one label-list split per discovered
        // label, each carrying its label, distributed across readers. The client is never touched
        // (labels come from the config), so a failing client factory must not be invoked.
        CapturingContext context = new CapturingContext(2);
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(
                        context,
                        readAllConfig("person", "software"),
                        1024L,
                        null,
                        failingClientFactory());

        enumerator.open();
        enumerator.run();

        List<HugeGraphSourceSplit> combined = new ArrayList<>();
        combined.addAll(context.assignedTo(0));
        combined.addAll(context.assignedTo(1));
        assertEquals(2, combined.size());
        Set<String> labels = new HashSet<>();
        for (HugeGraphSourceSplit split : combined) {
            assertFalse(split.isShardMode());
            labels.add(split.getLabel());
        }
        assertEquals(new HashSet<>(Arrays.asList("person", "software")), labels);
        assertTrue(context.noMoreSplits.contains(0));
        assertTrue(context.noMoreSplits.contains(1));
    }

    @Test
    void parallelismGreaterThanOneSplitsByShardRoundRobin() {
        CapturingContext context = new CapturingContext(2);
        FakeClient client = new FakeClient();
        client.vertexShards =
                Arrays.asList(
                        new Shard("0", "3", 0L), new Shard("3", "6", 0L), new Shard("6", "9", 0L));
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(context, config(), 1024L, null, () -> client);

        enumerator.open();
        enumerator.run();

        // 3 shards over 2 readers, round-robin: reader0 -> shard-0, shard-2; reader1 -> shard-1
        assertEquals(2, context.assignedTo(0).size());
        assertEquals(1, context.assignedTo(1).size());
        assertTrue(context.assignedTo(0).get(0).isShardMode());
        assertTrue(context.noMoreSplits.contains(0));
        assertTrue(context.noMoreSplits.contains(1));
        assertTrue(client.closed, "discovery client must be closed");
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void shardDiscoveryFailureSuggestsParallelismOne() {
        // Memory backend rejects vertexShards; the raw error is unactionable, so the enumerator
        // must wrap it with the parallelism=1 label-list guidance (and still close the client).
        CapturingContext context = new CapturingContext(2);
        FakeClient client = new FakeClient();
        client.shardFailure = new RuntimeException("Not support shard for memory backend");
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(context, config(), 1024L, null, () -> client);

        HugeGraphConnectorException ex =
                assertThrows(HugeGraphConnectorException.class, enumerator::open);
        assertTrue(ex.getMessage().contains("parallelism=1"));
        assertTrue(client.closed, "discovery client must be closed even on failure");
    }

    @Test
    void restoreDoesNotRediscoverAndAssignsOnlyUnassigned() {
        // Two shard splits discovered previously; shard-0 already assigned (lives in a reader),
        // shard-1 still unassigned. On restore the enumerator must assign only shard-1 and never
        // touch the discovery client.
        HugeGraphSourceSplit shard0 =
                HugeGraphSourceSplit.shardSplit("shard-0", new Shard("0", "5", 0L));
        HugeGraphSourceSplit shard1 =
                HugeGraphSourceSplit.shardSplit("shard-1", new Shard("5", "9", 0L));
        Set<HugeGraphSourceSplit> all = new LinkedHashSet<>(Arrays.asList(shard0, shard1));
        Set<HugeGraphSourceSplit> assigned = new HashSet<>(Arrays.asList(shard0));
        HugeGraphSourceState state = new HugeGraphSourceState(all, assigned);

        CapturingContext context = new CapturingContext(2);
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(
                        context, config(), 1024L, state, failingClientFactory());

        enumerator.open();
        enumerator.run();

        List<HugeGraphSourceSplit> all0 = context.assignedTo(0);
        List<HugeGraphSourceSplit> all1 = context.assignedTo(1);
        List<HugeGraphSourceSplit> combined = new ArrayList<>();
        combined.addAll(all0);
        combined.addAll(all1);
        assertEquals(1, combined.size(), "only the unassigned shard-1 should be re-assigned");
        assertEquals("shard-1", combined.get(0).splitId());
    }

    @Test
    void snapshotStatePersistsAllAndAssigned() {
        CapturingContext context = new CapturingContext(1);
        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(
                        context, config(), 1024L, null, failingClientFactory());

        enumerator.open();
        enumerator.run();
        HugeGraphSourceState state = enumerator.snapshotState(1L);

        assertEquals(1, state.getAllSplits().size());
        assertEquals(1, state.getAssignedSplits().size());
    }

    private static Supplier<HugeGraphOperations> failingClientFactory() {
        return () -> {
            throw new AssertionError("discovery client must not be created in this path");
        };
    }

    @Test
    void filterWithRuntimeParallelismGreaterThanOneFailsFast() {
        // The factory-level checkFilterParallelism() reads only the per-source 'parallelism'
        // option. The real runtime parallelism comes from env { parallelism = N } and is only
        // visible to the enumerator via context.currentParallelism(). This test pins the
        // runtime guard: when the enumerator sees parallelism > 1 AND a filter is configured,
        // it must throw before creating any shard splits — otherwise shard scans silently
        // ignore the filter.
        HugeGraphSourceConfig filterConfig = configWithFilter();
        CapturingContext context = new CapturingContext(2); // runtime parallelism=2

        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(context, filterConfig, 1024L, null, () -> null);

        HugeGraphConnectorException ex =
                assertThrows(HugeGraphConnectorException.class, enumerator::open);
        assertTrue(
                ex.getMessage().contains("filter"),
                "Error must mention 'filter': " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("parallelism"),
                "Error must mention 'parallelism': " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("2"),
                "Error must include the actual runtime parallelism: " + ex.getMessage());
    }

    @Test
    void filterWithRuntimeParallelismOneIsAllowed() {
        // Runtime parallelism 1 + filter is the supported label-list path with server-side
        // filtering. The enumerator must NOT throw.
        HugeGraphSourceConfig filterConfig = configWithFilter();
        CapturingContext context = new CapturingContext(1);

        HugeGraphSourceSplitEnumerator enumerator =
                new HugeGraphSourceSplitEnumerator(
                        context, filterConfig, 1024L, null, () -> new FakeClient());

        // Must not throw — filter + parallelism=1 is valid.
        enumerator.open();
        enumerator.run();

        List<HugeGraphSourceSplit> assigned = context.assignedTo(0);
        assertEquals(1, assigned.size());
        assertFalse(assigned.get(0).isShardMode(), "parallelism=1 should create label-list split");
    }

    private HugeGraphSourceConfig configWithFilter() {
        HugeGraphSourceConfig config = new HugeGraphSourceConfig();
        config.setLabel("person");
        config.setLabelType(MappingConfig.LabelType.VERTEX);
        config.setSchema(
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE}));
        config.setPageSize(100);
        config.setSplitSize(1024L);
        config.setFilter(Collections.singletonMap("status", "active"));
        return config;
    }

    private HugeGraphSourceConfig config() {
        HugeGraphSourceConfig config = new HugeGraphSourceConfig();
        config.setLabel("person");
        config.setLabelType(MappingConfig.LabelType.VERTEX);
        config.setSchema(
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE}));
        config.setPageSize(100);
        config.setSplitSize(1024L);
        return config;
    }

    private HugeGraphSourceConfig readAllConfig(String... labels) {
        HugeGraphSourceConfig config = new HugeGraphSourceConfig();
        config.setReadAllLabels(true);
        config.setLabelType(MappingConfig.LabelType.VERTEX);
        config.setLabels(Arrays.asList(labels));
        config.setPageSize(100);
        config.setSplitSize(1024L);
        return config;
    }

    private static class CapturingContext
            implements SourceSplitEnumerator.Context<HugeGraphSourceSplit> {
        private final int parallelism;
        private final Map<Integer, List<HugeGraphSourceSplit>> assignments = new HashMap<>();
        private final Set<Integer> noMoreSplits = new HashSet<>();

        private CapturingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        private List<HugeGraphSourceSplit> assignedTo(int subtask) {
            return assignments.getOrDefault(subtask, new ArrayList<>());
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return new HashSet<>();
        }

        @Override
        public void assignSplit(int subtaskId, List<HugeGraphSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplits.add(subtask);
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
    }

    private static class FakeClient implements HugeGraphOperations {
        private List<Shard> vertexShards = new ArrayList<>();
        private List<Shard> edgeShards = new ArrayList<>();
        private RuntimeException shardFailure;
        private boolean closed;

        @Override
        public Set<String> getVertexLabelPropertiesOrNull(String label) {
            return null;
        }

        @Override
        public Set<String> getEdgeLabelPropertiesOrNull(String label) {
            return null;
        }

        @Override
        public List<String> listVertexLabels() {
            return Collections.emptyList();
        }

        @Override
        public List<String> listEdgeLabels() {
            return Collections.emptyList();
        }

        @Override
        public DataType getPropertyDataType(String propertyName) {
            return null;
        }

        @Override
        public Cardinality getPropertyCardinality(String propertyName) {
            return Cardinality.SINGLE;
        }

        @Override
        public PageResult<Vertex> listVertices(
                String label, Map<String, Object> filter, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageResult<Edge> listEdges(
                String label, Map<String, Object> filter, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Shard> vertexShards(long splitSize) {
            if (shardFailure != null) {
                throw shardFailure;
            }
            return vertexShards;
        }

        @Override
        public List<Shard> edgeShards(long splitSize) {
            if (shardFailure != null) {
                throw shardFailure;
            }
            return edgeShards;
        }

        @Override
        public PageResult<Vertex> scanVertices(Shard shard, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PageResult<Edge> scanEdges(Shard shard, String page, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
