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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.graph.Shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Assigns read work to HugeGraph source readers.
 *
 * <p>Discovery is driven by parallelism:
 *
 * <ul>
 *   <li>parallelism == 1: a single {@code LABEL_LIST} split (server-side label/property filtering
 *       preserved).
 *   <li>parallelism &gt; 1: one {@code SHARD} split per key-range shard returned by {@code
 *       traverser().vertexShards / edgeShards}, so readers scan disjoint ranges in parallel.
 * </ul>
 *
 * <p>Splits are assigned exactly once and tracked in {@link #assignedSplits}. On restore the
 * enumerator does not re-discover; it assigns only the still-unassigned splits and relies on each
 * reader to resume its already-assigned splits from reader state, so no split is read twice.
 */
public class HugeGraphSourceSplitEnumerator
        implements SourceSplitEnumerator<HugeGraphSourceSplit, HugeGraphSourceState> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSourceSplitEnumerator.class);

    private final Context<HugeGraphSourceSplit> context;
    private final HugeGraphSourceConfig sourceConfig;
    private final long splitSize;
    private final Supplier<HugeGraphOperations> clientFactory;
    private final Object lock = new Object();

    private final Set<HugeGraphSourceSplit> allSplits = new LinkedHashSet<>();
    private final Set<HugeGraphSourceSplit> assignedSplits = new HashSet<>();
    private boolean needsDiscovery;

    public HugeGraphSourceSplitEnumerator(
            Context<HugeGraphSourceSplit> context,
            HugeGraphSourceConfig sourceConfig,
            long splitSize) {
        this(context, sourceConfig, splitSize, null);
    }

    public HugeGraphSourceSplitEnumerator(
            Context<HugeGraphSourceSplit> context,
            HugeGraphSourceConfig sourceConfig,
            long splitSize,
            HugeGraphSourceState restoredState) {
        this(
                context,
                sourceConfig,
                splitSize,
                restoredState,
                () -> new HugeGraphClient(sourceConfig.getConnectionConfig()));
    }

    HugeGraphSourceSplitEnumerator(
            Context<HugeGraphSourceSplit> context,
            HugeGraphSourceConfig sourceConfig,
            long splitSize,
            HugeGraphSourceState restoredState,
            Supplier<HugeGraphOperations> clientFactory) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitSize = splitSize;
        this.clientFactory = clientFactory;
        if (restoredState == null) {
            this.needsDiscovery = true;
        } else {
            this.needsDiscovery = false;
            this.allSplits.addAll(restoredState.getAllSplits());
            this.assignedSplits.addAll(restoredState.getAssignedSplits());
        }
    }

    @Override
    public void open() {
        if (needsDiscovery) {
            synchronized (lock) {
                discover();
                needsDiscovery = false;
            }
        }
    }

    private void discover() {
        if (sourceConfig.isReadAllLabels()) {
            for (String label : sourceConfig.getLabels()) {
                allSplits.add(HugeGraphSourceSplit.labelListSplit("label-list-" + label, label));
            }
            LOG.info(
                    "HugeGraph source: read-all-labels, created {} label-list split(s) for {} "
                            + "labels {}",
                    allSplits.size(),
                    sourceConfig.getLabelType(),
                    sourceConfig.getLabels());
            return;
        }
        int parallelism = context.currentParallelism();

        // Runtime guard: the factory-level checkFilterParallelism() reads the per-source
        // 'parallelism' option, which does not see env { parallelism = N }. This runtime check
        // catches the combination at the last safe point — before any shard splits are created
        // — so filter + parallelism > 1 is guaranteed to fail fast.
        Map<String, Object> filter = sourceConfig.getFilter();
        boolean hasFilter = filter != null && !filter.isEmpty();
        if (parallelism > 1 && hasFilter) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "HugeGraph source 'filter' cannot be combined with parallelism > 1 "
                                    + "(runtime parallelism is %d): parallel reads use shard "
                                    + "key-range scans that do not support server-side property "
                                    + "filtering. Either set parallelism to 1 to keep the filter, "
                                    + "or remove the filter to read in parallel.",
                            parallelism));
        }

        if (parallelism <= 1) {
            allSplits.add(
                    HugeGraphSourceSplit.labelListSplit("label-list", sourceConfig.getLabel()));
            LOG.info(
                    "HugeGraph source: parallelism=1, using single label-list split for label '{}'",
                    sourceConfig.getLabel());
            return;
        }
        boolean vertex = sourceConfig.getLabelType() == MappingConfig.LabelType.VERTEX;
        HugeGraphOperations client = clientFactory.get();
        List<Shard> shards;
        try {
            shards = vertex ? client.vertexShards(splitSize) : client.edgeShards(splitSize);
        } catch (RuntimeException e) {
            // Shard splitting is a scan-capable-backend feature. The in-memory backend rejects
            // vertexShards/edgeShards, and the raw server error gives the user no way forward, so
            // point them at the parallelism=1 label-list path (the original error is kept as
            // cause).
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "Failed to split %s label '%s' into shards for a parallel (parallelism>1) "
                                    + "read. Shard scans require a scan-capable HugeGraph backend "
                                    + "(RocksDB/HBase/Cassandra); the in-memory backend does not "
                                    + "support them. Set parallelism=1 to read via a single "
                                    + "label-list scan instead.",
                            vertex ? "vertex" : "edge", sourceConfig.getLabel()),
                    e);
        } finally {
            client.close();
        }
        int index = 0;
        for (Shard shard : shards) {
            allSplits.add(HugeGraphSourceSplit.shardSplit("shard-" + index, shard));
            index++;
        }
        LOG.info(
                "HugeGraph source: parallelism={}, discovered {} shard split(s) for {} label '{}' "
                        + "(split_size={})",
                parallelism,
                allSplits.size(),
                vertex ? "vertex" : "edge",
                sourceConfig.getLabel(),
                splitSize);
    }

    @Override
    public void run() {
        synchronized (lock) {
            int parallelism = context.currentParallelism();
            List<List<HugeGraphSourceSplit>> perReader = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                perReader.add(new ArrayList<>());
            }
            int cursor = 0;
            for (HugeGraphSourceSplit split : allSplits) {
                if (assignedSplits.contains(split)) {
                    continue;
                }
                perReader.get(cursor % parallelism).add(split);
                cursor++;
            }
            for (int subtask = 0; subtask < parallelism; subtask++) {
                List<HugeGraphSourceSplit> share = perReader.get(subtask);
                context.assignSplit(subtask, share);
                assignedSplits.addAll(share);
                // Bounded source: tell every reader (even those with no splits) that no more
                // splits are coming, so it can finish once it drains what it was assigned.
                context.signalNoMoreSplits(subtask);
            }
        }
    }

    @Override
    public void addSplitsBack(List<HugeGraphSourceSplit> splits, int subtaskId) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        synchronized (lock) {
            assignedSplits.removeAll(splits);
            context.assignSplit(subtaskId, splits);
            assignedSplits.addAll(splits);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (lock) {
            return allSplits.size() - assignedSplits.size();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Push model: splits are assigned eagerly in run()/addSplitsBack, not on request.
    }

    @Override
    public void registerReader(int subtaskId) {
        // No-op: assignment happens in run().
    }

    @Override
    public HugeGraphSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new HugeGraphSourceState(
                    new HashSet<>(allSplits), new HashSet<>(assignedSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // No-op.
    }

    @Override
    public void close() {
        // The discovery client is opened and closed within discover(); nothing long-lived to close.
    }
}
