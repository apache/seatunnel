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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartMode;

import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.metadata.TableInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class FlussSourceSplitEnumerator
        implements SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> {

    private final FlussSourceConfig config;
    private final Context<FlussSourceSplit> context;
    private final boolean streaming;

    private final Object lock = new Object();
    private final Set<FlussSourceSplit> pendingSplits = new HashSet<>();
    private final boolean restored;

    private boolean initialized = false;
    private final Set<Integer> noMoreSplitsSignaled = new HashSet<>();

    public FlussSourceSplitEnumerator(
            FlussSourceConfig config,
            Context<FlussSourceSplit> context,
            FlussSourceState restoreState,
            boolean streaming) {
        this.config = config;
        this.context = context;
        this.streaming = streaming;
        this.restored = restoreState != null;
        if (restored) {
            pendingSplits.addAll(restoreState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        // No-op: discovery opens its own short-lived admin client; a restored enumerator skips it.
    }

    @Override
    public void run() throws Exception {
        synchronized (lock) {
            if (!initialized) {
                if (!restored) {
                    pendingSplits.addAll(discoverSplits());
                }
                initialized = true;
            }
        }
        assignSplits();
    }

    private List<FlussSourceSplit> discoverSplits() {
        try (FlussAdminClient adminClient =
                new FlussAdminClient(
                        config.buildFlussConfig(), config.getTablePath().getFullName())) {
            TablePath tablePath = config.getTablePath();
            TableInfo tableInfo = adminClient.getTableInfo(tablePath);
            int numBuckets = tableInfo.getNumBuckets();
            long tableId = tableInfo.getTableId();
            List<Integer> buckets =
                    IntStream.range(0, numBuckets).boxed().collect(Collectors.toList());

            if (streaming) {
                Map<Integer, Long> latestOffsets =
                        config.getStartMode() == StartMode.LATEST
                                ? adminClient.latestOffsets(tablePath, buckets)
                                : null;
                List<FlussSourceSplit> splits = new ArrayList<>(numBuckets);
                for (int bucket : buckets) {
                    long startOffset =
                            latestOffsets != null
                                    ? latestOffsets.get(bucket)
                                    : LogScanner.EARLIEST_OFFSET;
                    splits.add(
                            new FlussSourceSplit(
                                    tablePath, tableId, bucket, startOffset, Long.MAX_VALUE));
                }
                log.info(
                        "Discovered {} bucket split(s) for table {} (streaming, start_mode={})",
                        splits.size(),
                        tablePath.getFullName(),
                        config.getStartMode());
                return splits;
            }

            FlussAdminClient.BucketBounds bounds = adminClient.bucketBounds(tablePath, buckets);
            List<FlussSourceSplit> splits = new ArrayList<>(numBuckets);
            int emptyBuckets = 0;
            for (int bucket : buckets) {
                long earliest = bounds.earliest.get(bucket);
                long latest = bounds.latest.get(bucket);
                if (earliest == latest) {
                    emptyBuckets++;
                    continue;
                }
                splits.add(
                        new FlussSourceSplit(
                                tablePath, tableId, bucket, LogScanner.EARLIEST_OFFSET, latest));
            }
            log.info(
                    "Discovered {} bucket split(s) for table {} (batch, {} empty bucket(s) skipped)",
                    splits.size(),
                    tablePath.getFullName(),
                    emptyBuckets);
            return splits;
        }
    }

    private void assignSplits() {
        synchronized (lock) {
            if (!initialized) {
                return;
            }
            int parallelism = context.currentParallelism();
            Set<Integer> registeredReaders = context.registeredReaders();
            Map<Integer, List<FlussSourceSplit>> assignment = new HashMap<>();
            for (FlussSourceSplit split : pendingSplits) {
                int owner = getSplitOwner(split, parallelism);
                if (registeredReaders.contains(owner)) {
                    assignment.computeIfAbsent(owner, k -> new ArrayList<>()).add(split);
                }
            }
            assignment.forEach(
                    (subtaskId, splits) -> {
                        context.assignSplit(subtaskId, splits);
                        splits.forEach(pendingSplits::remove);
                    });
            if (!streaming) {
                for (int subtaskId : registeredReaders) {
                    if (noMoreSplitsSignaled.add(subtaskId)) {
                        context.signalNoMoreSplits(subtaskId);
                    }
                }
            }
        }
    }

    private static int getSplitOwner(FlussSourceSplit split, int parallelism) {
        int hash = split.getTablePath().getFullName().hashCode() * 31 + split.getBucketId();
        return HashUtils.bucketIndex(hash, parallelism);
    }

    @Override
    public void addSplitsBack(List<FlussSourceSplit> splits, int subtaskId) {
        synchronized (lock) {
            if (splits == null || splits.isEmpty()) {
                return;
            }
            splits.forEach(pendingSplits::remove);
            pendingSplits.addAll(splits);
            if (context.registeredReaders().contains(subtaskId)) {
                assignSplits();
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new UnsupportedOperationException(
                "Does not support split requests: subtask " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        assignSplits();
    }

    @Override
    public FlussSourceState snapshotState(long checkpointId) {
        synchronized (lock) {
            return new FlussSourceState(new HashSet<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no-op: read positions are persisted in the split state
    }

    @Override
    public void close() throws IOException {
        // No-op: the admin client used for discovery is short-lived and closed there.
    }
}
