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

package org.apache.seatunnel.connectors.seatunnel.paimon.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceSplitGenerator;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceState;

import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class AbstractSplitEnumerator
        implements SourceSplitEnumerator<PaimonSourceSplit, PaimonSourceState> {

    /** Source split enumerator context */
    protected final Context<PaimonSourceSplit> context;

    protected final Set<Integer> readersAwaitingSplit;

    protected final PaimonSourceSplitGenerator splitGenerator;

    /** The splits that have not assigned */
    protected Deque<PaimonSourceSplit> pendingSplits;

    protected final TableScan tableScan;

    private final int splitMaxNum;

    @Nullable protected Long nextSnapshotId;

    private ExecutorService executorService;

    public AbstractSplitEnumerator(
            Context<PaimonSourceSplit> context,
            Deque<PaimonSourceSplit> pendingSplits,
            @Nullable Long nextSnapshotId,
            TableScan tableScan,
            int splitMaxPerTask) {
        this.context = context;
        this.pendingSplits = new LinkedList<>(pendingSplits);
        this.nextSnapshotId = nextSnapshotId;
        this.readersAwaitingSplit = new LinkedHashSet<>();
        this.splitGenerator = new PaimonSourceSplitGenerator();
        this.tableScan = tableScan;
        this.splitMaxNum = context.currentParallelism() * splitMaxPerTask;
        this.executorService =
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder()
                                .setNameFormat("Seatunnel-PaimonSourceSplitEnumerator-%d")
                                .build());
        if (tableScan instanceof StreamTableScan && nextSnapshotId != null) {
            ((StreamTableScan) tableScan).restore(nextSnapshotId);
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() throws Exception {
        loadNewSplits();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(executorService) && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }

    @Override
    public void addSplitsBack(List<PaimonSourceSplit> splits, int subtaskId) {
        log.debug("Paimon Source Enumerator adds splits back: {}", splits);
        this.pendingSplits.addAll(splits);
        if (context.registeredReaders().contains(subtaskId)) {
            assignSplits();
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void registerReader(int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
    }

    @Override
    public PaimonSourceState snapshotState(long checkpointId) throws Exception {
        return new PaimonSourceState(pendingSplits, nextSnapshotId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void addSplits(Collection<PaimonSourceSplit> newSplits) {
        this.pendingSplits.addAll(newSplits);
    }

    /**
     * Method should be synchronized because {@link #handleSplitRequest} and {@link
     * #processDiscoveredSplits} have thread conflicts.
     */
    protected synchronized void assignSplits() {
        Iterator<Integer> pendingReaderIterator = readersAwaitingSplit.iterator();
        while (pendingReaderIterator.hasNext()) {
            Integer pendingReader = pendingReaderIterator.next();
            if (!context.registeredReaders().contains(pendingReader)) {
                pendingReaderIterator.remove();
                continue;
            }
            LinkedList<PaimonSourceSplit> assignedTaskSplits = new LinkedList<>();
            for (PaimonSourceSplit fileSourceSplit : pendingSplits) {
                final int splitOwner =
                        getSplitOwner(fileSourceSplit.splitId(), context.currentParallelism());
                if (splitOwner == pendingReader) {
                    assignedTaskSplits.add(fileSourceSplit);
                }
            }

            if (!assignedTaskSplits.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignedTaskSplits, pendingReader);
                try {
                    context.assignSplit(pendingReader, assignedTaskSplits);
                    // remove the assigned splits from pending splits
                    assignedTaskSplits.forEach(pendingSplits::remove);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignedTaskSplits,
                            pendingReader,
                            e);
                    pendingSplits.addAll(assignedTaskSplits);
                }
            }
        }
    }

    protected void loadNewSplits() {
        CompletableFuture.supplyAsync(this::scanNextSnapshot, executorService)
                .whenComplete(this::processDiscoveredSplits);
    }

    /** Hash algorithm for assigning splits to readers */
    protected static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    // ------------------------------------------------------------------------

    // This need to be synchronized because scan object is not thread safe. handleSplitRequest and
    // CompletableFuture.supplyAsync will invoke this.
    protected synchronized Optional<PlanWithNextSnapshotId> scanNextSnapshot() {
        if (pendingSplits.size() >= splitMaxNum) {
            return Optional.empty();
        }
        TableScan.Plan plan = tableScan.plan();
        Long nextSnapshotId = null;
        if (tableScan instanceof StreamTableScan) {
            nextSnapshotId = ((StreamTableScan) tableScan).checkpoint();
        }
        return Optional.of(new PlanWithNextSnapshotId(plan, nextSnapshotId));
    }

    // This method could not be synchronized, because it runs in coordinatorThread, which will make
    // it serializable execution.
    protected void processDiscoveredSplits(
            Optional<PlanWithNextSnapshotId> planWithNextSnapshotIdOptional, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                log.debug("Catching EndOfStreamException, the stream is finished.");
                assignSplits();
            } else {
                log.error("Failed to enumerate files", error);
                throw new SeaTunnelException(error);
            }
            return;
        }
        if (!planWithNextSnapshotIdOptional.isPresent()) {
            return;
        }
        PlanWithNextSnapshotId planWithNextSnapshotId = planWithNextSnapshotIdOptional.get();
        nextSnapshotId = planWithNextSnapshotId.nextSnapshotId;
        TableScan.Plan plan = planWithNextSnapshotId.plan;

        if (plan.splits().isEmpty()) {
            return;
        }

        addSplits(splitGenerator.createSplits(plan));
        assignSplits();
    }

    /** The result of scan. */
    @Getter
    protected static class PlanWithNextSnapshotId {
        private final TableScan.Plan plan;
        private final Long nextSnapshotId;

        public PlanWithNextSnapshotId(TableScan.Plan plan, Long nextSnapshotId) {
            this.plan = plan;
            this.nextSnapshotId = nextSnapshotId;
        }
    }
}
