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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enumerates the single split used by the Amazon DocumentDB V1 basic read path.
 *
 * <p>The split id hashes to subtask 0, and every other registered reader immediately receives
 * no-more-splits. Checkpoint state retains only unassigned filter/projection descriptors; it does
 * not capture cursor progress, so recovery of an assigned split performs a full rescan.
 */
public class AmazonDocumentDBSourceSplitEnumerator
        implements SourceSplitEnumerator<AmazonDocumentDBSourceSplit, AmazonDocumentDBSourceState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(AmazonDocumentDBSourceSplitEnumerator.class);

    private final Context<AmazonDocumentDBSourceSplit> enumeratorContext;
    private final String matchQuery;
    private final String projection;
    private final Map<Integer, List<AmazonDocumentDBSourceSplit>> pendingSplits = new HashMap<>();
    private final Object stateLock = new Object();

    private boolean shouldEnumerate;

    public AmazonDocumentDBSourceSplitEnumerator(
            Context<AmazonDocumentDBSourceSplit> enumeratorContext,
            AmazonDocumentDBSourceState sourceState,
            String matchQuery,
            String projection) {
        this.enumeratorContext = enumeratorContext;
        this.matchQuery = matchQuery;
        this.projection = projection;
        this.shouldEnumerate = sourceState == null || sourceState.isShouldEnumerate();
        if (sourceState != null) {
            this.pendingSplits.putAll(sourceState.getPendingSplits());
        }
    }

    /** Defers enumeration to {@link #run()} so registered readers are visible before assignment. */
    @Override
    public void open() {
        // no-op
    }

    /**
     * Creates exactly one split on the first run and assigns it according to its fixed owner.
     * Readers without that split are completed immediately rather than waiting for more work that
     * V1 will never enumerate.
     */
    @Override
    public void run() {
        synchronized (stateLock) {
            if (shouldEnumerate) {
                addPendingSplits(
                        Collections.singletonList(
                                new AmazonDocumentDBSourceSplit(0, matchQuery, projection)));
                shouldEnumerate = false;
            }
            assignSplits(enumeratorContext.registeredReaders());
        }
    }

    /** Holds no external resources; readers own the MongoDB clients and cursors. */
    @Override
    public void close() throws IOException {
        // no-op
    }

    /**
     * Returns failed reader work to its deterministic owner and defers reassignment until that
     * reader is registered again.
     */
    @Override
    public void addSplitsBack(List<AmazonDocumentDBSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            addPendingSplits(splits);
            assignSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }
    }

    /** Ignores pull requests because the bounded split is assigned eagerly in {@link #run()}. */
    @Override
    public void handleSplitRequest(int subtaskId) {
        // The bounded source enumerates its only split eagerly.
    }

    /**
     * Assigns pending work when a reader registers.
     *
     * <p>Zeta registers readers before invoking {@link #run()}, so an empty pending-split set here
     * does not mean that enumeration has finished. No-more-splits is signaled only after {@link
     * #run()} has completed enumeration; a reader registered after that point is signaled
     * immediately.
     */
    @Override
    public void registerReader(int subtaskId) {
        synchronized (stateLock) {
            assignSplits(Collections.singleton(subtaskId));
        }
    }

    /**
     * Snapshots only enumeration status and unassigned split descriptors, never cursor progress.
     */
    @Override
    public AmazonDocumentDBSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            return new AmazonDocumentDBSourceState(shouldEnumerate, pendingSplits);
        }
    }

    /**
     * Has no post-checkpoint commit because enumeration state is fully represented in snapshots.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no-op
    }

    private void addPendingSplits(Collection<AmazonDocumentDBSourceSplit> splits) {
        int readerCount = enumeratorContext.currentParallelism();
        for (AmazonDocumentDBSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.getSplitId(), readerCount);
            pendingSplits.computeIfAbsent(ownerReader, ignored -> new ArrayList<>()).add(split);
        }
    }

    /**
     * Gives the sole split to subtask 0 and, after enumeration, sends no-more-splits to all
     * registered readers.
     *
     * <p>Assignments are removed before the callback and restored if the callback fails, keeping a
     * checkpoint from losing an unacknowledged split.
     */
    private void assignSplits(Set<Integer> readers) {
        Set<Integer> registeredReaders = enumeratorContext.registeredReaders();
        for (int reader : readers) {
            if (!registeredReaders.contains(reader)) {
                LOG.warn("Reader {} is not registered. Pending splits are not assigned.", reader);
                continue;
            }

            List<AmazonDocumentDBSourceSplit> assignment = pendingSplits.remove(reader);
            if (assignment != null && !assignment.isEmpty()) {
                LOG.info("Assign splits {} to reader {}", assignment, reader);
                try {
                    enumeratorContext.assignSplit(reader, assignment);
                } catch (Exception e) {
                    LOG.error("Failed to assign splits {} to reader {}", assignment, reader, e);
                    pendingSplits.put(reader, assignment);
                    continue;
                }
            }
            if (!shouldEnumerate) {
                enumeratorContext.signalNoMoreSplits(reader);
            }
        }
    }

    private static int getSplitOwner(Integer splitId, int readerCount) {
        return (splitId.hashCode() & Integer.MAX_VALUE) % readerCount;
    }
}
