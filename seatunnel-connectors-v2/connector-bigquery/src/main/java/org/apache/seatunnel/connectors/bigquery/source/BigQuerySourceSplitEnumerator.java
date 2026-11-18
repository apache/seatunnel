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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class BigQuerySourceSplitEnumerator
        implements SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> {

    private final Context<BigQuerySourceSplit> context;
    private final String query;
    private final ConcurrentLinkedQueue<BigQuerySourceSplit> pendingSplits;

    /** Tracks whether initial split discovery has completed in {@link #run()}. */
    private volatile boolean splitsDiscovered = false;

    /** Tracks the reader that received the split assignment, or -1 if none assigned. */
    private volatile int assignedReader = -1;

    public BigQuerySourceSplitEnumerator(
            Context<BigQuerySourceSplit> context, String query, BigQuerySourceState restoreState) {
        this.context = context;
        this.query = query;
        if (restoreState != null && restoreState.getPendingSplits() != null) {
            this.pendingSplits = new ConcurrentLinkedQueue<>(restoreState.getPendingSplits());
        } else {
            this.pendingSplits = new ConcurrentLinkedQueue<>();
            this.pendingSplits.add(new BigQuerySourceSplit("0", query));
        }
    }

    @Override
    public void open() {
        // no-op
    }

    @Override
    public void run() {
        assignPendingSplits();
        splitsDiscovered = true;
        signalNoMoreSplitsToIdleReaders();
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public void addSplitsBack(List<BigQuerySourceSplit> splits, int subtaskId) {
        if (splits != null) {
            pendingSplits.addAll(splits);
            assignPendingSplits();
            signalNoMoreSplitsToIdleReaders();
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.isEmpty() ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        log.warn(
                "BigQuery source does not support split request from subtask {}, splits are pushed.",
                subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        assignPendingSplits();
        if (splitsDiscovered && assignedReader != subtaskId) {
            context.signalNoMoreSplits(subtaskId);
            log.info("Signaled no more splits to late-registered reader {}", subtaskId);
        }
    }

    @Override
    public BigQuerySourceState snapshotState(long checkpointId) {
        return new BigQuerySourceState(new ArrayList<>(pendingSplits));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no-op
    }

    /**
     * Assigns all pending splits to the first registered reader. BigQuery source uses a single
     * split (one query), so all work goes to one reader. The remaining readers are signaled
     * separately via {@link #signalNoMoreSplitsToIdleReaders()}.
     */
    private void assignPendingSplits() {
        Set<Integer> readers = context.registeredReaders();
        if (readers.isEmpty() || pendingSplits.isEmpty()) {
            return;
        }
        int firstReader = readers.iterator().next();
        List<BigQuerySourceSplit> toAssign = new ArrayList<>();
        while (!pendingSplits.isEmpty()) {
            toAssign.add(pendingSplits.poll());
        }
        if (!toAssign.isEmpty()) {
            context.assignSplit(firstReader, toAssign);
            context.signalNoMoreSplits(firstReader);
            assignedReader = firstReader;
            log.info("Assigned {} split(s) to reader {}", toAssign.size(), firstReader);
        }
    }

    /**
     * Signals no-more-splits to every registered reader that did not receive a split assignment.
     * Ensures idle and late-registered readers can terminate instead of waiting indefinitely. Must
     * be called after {@link #assignPendingSplits()} and after {@code splitsDiscovered} is set.
     */
    private void signalNoMoreSplitsToIdleReaders() {
        if (!splitsDiscovered) {
            return;
        }
        Set<Integer> readers = context.registeredReaders();
        for (int reader : readers) {
            if (reader != assignedReader) {
                context.signalNoMoreSplits(reader);
                log.info("Signaled no more splits to idle reader {}", reader);
            }
        }
    }
}
