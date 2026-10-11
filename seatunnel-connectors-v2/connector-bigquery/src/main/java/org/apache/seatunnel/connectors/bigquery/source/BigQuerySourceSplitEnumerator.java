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
    }

    @Override
    public BigQuerySourceState snapshotState(long checkpointId) {
        return new BigQuerySourceState(new ArrayList<>(pendingSplits));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no-op
    }

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
            log.info("Assigned {} split(s) to reader {}", toAssign.size(), firstReader);
        }
    }
}
