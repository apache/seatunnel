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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

/**
 * Enumerator for the first Vitess CDC delivery.
 *
 * <p>The connector intentionally owns one long-running streaming split. This keeps the first
 * delivery narrow while still integrating with SeaTunnel checkpoint and restore semantics.
 */
public class VitessSourceEnumerator
        implements SourceSplitEnumerator<VitessSourceSplit, VitessSourceEnumeratorState> {

    /** Pending split list doubles as the checkpoint state. */
    private final List<VitessSourceSplit> pendingSplits;

    private final Context<VitessSourceSplit> context;

    public VitessSourceEnumerator(
            Context<VitessSourceSplit> context, VitessSourceEnumeratorState restoredState) {
        this.context = context;
        if (restoredState == null) {
            this.pendingSplits = new ArrayList<>();
        } else {
            this.pendingSplits = restoredState.getPendingSplits();
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        assignPendingSplits(context.registeredReaders());
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<VitessSourceSplit> splits, int subtaskId) {
        if (splits == null || splits.isEmpty()) {
            return;
        }
        pendingSplits.addAll(copySplits(splits));
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        assignPendingSplits(Collections.singleton(subtaskId));
    }

    @Override
    public VitessSourceEnumeratorState snapshotState(long checkpointId) {
        return new VitessSourceEnumeratorState(pendingSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void assignPendingSplits(Set<Integer> readers) {
        if (pendingSplits.isEmpty() || readers == null || readers.isEmpty()) {
            return;
        }
        OptionalInt readerId = readers.stream().mapToInt(Integer::intValue).min();
        if (!readerId.isPresent()) {
            return;
        }
        VitessSourceSplit split = pendingSplits.remove(0);
        context.assignSplit(readerId.getAsInt(), split);
        for (Integer registeredReader : readers) {
            context.signalNoMoreSplits(registeredReader);
        }
    }

    private static List<VitessSourceSplit> copySplits(List<VitessSourceSplit> splits) {
        List<VitessSourceSplit> copies = new ArrayList<>(splits.size());
        for (VitessSourceSplit split : splits) {
            copies.add(split.copy());
        }
        return copies;
    }
}
