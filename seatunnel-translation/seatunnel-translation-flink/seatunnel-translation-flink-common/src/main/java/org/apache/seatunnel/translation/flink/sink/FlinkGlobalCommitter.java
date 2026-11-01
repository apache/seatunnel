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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportResourceShare;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The committer wrapper of {@link SinkAggregatedCommitter}, which is created by {@link
 * Sink#createGlobalCommitter()}, used to unify the different implementations of {@link
 * SinkAggregatedCommitter}
 *
 * @param <CommT> The generic type of commit message type
 * @param <GlobalCommT> The generic type of global commit message type
 */
@Slf4j
public class FlinkGlobalCommitter<CommT, GlobalCommT>
        implements GlobalCommitter<CommitWrapper<CommT>, GlobalCommT> {

    private final SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter;

    private MultiTableResourceManager resourceManager;
    private final RecoveredCommitTracker tracker;
    private final java.lang.ref.ReferenceQueue<GlobalCommT> recoveredQueue =
            new java.lang.ref.ReferenceQueue<>();
    private final java.util.Map<IdentityReference<GlobalCommT>, Boolean> recovered =
            new java.util.HashMap<>();

    private static final class IdentityReference<T> extends java.lang.ref.WeakReference<T> {
        private final int hash;

        IdentityReference(T value, java.lang.ref.ReferenceQueue<T> queue) {
            super(value, queue);
            hash = System.identityHashCode(value);
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(Object other) {
            return this == other
                    || (other instanceof IdentityReference
                            && get() != null
                            && get() == ((IdentityReference<?>) other).get());
        }
    }

    private void cleanRecovered() {
        java.lang.ref.Reference<? extends GlobalCommT> reference;
        while ((reference = recoveredQueue.poll()) != null) {
            recovered.remove(reference);
        }
    }

    private void markRecovered(GlobalCommT value) {
        cleanRecovered();
        recovered.put(new IdentityReference<>(value, recoveredQueue), Boolean.TRUE);
    }

    private boolean isRecovered(GlobalCommT value) {
        cleanRecovered();
        return recovered.containsKey(new IdentityReference<>(value, null));
    }

    FlinkGlobalCommitter(SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter) {
        this(aggregatedCommitter, null);
    }

    FlinkGlobalCommitter(
            SinkAggregatedCommitter<CommT, GlobalCommT> aggregatedCommitter,
            RecoveredCommitTracker tracker) {
        this.tracker = tracker;
        this.aggregatedCommitter = aggregatedCommitter;
        if (this.aggregatedCommitter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) this.aggregatedCommitter)
                            .initMultiTableResourceManager(1, 1);
        }
        aggregatedCommitter.init();
        if (resourceManager != null) {
            ((SupportResourceShare) this.aggregatedCommitter)
                    .setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public List<GlobalCommT> filterRecoveredCommittables(List globalCommittables)
            throws IOException {
        if (tracker != null) {
            tracker.finishRestore();
        }
        List<GlobalCommT> pending = aggregatedCommitter.restoreCommit(globalCommittables);
        if (pending != null) {
            pending.forEach(this::markRecovered);
        }
        return pending == null ? Collections.emptyList() : pending;
    }

    @Override
    public GlobalCommT combine(List<CommitWrapper<CommT>> committables) throws IOException {
        boolean replay = !committables.isEmpty() && committables.get(0).isRecovered();
        if (committables.stream().anyMatch(wrapper -> wrapper.isRecovered() != replay)) {
            throw new IOException("Cannot combine restored and live committables");
        }
        GlobalCommT combined =
                aggregatedCommitter.combine(
                        committables.stream()
                                .map(CommitWrapper::getCommit)
                                .collect(Collectors.toList()));
        if (replay) {
            markRecovered(combined);
        } else if (isRecovered(combined)) {
            throw new IOException("Aggregated committer reused a pending restored transaction");
        }
        return combined;
    }

    @Override
    public List<GlobalCommT> commit(List<GlobalCommT> globalCommittables) throws IOException {
        List<GlobalCommT> replay = new ArrayList<>();
        List<GlobalCommT> live = new ArrayList<>();
        for (GlobalCommT committable : globalCommittables) {
            (isRecovered(committable) ? replay : live).add(committable);
        }
        List<GlobalCommT> pending = new ArrayList<>();
        List<GlobalCommT> replayRetry = Collections.emptyList();
        if (!replay.isEmpty()) {
            List<GlobalCommT> retry = aggregatedCommitter.restoreCommit(replay);
            if (retry != null) {
                replayRetry = retry;
                pending.addAll(retry);
            }
        }
        if (!live.isEmpty()) {
            List<GlobalCommT> retry = aggregatedCommitter.commit(live);
            if (retry != null) {
                pending.addAll(retry);
            }
        }
        replay.forEach(value -> recovered.remove(new IdentityReference<>(value, null)));
        replayRetry.forEach(this::markRecovered);
        return pending;
    }

    @Override
    public void endOfInput() throws IOException {}

    @Override
    public void close() throws Exception {
        // TODO we should move FlinkGlobalCommitter to WithPostCommitTopology with
        // StandardSinkTopologies#addGlobalCommitter,
        // because FlinkGlobalCommitter never invoke close method
        aggregatedCommitter.close();
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
    }
}
