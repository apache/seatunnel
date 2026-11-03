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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkAggregatedCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryAggregatedCommitter
        implements SinkAggregatedCommitter<InMemoryCommitInfo, InMemoryAggregatedCommitInfo>,
                SupportMultiTableSinkAggregatedCommitter<InMemoryConnection> {

    private static final List<String> events = new ArrayList<>();
    private static final List<InMemoryMultiTableResourceManager> resourceManagers =
            new ArrayList<>();

    /**
     * Records every {@code commit} and {@code restoreCommit} call as {@code "<method>:<size>"},
     * where size is the number of aggregated commit infos handed to this table's committer.
     *
     * <p>Kept separate from {@link #events}, whose exact contents existing multi-table tests
     * assert: Zeta calls {@code restoreCommit} with an empty list on every fresh task start, so
     * mixing these entries into {@link #events} would break those assertions. Thread-safe because
     * commits arrive on engine task threads while the test thread reads and clears it.
     */
    private static final List<String> commitEvents = new CopyOnWriteArrayList<>();

    private ReadonlyConfig config;

    public InMemoryAggregatedCommitter(ReadonlyConfig config) {
        this.config = config;
    }

    public static List<String> getEvents() {
        return events;
    }

    public static List<String> getCommitEvents() {
        return commitEvents;
    }

    public static List<InMemoryMultiTableResourceManager> getResourceManagers() {
        return resourceManagers;
    }

    private InMemoryMultiTableResourceManager resourceManager;

    @Override
    public MultiTableResourceManager<InMemoryConnection> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        events.add("initMultiTableResourceManager" + queueSize);
        return new InMemoryMultiTableResourceManager();
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<InMemoryConnection> multiTableResourceManager,
            int queueIndex) {
        events.add("setMultiTableResourceManager" + queueIndex);
        this.resourceManager = (InMemoryMultiTableResourceManager) multiTableResourceManager;
        resourceManagers.add(this.resourceManager);
    }

    @Override
    public List<InMemoryAggregatedCommitInfo> commit(
            List<InMemoryAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        commitEvents.add("commit:" + aggregatedCommitInfo.size());
        if (config.get(InMemorySinkFactory.THROW_EXCEPTION_OF_COMMITTER)) {
            throw new IOException("commit failed");
        }
        return new ArrayList<>();
    }

    /**
     * Records that the engine's restore path reached this per-table committer, then behaves exactly
     * like the inherited default by delegating to {@link #commit(List)}, so every existing test
     * using this sink keeps its behavior.
     *
     * <p>Regression guard for apache/seatunnel#12805 (fixed by #12807): {@code
     * MultiTableSinkAggregatedCommitter} did not override {@code restoreCommit}, so a restored
     * checkpoint was routed to each table's {@code commit} instead (JDBC XA then failed with
     * XAER_NOTA). Before this override this test sink used the default {@code restoreCommit ->
     * commit}, which made the two routes indistinguishable end to end; only the mocked {@code
     * MultiTableSinkAggregatedCommitterTest} covered the routing.
     */
    @Override
    public List<InMemoryAggregatedCommitInfo> restoreCommit(
            List<InMemoryAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        commitEvents.add("restoreCommit:" + aggregatedCommitInfo.size());
        return commit(aggregatedCommitInfo);
    }

    @Override
    public InMemoryAggregatedCommitInfo combine(List<InMemoryCommitInfo> commitInfos) {
        return new InMemoryAggregatedCommitInfo();
    }

    @Override
    public void abort(List<InMemoryAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {}

    @Override
    public void close() throws IOException {}
}
