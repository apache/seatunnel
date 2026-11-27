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

package org.apache.seatunnel.engine.e2e.committerregistration;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only sink with an aggregated committer whose writers can be held while they are created.
 *
 * <p>Zeta creates a sink writer in {@code SinkFlowLifeCycle#restoreState}, before {@code
 * SinkFlowLifeCycle#open} registers the writer with the pipeline's aggregated committer. Holding
 * {@link #createWriter} therefore holds the registration, so a test can end the committer's task
 * group first and then let the writers register with a committer that has already ended.
 *
 * <p>The Zeta cluster of the engine E2E tests runs inside the test JVM, so the test controls the
 * hold and reads the commits through the static fields below. Call {@link #reset} before each job.
 */
public class CommitterRegistrationTestSink
        implements SeaTunnelSink<SeaTunnelRow, Void, String, String> {

    /** Upper bound for a held writer, so a broken test cannot leave a task thread waiting. */
    private static final long MAX_HOLD_SECONDS = 300L;

    private static final AtomicBoolean HOLD_WRITERS = new AtomicBoolean();
    private static final AtomicInteger NEXT_COMMITTER_DEPLOYMENT = new AtomicInteger();
    private static volatile CountDownLatch writersHeld = new CountDownLatch(0);
    private static volatile CountDownLatch writersReleased = new CountDownLatch(0);

    /** Subtask indexes committed, by the deployment of the committer that committed them. */
    private static final Map<Integer, Set<String>> COMMITTED_BY_DEPLOYMENT =
            new ConcurrentHashMap<>();

    /**
     * Prepares a new job: the next {@code writers} writers are held in {@link #createWriter} until
     * {@link #releaseWriters()} is called, and no commit is recorded yet.
     *
     * @param writers number of writers to hold
     */
    public static void reset(int writers) {
        COMMITTED_BY_DEPLOYMENT.clear();
        writersHeld = new CountDownLatch(writers);
        writersReleased = new CountDownLatch(1);
        HOLD_WRITERS.set(writers > 0);
    }

    /**
     * Waits until every writer to hold has entered {@link #createWriter}.
     *
     * @return {@code true} if all of them are held
     */
    public static boolean awaitWritersHeld(long timeout, TimeUnit unit)
            throws InterruptedException {
        return writersHeld.await(timeout, unit);
    }

    /** Lets the held writers return, and stops holding writers created after this call. */
    public static void releaseWriters() {
        HOLD_WRITERS.set(false);
        writersReleased.countDown();
    }

    /**
     * Returns the subtask indexes committed, by the deployment of the committer that committed
     * them.
     */
    public static Map<Integer, Set<String>> committedByDeployment() {
        return Collections.unmodifiableMap(COMMITTED_BY_DEPLOYMENT);
    }

    /**
     * Returns the deployment number of a deployed committer of this sink.
     *
     * @param committer the aggregated committer of a deployed {@code SinkAggregatedCommitterTask}
     */
    public static int committerDeployment(Object committer) {
        return ((CountingCommitter) committer).deployment;
    }

    @Override
    public String getPluginName() {
        return CommitterRegistrationTestSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, String, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        if (HOLD_WRITERS.get()) {
            writersHeld.countDown();
            try {
                if (!writersReleased.await(MAX_HOLD_SECONDS, TimeUnit.SECONDS)) {
                    throw new IOException("The test did not release the held writer");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while the writer was held", e);
            }
        }
        return new SubtaskWriter(context.getIndexOfSubtask());
    }

    @Override
    public Optional<Serializer<String>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<String, String>> createAggregatedCommitter() {
        return Optional.of(new CountingCommitter());
    }

    @Override
    public Optional<Serializer<String>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    /** Writer that drops every row and hands its subtask index to the committer. */
    private static class SubtaskWriter implements SinkWriter<SeaTunnelRow, String, Void> {

        private final String subtask;

        private SubtaskWriter(int indexOfSubtask) {
            this.subtask = "subtask-" + indexOfSubtask;
        }

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<String> prepareCommit() {
            return Optional.of(subtask);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    /**
     * Aggregated committer that records which subtasks it committed.
     *
     * <p>Zeta creates the committer once when it builds the physical plan and deploys a copy of it
     * each time the pipeline is deployed, so a restored pipeline's committer is a copy of the first
     * one. {@link #init()} runs once per deployment and numbers each deployed copy.
     */
    private static class CountingCommitter implements SinkAggregatedCommitter<String, String> {

        private int deployment;

        @Override
        public void init() {
            deployment = NEXT_COMMITTER_DEPLOYMENT.incrementAndGet();
        }

        /**
         * Records the committed subtasks. A restore calls this through {@link #restoreCommit} with
         * an empty list when there is nothing to re-commit, which is not recorded.
         */
        @Override
        public List<String> commit(List<String> aggregatedCommitInfo) {
            if (aggregatedCommitInfo.isEmpty()) {
                return Collections.emptyList();
            }
            Set<String> committed =
                    COMMITTED_BY_DEPLOYMENT.computeIfAbsent(
                            deployment, key -> ConcurrentHashMap.newKeySet());
            for (String combined : aggregatedCommitInfo) {
                committed.addAll(Arrays.asList(combined.split(",")));
            }
            return Collections.emptyList();
        }

        @Override
        public String combine(List<String> commitInfos) {
            return String.join(",", commitInfos);
        }

        @Override
        public void abort(List<String> aggregatedCommitInfo) {}

        @Override
        public void close() {}
    }
}
