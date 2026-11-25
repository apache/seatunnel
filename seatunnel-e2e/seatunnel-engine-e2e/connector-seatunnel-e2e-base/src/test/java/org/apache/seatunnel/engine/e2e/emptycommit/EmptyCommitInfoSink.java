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

package org.apache.seatunnel.engine.e2e.emptycommit;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only sink that owns an aggregated committer but whose writers never produce commit info.
 *
 * <p>Because {@link #createAggregatedCommitter()} is present, Zeta deploys a {@code
 * SinkAggregatedCommitterTask} for the pipeline, and every writer barrier still reaches that task's
 * {@code checkpointBarrierCounter}. Because {@link EmptyCommitInfoWriter#prepareCommit()} always
 * returns {@link Optional#empty()}, the task never receives commit info, so {@code commitInfoCache}
 * and {@code checkpointCommitInfoMap} stay empty for every checkpoint. This is the "empty
 * checkpoint" shape of apache/seatunnel#12256 that real transactional sinks such as LocalFile never
 * produce, since their writers always return commit info.
 *
 * <p>The Zeta cluster of the engine E2E tests runs inside the test JVM, so the static {@link
 * #COMPLETED_COMMITS} counter lets the test observe how many checkpoints the committer task has
 * completed without relying on wall-clock timing.
 */
public class EmptyCommitInfoSink implements SeaTunnelSink<SeaTunnelRow, Void, String, String> {

    /**
     * Number of {@code SinkAggregatedCommitter#commit} calls, one per checkpoint completed by the
     * committer task. Reset by the test before it submits the job.
     */
    public static final AtomicLong COMPLETED_COMMITS = new AtomicLong();

    @Override
    public String getPluginName() {
        return EmptyCommitInfoSinkFactory.IDENTIFIER;
    }

    @Override
    public SinkWriter<SeaTunnelRow, String, Void> createWriter(SinkWriter.Context context) {
        return new EmptyCommitInfoWriter();
    }

    @Override
    public Optional<Serializer<String>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<String, String>> createAggregatedCommitter() {
        return Optional.of(new CountingAggregatedCommitter());
    }

    @Override
    public Optional<Serializer<String>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    /** Writer that drops every row and never hands commit info to the aggregated committer. */
    private static class EmptyCommitInfoWriter implements SinkWriter<SeaTunnelRow, String, Void> {

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    /** Aggregated committer that only counts completed checkpoints. */
    private static class CountingAggregatedCommitter
            implements SinkAggregatedCommitter<String, String> {

        @Override
        public List<String> commit(List<String> aggregatedCommitInfo) {
            COMPLETED_COMMITS.incrementAndGet();
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
