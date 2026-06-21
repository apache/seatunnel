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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultErrorSinkWriterTest {

    @Test
    public void testValidateSupportedErrorSinkLifecycleAllowsSimpleSink() {
        assertDoesNotThrow(
                () ->
                        DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                "simple", new TestSink(false, false, false)));
    }

    @Test
    public void testValidateSupportedErrorSinkLifecycleRejectsStatefulSink() {
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                        "stateful", new TestSink(true, false, false)));

        assertTrue(ex.getMessage().contains("writer state serializer"));
    }

    @Test
    public void testValidateSupportedErrorSinkLifecycleRejectsCommittingSink() {
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                        "committing", new TestSink(false, true, true)));

        assertTrue(ex.getMessage().contains("commit info serializer"));
        assertTrue(ex.getMessage().contains("committer"));
        assertTrue(ex.getMessage().contains("aggregated commit info serializer"));
        assertTrue(ex.getMessage().contains("aggregated committer"));
    }

    private static class TestSink implements SeaTunnelSink<SeaTunnelRow, String, String, String> {

        private final boolean writerStateful;
        private final boolean committing;
        private final boolean aggregatedCommitting;

        private TestSink(boolean writerStateful, boolean committing, boolean aggregatedCommitting) {
            this.writerStateful = writerStateful;
            this.committing = committing;
            this.aggregatedCommitting = aggregatedCommitting;
        }

        @Override
        public String getPluginName() {
            return "Test";
        }

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            return new NoopWriter();
        }

        @Override
        public Optional<Serializer<String>> getWriterStateSerializer() {
            return writerStateful ? Optional.of(new NoopSerializer()) : Optional.empty();
        }

        @Override
        public Optional<SinkCommitter<String>> createCommitter() {
            return committing ? Optional.of(new NoopCommitter()) : Optional.empty();
        }

        @Override
        public Optional<Serializer<String>> getCommitInfoSerializer() {
            return committing ? Optional.of(new NoopSerializer()) : Optional.empty();
        }

        @Override
        public Optional<SinkAggregatedCommitter<String, String>> createAggregatedCommitter() {
            return aggregatedCommitting
                    ? Optional.of(new NoopAggregatedCommitter())
                    : Optional.empty();
        }

        @Override
        public Optional<Serializer<String>> getAggregatedCommitInfoSerializer() {
            return aggregatedCommitting ? Optional.of(new NoopSerializer()) : Optional.empty();
        }
    }

    private static class NoopSerializer implements Serializer<String> {

        @Override
        public byte[] serialize(String obj) {
            return new byte[0];
        }

        @Override
        public String deserialize(byte[] serialized) {
            return "";
        }
    }

    private static class NoopWriter implements SinkWriter<SeaTunnelRow, String, String> {

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

    private static class NoopCommitter implements SinkCommitter<String> {

        @Override
        public List<String> commit(List<String> commitInfos) {
            return Collections.emptyList();
        }

        @Override
        public void abort(List<String> commitInfos) {}
    }

    private static class NoopAggregatedCommitter
            implements SinkAggregatedCommitter<String, String> {

        @Override
        public List<String> commit(List<String> aggregatedCommitInfo) {
            return Collections.emptyList();
        }

        @Override
        public String combine(List<String> commitInfos) {
            return "";
        }

        @Override
        public void abort(List<String> aggregatedCommitInfo) {}

        @Override
        public void close() throws IOException {}
    }
}
