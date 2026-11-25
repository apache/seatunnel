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

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Isolated
class RecoveryTopologyTest {
    @Test
    void testExistingOperatorIdsRemainUnchanged() {
        Assertions.assertEquals(operatorIds(false), operatorIds(true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRealOperatorRestoresCheckpointAndKeepsLaterTrafficLive(boolean localCommitter)
            throws Exception {
        RecordingCommitter.reset();
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness = openHarness(null, localCommitter)) {
            receive(harness, "checkpoint-one", 0L, 0, 1);
            snapshot = harness.snapshot(0L, 0L);
        }
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                restored = openHarness(snapshot, localCommitter)) {
            Assertions.assertEquals(
                    Collections.singletonList("checkpoint-one"), RecordingCommitter.recovered);
            receive(restored, "checkpoint-two", 1L, 0, 1);
            restored.snapshot(1L, 1L);
            restored.notifyOfCompletedCheckpoint(1L);
            Assertions.assertEquals(
                    Collections.singletonList("checkpoint-two"), RecordingCommitter.live);
            Assertions.assertEquals(
                    new LinkedHashSet<>(Arrays.asList("checkpoint-one", "checkpoint-two")),
                    RecordingCommitter.committed);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRealOperatorRestoresPartiallyFinishedEndOfInput(boolean localCommitter)
            throws Exception {
        RecordingCommitter.reset();
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness = openHarness(null, localCommitter)) {
            receive(harness, "finished-before-restore", null, 0, 2);
            snapshot = harness.snapshot(1L, 1L);
        }
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                restored = openHarness(snapshot, localCommitter)) {
            receive(restored, "finished-after-restore", null, 1, 2);
            restored.endInput();
            Assertions.assertEquals(Collections.emptyList(), RecordingCommitter.live);
            Assertions.assertEquals(
                    Collections.singletonList("finished-before-restore|finished-after-restore"),
                    RecordingCommitter.recovered);
            Assertions.assertEquals(
                    new LinkedHashSet<>(
                            Arrays.asList("finished-before-restore", "finished-after-restore")),
                    RecordingCommitter.committed);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRealOperatorFailsUnresolvedCommitAndRestoresWithoutLosingPartialSuccess(
            boolean localCommitter) throws Exception {
        RecordingCommitter.reset();
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness = openHarness(null, localCommitter)) {
            receive(harness, "previous", 0L, 0, 1);
            harness.snapshot(0L, 0L);
            harness.notifyOfCompletedCheckpoint(0L);
            receive(harness, "first", 1L, 0, 2);
            receive(harness, "second", 1L, 1, 2);
            snapshot = harness.snapshot(1L, 1L);
            RecordingCommitter.fail = true;
            Assertions.assertThrows(
                    IOException.class, () -> harness.notifyOfCompletedCheckpoint(1L));
            Assertions.assertEquals(
                    new LinkedHashSet<>(Arrays.asList("previous", "first")),
                    RecordingCommitter.committed);
        }
        RecordingCommitter.fail = false;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                restored = openHarness(snapshot, localCommitter)) {
            // The harness reports restored checkpoint 0; deliver completion for the saved batch.
            restored.notifyOfCompletedCheckpoint(1L);
            Assertions.assertEquals(
                    new LinkedHashSet<>(Arrays.asList("previous", "first", "second")),
                    RecordingCommitter.committed);
            Assertions.assertEquals(
                    Collections.singletonList("first|second"), RecordingCommitter.recovered);
            Assertions.assertEquals(
                    Arrays.asList("previous", "first|second"), RecordingCommitter.live);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRealOperatorPermanentEndOfInputFailureStopsWithoutBusyLoop(boolean localCommitter)
            throws Exception {
        RecordingCommitter.reset();
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness = openHarness(null, localCommitter)) {
            receive(harness, "unresolved", null, 0, 1);
            RecordingCommitter.fail = true;
            Assertions.assertThrows(IOException.class, harness::endInput);
            Assertions.assertEquals(
                    Collections.singletonList("unresolved"), RecordingCommitter.live);
            Assertions.assertTrue(RecordingCommitter.committed.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRealOperatorClassifiesUpstreamReplayAfterRestore(boolean localCommitter)
            throws Exception {
        RecordingCommitter.reset();
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                harness = openHarness(null, localCommitter)) {
            snapshot = harness.snapshot(0L, 0L);
        }
        try (OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                restored = openHarness(snapshot, localCommitter)) {
            receive(restored, "upstream-replay", 0L, 0, 1);
            restored.notifyOfCompletedCheckpoint(0L);
            Assertions.assertEquals(
                    Collections.singletonList("upstream-replay"), RecordingCommitter.recovered);
            Assertions.assertTrue(RecordingCommitter.live.isEmpty());
            Assertions.assertEquals(
                    Collections.singleton("upstream-replay"), RecordingCommitter.committed);
        }
    }

    @Test
    void testWrappingPreservesAdapterCapabilitiesAndOperatorIds() {
        for (boolean localCommitter : new boolean[] {false, true}) {
            FlinkSink<SeaTunnelRow, String, String, String> legacy =
                    new FlinkSink<>(
                            new RecordingSink(localCommitter, true), Collections.emptyList(), 2);
            Sink<SeaTunnelRow> original = SinkV1Adapter.wrap(legacy);
            Sink<SeaTunnelRow> wrapped = RecoveryAwareSink.wrap(legacy);
            Assertions.assertEquals(
                    original instanceof StatefulSink, wrapped instanceof StatefulSink);
            Assertions.assertEquals(
                    original instanceof WithPostCommitTopology,
                    wrapped instanceof WithPostCommitTopology);
            Assertions.assertEquals(actualOperatorIds(original), actualOperatorIds(wrapped));
        }
        FlinkSink<SeaTunnelRow, String, String, String> noGlobal =
                new FlinkSink<>(new RecordingSink(false, false), Collections.emptyList(), 2);
        Assertions.assertEquals(
                SinkV1Adapter.wrap(noGlobal).getClass(),
                RecoveryAwareSink.wrap(noGlobal).getClass());
    }

    private Map<String, String> actualOperatorIds(Sink<SeaTunnelRow> sink) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements(new SeaTunnelRow(new Object[] {"value"}))
                .sinkTo(sink)
                .name("Recording sink");
        Map<String, String> ids = new TreeMap<>();
        for (JobVertex vertex : env.getStreamGraph().getJobGraph().getVertices()) {
            ids.put(
                    vertex.getName(),
                    vertex.getOperatorIDs().stream()
                            .map(pair -> pair.getGeneratedOperatorID().toString())
                            .collect(Collectors.joining(",")));
        }
        return ids;
    }

    private OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
            openHarness(OperatorSubtaskState snapshot, boolean localCommitter) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<CommittableMessage<CommitWrapper<String>>> input =
                env.fromElements(1)
                        .map(
                                value ->
                                        (CommittableMessage<CommitWrapper<String>>)
                                                new CommittableWithLineage<>(
                                                        new CommitWrapper<>("unused"), 1L, 0))
                        .returns(
                                TypeInformation.of(
                                        new TypeHint<
                                                CommittableMessage<CommitWrapper<String>>>() {}));
        Sink<SeaTunnelRow> sink =
                RecoveryAwareSink.wrap(
                        new FlinkSink<>(
                                new RecordingSink(localCommitter, true),
                                Collections.emptyList(),
                                2));
        ((WithPostCommitTopology<SeaTunnelRow, CommitWrapper<String>>) sink)
                .addPostCommitTopology(input);
        OneInputStreamOperatorFactory<CommittableMessage<CommitWrapper<String>>, Void> factory =
                (OneInputStreamOperatorFactory<CommittableMessage<CommitWrapper<String>>, Void>)
                        env.getStreamGraph().getStreamNodes().stream()
                                .filter(
                                        node ->
                                                node.getOperatorFactory()
                                                        instanceof
                                                        RestoredCommittableOperatorFactory)
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new AssertionError(
                                                        "Recovery operator factory missing from wrapped sink topology"))
                                .getOperatorFactory();
        // Clone the real topology factory as Flink does when shipping an operator to its task.
        factory = InstantiationUtil.clone(factory, getClass().getClassLoader());
        OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void> harness =
                new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0);
        harness.setup();
        if (snapshot == null) {
            harness.initializeEmptyState();
        } else {
            harness.initializeState(snapshot);
        }
        harness.open();
        return harness;
    }

    private void receive(
            OneInputStreamOperatorTestHarness<CommittableMessage<CommitWrapper<String>>, Void>
                    harness,
            String value,
            Long checkpoint,
            int subtask,
            int parallelism)
            throws Exception {
        harness.processElement(
                new StreamRecord<>(
                        new CommittableSummary<>(subtask, parallelism, checkpoint, 1, 1, 0)));
        harness.processElement(
                new StreamRecord<>(
                        new CommittableWithLineage<>(
                                new CommitWrapper<>(value), checkpoint, subtask)));
    }

    private static final class RecordingSink
            implements SeaTunnelSink<SeaTunnelRow, String, String, String> {
        private final boolean localCommitter;
        private final boolean aggregatedCommitter;

        private RecordingSink(boolean localCommitter, boolean aggregatedCommitter) {
            this.localCommitter = localCommitter;
            this.aggregatedCommitter = aggregatedCommitter;
        }

        @Override
        public String getPluginName() {
            return "Recording";
        }

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SinkAggregatedCommitter<String, String>> createAggregatedCommitter() {
            return aggregatedCommitter ? Optional.of(new RecordingCommitter()) : Optional.empty();
        }

        @Override
        public Optional<SinkCommitter<String>> createCommitter() {
            if (!localCommitter) {
                return Optional.empty();
            }
            return Optional.of(
                    new SinkCommitter<String>() {
                        @Override
                        public List<String> commit(List<String> values) {
                            return Collections.emptyList();
                        }

                        @Override
                        public void abort(List<String> values) {}
                    });
        }

        @Override
        public Optional<Serializer<String>> getCommitInfoSerializer() {
            return Optional.of(new DefaultSerializer<>());
        }

        @Override
        public Optional<Serializer<String>> getAggregatedCommitInfoSerializer() {
            return Optional.of(new DefaultSerializer<>());
        }

        @Override
        public Optional<Serializer<String>> getWriterStateSerializer() {
            return Optional.of(new DefaultSerializer<>());
        }
    }

    /**
     * Test-owned ledger outside the cloned operator; recovery models the sink API's idempotency
     * contract. This is operator-state coverage, not external database durability coverage.
     */
    private static final class RecordingCommitter
            implements SinkAggregatedCommitter<String, String> {
        private static final List<String> live = new ArrayList<>();
        private static final List<String> recovered = new ArrayList<>();
        private static final Set<String> committed = new LinkedHashSet<>();
        private static boolean fail;

        static void reset() {
            live.clear();
            recovered.clear();
            committed.clear();
            fail = false;
        }

        @Override
        public List<String> commit(List<String> values) {
            live.addAll(values);
            if (live.size() > 3) {
                throw new AssertionError("Unbounded retry of unresolved committables");
            }
            for (String value : values) {
                String[] transactions = value.split("\\|");
                if (fail) {
                    if (transactions.length > 1) {
                        committed.add(transactions[0]);
                    }
                    return values;
                }
                committed.addAll(Arrays.asList(transactions));
            }
            return Collections.emptyList();
        }

        @Override
        public List<String> restoreCommit(List<String> values) {
            recovered.addAll(values);
            values.forEach(value -> committed.addAll(Arrays.asList(value.split("\\|"))));
            return Collections.emptyList();
        }

        @Override
        public String combine(List<String> values) {
            return String.join("|", values);
        }

        @Override
        public void abort(List<String> values) {}

        @Override
        public void close() {}
    }

    private Map<String, String> operatorIds(boolean patched) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<CommittableMessage<CommitWrapper<String>>> input =
                env.fromElements(1)
                        .map(
                                value ->
                                        (CommittableMessage<CommitWrapper<String>>)
                                                new CommittableWithLineage<>(
                                                        new CommitWrapper<>("prepared"), 1L, 0))
                        .returns(
                                TypeInformation.of(
                                        new TypeHint<
                                                CommittableMessage<CommitWrapper<String>>>() {}))
                        .name("Commit input");
        StandardSinkTopologies.addGlobalCommitter(
                patched ? new RecoveryAwareSink.RecoveryDataStream<>(input) : input,
                EmptyCommitter::new,
                TestSerializer::new);
        JobGraph graph = env.getStreamGraph().getJobGraph();
        Map<String, String> ids = new TreeMap<>();
        for (JobVertex vertex : graph.getVertices()) {
            ids.put(
                    vertex.getName(),
                    vertex.getOperatorIDs().stream()
                            .map(pair -> pair.getGeneratedOperatorID().toString())
                            .collect(Collectors.joining(",")));
        }
        return ids;
    }

    private static final class EmptyCommitter implements Committer<CommitWrapper<String>> {
        public void commit(Collection<Committer.CommitRequest<CommitWrapper<String>>> requests) {}

        public void close() {}
    }

    private static final class TestSerializer
            implements SimpleVersionedSerializer<CommitWrapper<String>> {
        public int getVersion() {
            return 0;
        }

        public byte[] serialize(CommitWrapper<String> value) {
            return new byte[0];
        }

        public CommitWrapper<String> deserialize(int version, byte[] bytes) {
            return new CommitWrapper<>("prepared");
        }
    }
}
