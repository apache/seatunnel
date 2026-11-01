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

package org.apache.seatunnel.connectors.seatunnel.kafka.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class KafkaSinkWriterTest {

    private static final String PREFIX = "shared-prefix-7";
    private static final String GENERATION = "gtest";

    @Test
    void shouldKeepParallelWritersIsolatedAcrossCheckpoints() {
        // SeaTunnel-13044: a shared configured prefix must not let parallel writers fence each
        // other.
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(new ArrayList<>());
                TestWriter first = writer(0, Collections.emptyList());
                TestWriter second = writer(1, Collections.emptyList())) {
            first.write(new SeaTunnelRow(new Object[] {"first"}));
            second.write(new SeaTunnelRow(new Object[] {"second"}));
            List<String> ids = new ArrayList<>();
            ids.add(first.prepareCommit().get().getTransactionId());
            ids.add(second.prepareCommit().get().getTransactionId());

            List<KafkaSinkState> firstState = first.snapshotState(1);
            List<KafkaSinkState> secondState = second.snapshotState(1);
            ids.add(first.prepareCommit().get().getTransactionId());
            ids.add(second.prepareCommit().get().getTransactionId());

            Assertions.assertEquals(
                    4,
                    new HashSet<>(ids).size(),
                    "Every writer/checkpoint needs its own transaction");
            Assertions.assertEquals(PREFIX, firstState.get(0).getTransactionIdPrefix());
            Assertions.assertEquals(PREFIX, secondState.get(0).getTransactionIdPrefix());
        }
    }

    @Test
    void shouldFenceLegacyTransactionsBeforeStartingWriterScopedTransactions() {
        List<String> initializedIds = new ArrayList<>();
        try (MockedConstruction<KafkaInternalProducer> producers = mockProducers(initializedIds);
                TestWriter writer = writer(2, legacyState())) {
            String currentId = writer.prepareCommit().get().getTransactionId();

            Assertions.assertTrue(
                    initializedIds.contains(PREFIX + "-5"),
                    "Restore must fence transactions from the old ID scheme");
            Assertions.assertTrue(
                    initializedIds.contains(PREFIX + "-6"),
                    "Legacy cleanup must continue until an unused ID");
            Assertions.assertNotEquals(
                    PREFIX + "-5",
                    currentId,
                    "New transactions must leave the legacy shared namespace");
            Assertions.assertFalse(
                    initializedIds.stream()
                            .anyMatch(id -> id.startsWith(freshWriterPrefix(PREFIX, 1) + "-")),
                    "Restore must not fence another current writer");
        }
    }

    @Test
    void shouldIsolateWritersRestoredFromLegacyState() {
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(new ArrayList<>());
                TestWriter first = writer(0, legacyState());
                TestWriter second = writer(1, legacyState())) {
            Assertions.assertNotEquals(
                    first.prepareCommit().get().getTransactionId(),
                    second.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldKeepLegacyPrefixWhenConfiguredPrefixWasOmitted() {
        try (MockedConstruction<KafkaInternalProducer> producers =
                mockProducers(new ArrayList<>())) {
            List<KafkaSinkState> migratedState;
            try (TestWriter migrated = writerWithoutConfiguredPrefix(2, 3, legacyState())) {
                migratedState = migrated.snapshotState(5);
                Assertions.assertEquals(PREFIX, migratedState.get(0).getTransactionIdPrefix());
                Assertions.assertEquals(
                        freshWriterPrefix(PREFIX, 2) + "-1",
                        migratedState.get(0).getTransactionId());
            }

            try (TestWriter restored = writerWithoutConfiguredPrefix(2, 3, migratedState)) {
                Assertions.assertEquals(
                        freshWriterPrefix(PREFIX, 2) + "-2",
                        restored.prepareCommit().orElseThrow().getTransactionId());
            }
        }
    }

    @Test
    void shouldFenceContiguouslyAssignedFlinkNamespacesAfterScaleDown() {
        List<String> initializedIds = new ArrayList<>();
        String firstWriterPrefix = freshWriterPrefix(PREFIX, 0);
        String secondWriterPrefix = freshWriterPrefix(PREFIX, 1);
        Set<String> unfinishedIds = Set.of(firstWriterPrefix + "-5", secondWriterPrefix + "-5");
        List<KafkaSinkState> states =
                List.of(
                        new KafkaSinkState(firstWriterPrefix + "-4", PREFIX, 4, new Properties()),
                        new KafkaSinkState(secondWriterPrefix + "-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(0, 2, states)) {
            Assertions.assertTrue(
                    initializedIds.containsAll(unfinishedIds),
                    "A Flink 3-to-2 restore must fence both contiguously assigned namespaces");
            Assertions.assertEquals(
                    firstWriterPrefix + "-5", writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldRejectMalformedRestoredWriterNamespace() {
        List<KafkaSinkState> states =
                Collections.singletonList(
                        new KafkaSinkState(PREFIX + "-writer-4", PREFIX, 4, new Properties()));

        IllegalStateException exception =
                Assertions.assertThrows(IllegalStateException.class, () -> writer(0, 2, states));

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Cannot determine the writer owner of Kafka transaction prefix"));
    }

    @Test
    void shouldFenceEveryAssignedWriterNamespaceAfterScaleDown() {
        List<String> initializedIds = new ArrayList<>();
        String firstWriterPrefix = freshWriterPrefix(PREFIX, 0);
        String thirdWriterPrefix = freshWriterPrefix(PREFIX, 2);
        Set<String> unfinishedIds = Set.of(firstWriterPrefix + "-5", thirdWriterPrefix + "-5");
        List<KafkaSinkState> states =
                List.of(
                        new KafkaSinkState(firstWriterPrefix + "-4", PREFIX, 4, new Properties()),
                        new KafkaSinkState(thirdWriterPrefix + "-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(0, 2, states)) {
            Assertions.assertTrue(
                    initializedIds.containsAll(unfinishedIds),
                    "A 3-to-2 restore must fence both namespaces assigned to writer zero");
            Assertions.assertTrue(initializedIds.contains(firstWriterPrefix + "-6"));
            Assertions.assertTrue(initializedIds.contains(thirdWriterPrefix + "-6"));
            Assertions.assertEquals(
                    firstWriterPrefix + "-5", writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldRestoreMixedGeneratedPrefixesAfterScaleDown() {
        String firstPrefix = "SeaTunnel0001";
        String secondPrefix = "SeaTunnel0002";
        String firstWriterPrefix = freshWriterPrefix(firstPrefix, 0);
        String thirdWriterPrefix = freshWriterPrefix(secondPrefix, 2);
        List<String> initializedIds = new ArrayList<>();
        Set<String> unfinishedIds = Set.of(firstWriterPrefix + "-5", thirdWriterPrefix + "-5");
        List<KafkaSinkState> states =
                List.of(
                        new KafkaSinkState(
                                firstWriterPrefix + "-4", firstPrefix, 4, new Properties()),
                        new KafkaSinkState(
                                thirdWriterPrefix + "-4", secondPrefix, 4, new Properties()));

        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writerWithoutConfiguredPrefix(0, 2, states)) {
            Assertions.assertTrue(initializedIds.containsAll(unfinishedIds));
            Assertions.assertEquals(
                    firstWriterPrefix + "-5", writer.prepareCommit().get().getTransactionId());
            Assertions.assertEquals(
                    firstPrefix, writer.snapshotState(5).get(0).getTransactionIdPrefix());
        }
    }

    @Test
    void shouldRelocateSameGenerationNamespaceBeforeScaleUp() {
        List<String> initializedIds = new ArrayList<>();
        String reassignedWriterPrefix = freshWriterPrefix(PREFIX, 2);
        String relocatedWriterPrefix = reassignedWriterPrefix + "r1c4";
        List<KafkaSinkState> adoptedState =
                Collections.singletonList(
                        new KafkaSinkState(
                                reassignedWriterPrefix + "-4", PREFIX, 4, new Properties()));

        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(
                                initializedIds,
                                Collections.singleton(relocatedWriterPrefix + "-1"));
                TestWriter adoptedWriter = writer(1, 2, adoptedState)) {
            String adoptedTransactionId =
                    adoptedWriter.prepareCommit().orElseThrow().getTransactionId();
            Assertions.assertTrue(
                    initializedIds.contains(relocatedWriterPrefix + "-1"),
                    "A retry must fence its deterministic relocated namespace");
            Assertions.assertEquals(relocatedWriterPrefix + "-1", adoptedTransactionId);
            initializedIds.clear();

            try (TestWriter freshWriter = writer(2, 3, Collections.emptyList())) {
                String freshTransactionId =
                        freshWriter.prepareCommit().orElseThrow().getTransactionId();

                Assertions.assertNotEquals(
                        transactionIdPrefix(adoptedTransactionId),
                        transactionIdPrefix(freshTransactionId),
                        "A stateless writer must not reuse a namespace adopted after scale-down");
                Assertions.assertFalse(
                        initializedIds.stream()
                                .anyMatch(id -> id.startsWith(relocatedWriterPrefix + "-")),
                        "Fresh-writer cleanup must not fence the relocated live namespace");
            }
        }
    }

    @Test
    void shouldAvoidRelocationCollisionAcrossRepeatedFlinkRescaling() {
        List<String> initializedIds = new ArrayList<>();
        String firstWriterPrefix = freshWriterPrefix(PREFIX, 0);
        String reassignedWriterPrefix = freshWriterPrefix(PREFIX, 2);
        String firstRelocatedWriterPrefix = reassignedWriterPrefix + "r1c4";
        String secondRelocatedWriterPrefix = reassignedWriterPrefix + "r1c6";

        List<KafkaSinkState> firstScaleDownState =
                Collections.singletonList(
                        new KafkaSinkState(
                                reassignedWriterPrefix + "-4", PREFIX, 4, new Properties()));
        List<KafkaSinkState> firstRelocatedState;
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, Collections.emptySet());
                TestWriter writer = writer(1, 2, firstScaleDownState)) {
            Assertions.assertEquals(
                    firstRelocatedWriterPrefix + "-1",
                    writer.prepareCommit().orElseThrow().getTransactionId());
            firstRelocatedState = writer.snapshotState(5);
        }

        List<KafkaSinkState> secondScaleDownRelocatedState;
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, Collections.emptySet());
                TestWriter writer = writer(1, 3, firstRelocatedState)) {
            secondScaleDownRelocatedState = writer.snapshotState(6);
        }

        List<KafkaSinkState> baseState;
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, Collections.emptySet());
                TestWriter writer = writer(2, 3, Collections.emptyList())) {
            baseState = writer.snapshotState(6);
        }

        List<KafkaSinkState> firstFlinkAssignment =
                List.of(
                        new KafkaSinkState(firstWriterPrefix + "-6", PREFIX, 6, new Properties()),
                        secondScaleDownRelocatedState.get(0));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, Collections.emptySet());
                TestWriter writer = writer(0, 2, firstFlinkAssignment)) {
            Assertions.assertEquals(
                    firstWriterPrefix + "-7",
                    writer.prepareCommit().orElseThrow().getTransactionId());
        }

        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, Collections.emptySet());
                TestWriter writer = writer(1, 2, baseState)) {
            String transactionId = writer.prepareCommit().orElseThrow().getTransactionId();
            Assertions.assertEquals(secondRelocatedWriterPrefix + "-1", transactionId);
            Assertions.assertNotEquals(
                    firstRelocatedWriterPrefix,
                    transactionIdPrefix(transactionId),
                    "A later Flink scale-down must not recreate a sibling's relocation namespace");
        }
    }

    @Test
    void shouldKeepTransactionSequenceContiguousWhenCheckpointIdsJump() {
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(new ArrayList<>(), Collections.emptySet());
                TestWriter writer = writer(0, Collections.emptyList())) {
            List<KafkaSinkState> state = writer.snapshotState(9);

            Assertions.assertEquals(
                    freshWriterPrefix(PREFIX, 0) + "-1", state.get(0).getTransactionId());
            Assertions.assertEquals(
                    freshWriterPrefix(PREFIX, 0) + "-2",
                    writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldFenceEveryStatelessWriterTransactionAfterScaleUpRetry() {
        List<String> initializedIds = new ArrayList<>();
        String writerPrefix = freshWriterPrefix(PREFIX, 2);
        Set<String> unfinishedIds = Set.of(writerPrefix + "-1", writerPrefix + "-2");

        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(2, 3, Collections.emptyList())) {
            Assertions.assertTrue(initializedIds.containsAll(unfinishedIds));
            Assertions.assertTrue(
                    initializedIds.contains(writerPrefix + "-3"),
                    "Recovery must probe the first unused contiguous transaction ID");
            Assertions.assertEquals(
                    writerPrefix + "-1", writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldReuseGeneratedPrefixWhenStatelessWriterRetries() throws Exception {
        ReadonlyConfig config = config(false);
        SeaTunnelRowType rowType = rowType();
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable("test", "test", "test", "test", rowType);
        KafkaSink sink = new KafkaSink(config, catalogTable);
        SinkWriter.Context context = context(2, 3);
        String abandonedTransactionId;

        try (MockedConstruction<KafkaInternalProducer> producers =
                mockProducers(new ArrayList<>(), Collections.emptySet())) {
            KafkaSinkWriter firstAttempt = (KafkaSinkWriter) sink.createWriter(context);
            try {
                abandonedTransactionId =
                        firstAttempt.prepareCommit().orElseThrow().getTransactionId();
            } finally {
                firstAttempt.close();
            }
        }

        DefaultSerializer<KafkaSink> sinkSerializer = new DefaultSerializer<>();
        KafkaSink retrySink = sinkSerializer.deserialize(sinkSerializer.serialize(sink));
        List<String> initializedIds = new ArrayList<>();
        try (MockedConstruction<KafkaInternalProducer> producers =
                mockProducers(initializedIds, Collections.singleton(abandonedTransactionId))) {
            KafkaSinkWriter retry = (KafkaSinkWriter) retrySink.createWriter(context);
            try {
                Assertions.assertTrue(
                        initializedIds.contains(abandonedTransactionId),
                        "A retry must fence the abandoned generated-prefix transaction");
                Assertions.assertEquals(
                        abandonedTransactionId,
                        retry.prepareCommit().orElseThrow().getTransactionId());
            } finally {
                retry.close();
            }
        }
    }

    @Test
    void shouldFenceAllUnfinishedTransactionsInItsOwnNamespace() {
        List<String> initializedIds = new ArrayList<>();
        String writerPrefix = freshWriterPrefix(PREFIX, 2);
        Set<String> unfinishedIds = Set.of(writerPrefix + "-5", writerPrefix + "-6");
        List<KafkaSinkState> state =
                Collections.singletonList(
                        new KafkaSinkState(writerPrefix + "-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(2, state)) {
            Assertions.assertTrue(
                    initializedIds.containsAll(unfinishedIds),
                    "Recovery must fence every unfinished transaction in the writer namespace");
            Assertions.assertTrue(
                    initializedIds.contains(writerPrefix + "-7"),
                    "Recovery must probe the next unused transaction before it stops");
            Assertions.assertEquals(
                    writerPrefix + "-5", writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldReuseTheWriterNamespaceAfterRestore() {
        try (MockedConstruction<KafkaInternalProducer> producers =
                mockProducers(new ArrayList<>())) {
            List<KafkaSinkState> state;
            String expectedNextId;
            try (TestWriter original = writer(2, Collections.emptyList())) {
                state = original.snapshotState(3);
                expectedNextId = original.prepareCommit().get().getTransactionId();
            }

            try (TestWriter restored = writer(2, state)) {
                Assertions.assertEquals(
                        expectedNextId,
                        restored.prepareCommit().get().getTransactionId(),
                        "Restore must not append the subtask index again");
                Assertions.assertEquals(
                        PREFIX, restored.snapshotState(4).get(0).getTransactionIdPrefix());
            }
        }
    }

    private static List<KafkaSinkState> legacyState() {
        return Collections.singletonList(
                new KafkaSinkState(PREFIX + "-4", PREFIX, 4, new Properties()));
    }

    private static TestWriter writer(int index, List<KafkaSinkState> state) {
        return writer(index, Math.max(index + 1, 1), state);
    }

    private static TestWriter writer(int index, int parallelism, List<KafkaSinkState> state) {
        return writer(index, parallelism, state, true);
    }

    private static TestWriter writerWithoutConfiguredPrefix(
            int index, int parallelism, List<KafkaSinkState> state) {
        return writer(index, parallelism, state, false);
    }

    private static TestWriter writer(
            int index,
            int parallelism,
            List<KafkaSinkState> state,
            boolean configureTransactionPrefix) {
        SinkWriter.Context context = context(index, parallelism);
        ReadonlyConfig config = config(configureTransactionPrefix);
        return new TestWriter(
                context,
                rowType(),
                config,
                state,
                configureTransactionPrefix ? PREFIX : "SeaTunnel9999",
                GENERATION);
    }

    private static SinkWriter.Context context(int index, int parallelism) {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(index);
        when(context.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        return context;
    }

    private static ReadonlyConfig config(boolean configureTransactionPrefix) {
        Map<String, Object> configValues = new HashMap<>();
        configValues.put("bootstrap.servers", "unused:9092");
        configValues.put("topic", "test");
        configValues.put("format", "text");
        configValues.put("semantics", "EXACTLY_ONCE");
        if (configureTransactionPrefix) {
            configValues.put("transaction_prefix", PREFIX);
        }
        return ReadonlyConfig.fromMap(configValues);
    }

    private static SeaTunnelRowType rowType() {
        return new SeaTunnelRowType(
                new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    private static String freshWriterPrefix(String transactionPrefix, int writerIndex) {
        return transactionPrefix + "-" + GENERATION + "w" + writerIndex;
    }

    private static String transactionIdPrefix(String transactionId) {
        return transactionId.substring(0, transactionId.lastIndexOf('-'));
    }

    /** Adapt the writer's close lifecycle to try-with-resources without changing its SPI. */
    private static final class TestWriter extends KafkaSinkWriter implements AutoCloseable {
        private TestWriter(
                SinkWriter.Context context,
                SeaTunnelRowType rowType,
                ReadonlyConfig config,
                List<KafkaSinkState> state,
                String transactionPrefix,
                String writerNamespaceGeneration) {
            super(context, rowType, config, state, transactionPrefix, writerNamespaceGeneration);
        }
    }

    /** Replace only the external Kafka client; execute the real writer and transaction sender. */
    private static MockedConstruction<KafkaInternalProducer> mockProducers(
            List<String> initializedIds) {
        return mockProducers(initializedIds, Collections.singleton(PREFIX + "-5"));
    }

    private static MockedConstruction<KafkaInternalProducer> mockProducers(
            List<String> initializedIds, Set<String> unfinishedIds) {
        return mockConstruction(
                KafkaInternalProducer.class,
                (producer, context) -> {
                    String transactionId = (String) context.arguments().get(1);
                    doAnswer(
                                    invocation -> {
                                        initializedIds.add(transactionId);
                                        return null;
                                    })
                            .when(producer)
                            .initTransactions();
                    when(producer.getEpoch())
                            .thenReturn(
                                    unfinishedIds.contains(transactionId) ? (short) 1 : (short) 0);
                    when(producer.isTxnStarted()).thenReturn(true);
                });
    }
}
