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
import org.apache.seatunnel.api.sink.SinkWriter;
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
                    initializedIds.stream().anyMatch(id -> id.startsWith(PREFIX + "-1-")),
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
    void shouldRejectStateAssignedToTheWrongWriter() {
        List<String> initializedIds = new ArrayList<>();
        List<KafkaSinkState> state =
                Collections.singletonList(
                        new KafkaSinkState(PREFIX + "-1-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers = mockProducers(initializedIds)) {
            IllegalStateException exception =
                    Assertions.assertThrows(IllegalStateException.class, () -> writer(2, 3, state));
            Assertions.assertTrue(
                    exception.getMessage().contains("does not own that restored state"));
            Assertions.assertTrue(
                    initializedIds.isEmpty(),
                    "Ownership must be validated before any transaction is fenced or started");
        }
    }

    @Test
    void shouldFenceEveryAssignedWriterNamespaceAfterScaleDown() {
        List<String> initializedIds = new ArrayList<>();
        Set<String> unfinishedIds = Set.of(PREFIX + "-0-5", PREFIX + "-2-5");
        List<KafkaSinkState> states =
                List.of(
                        new KafkaSinkState(PREFIX + "-0-4", PREFIX, 4, new Properties()),
                        new KafkaSinkState(PREFIX + "-2-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(0, 2, states)) {
            Assertions.assertTrue(
                    initializedIds.containsAll(unfinishedIds),
                    "A 3-to-2 restore must fence both namespaces assigned to writer zero");
            Assertions.assertTrue(initializedIds.contains(PREFIX + "-0-6"));
            Assertions.assertTrue(initializedIds.contains(PREFIX + "-2-6"));
            Assertions.assertEquals(
                    PREFIX + "-0-5", writer.prepareCommit().get().getTransactionId());
        }
    }

    @Test
    void shouldFenceAllUnfinishedTransactionsInItsOwnNamespace() {
        List<String> initializedIds = new ArrayList<>();
        Set<String> unfinishedIds = Set.of(PREFIX + "-2-5", PREFIX + "-2-6");
        List<KafkaSinkState> state =
                Collections.singletonList(
                        new KafkaSinkState(PREFIX + "-2-4", PREFIX, 4, new Properties()));
        try (MockedConstruction<KafkaInternalProducer> producers =
                        mockProducers(initializedIds, unfinishedIds);
                TestWriter writer = writer(2, state)) {
            Assertions.assertTrue(
                    initializedIds.containsAll(unfinishedIds),
                    "Recovery must fence every unfinished transaction in the writer namespace");
            Assertions.assertTrue(
                    initializedIds.contains(PREFIX + "-2-7"),
                    "Recovery must probe the next unused transaction before it stops");
            Assertions.assertEquals(
                    PREFIX + "-2-5", writer.prepareCommit().get().getTransactionId());
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
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(index);
        when(context.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Map.of(
                                "bootstrap.servers",
                                "unused:9092",
                                "topic",
                                "test",
                                "format",
                                "text",
                                "semantics",
                                "EXACTLY_ONCE",
                                "transaction_prefix",
                                PREFIX));
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        return new TestWriter(context, rowType, config, state);
    }

    /** Adapt the writer's close lifecycle to try-with-resources without changing its SPI. */
    private static final class TestWriter extends KafkaSinkWriter implements AutoCloseable {
        private TestWriter(
                SinkWriter.Context context,
                SeaTunnelRowType rowType,
                ReadonlyConfig config,
                List<KafkaSinkState> state) {
            super(context, rowType, config, state);
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
