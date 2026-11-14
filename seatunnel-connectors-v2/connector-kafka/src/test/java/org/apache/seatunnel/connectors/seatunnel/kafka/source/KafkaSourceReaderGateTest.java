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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceGateState;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class KafkaSourceReaderGateTest {

    @Test
    void restoreGateStateShouldDeferActivationUntilOpen() throws Exception {
        KafkaSourceReader reader = newReader(true);
        @SuppressWarnings("unchecked")
        SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>
                fetcherManager =
                        (SingleThreadFetcherManager<
                                        ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>)
                                getField(reader, "splitFetcherManager");
        KafkaSourceSplit split = split(10L);

        reader.restoreGateState(
                new SourceGateState(true, false, Collections.singletonList(preparedSplit(split))));

        Mockito.verify(fetcherManager, Mockito.never()).addSplits(Mockito.anyList());

        reader.open();

        Mockito.verify(fetcherManager)
                .addSplits(
                        Mockito.argThat(
                                (ArgumentMatcher<java.util.List<KafkaSourceSplit>>)
                                        splits ->
                                                splits.size() == 1
                                                        && splits.get(0).getStartOffset() == 10L));
    }

    @Test
    void snapshotStateShouldNotPopulateCheckpointOffsetsWhenGateClosed() throws Exception {
        KafkaSourceReader reader = newReader(true);
        reader.prepareClosedGate();
        reader.addSplits(Collections.singletonList(split(15L)));

        Assertions.assertEquals(1, reader.snapshotState(1L).size());
        Assertions.assertTrue(checkpointOffsetMap(reader).isEmpty());
    }

    @Test
    void stagedSplitsShouldBeDeduplicatedBySplitId() throws Exception {
        KafkaSourceReader reader = newReader(false);
        reader.prepareClosedGate();
        reader.addSplits(Arrays.asList(split(10L), split(20L)));

        SourceGateState gateState = reader.snapshotGate(1L);

        Assertions.assertEquals(1, gateState.getPreparedSplits().size());
        KafkaSourceSplit restoredSplit =
                KafkaSourceReader.deserializeSplit(
                        gateState.getPreparedSplits().get(0).getSerializedSplit());
        Assertions.assertEquals(20L, restoredSplit.getStartOffset());
    }

    @Test
    void snapshotGateAfterOpenShouldPreserveActivatedSplitsAndNoMoreSplits() throws Exception {
        KafkaSourceReader reader = newReader(false);
        reader.prepareClosedGate();
        reader.addSplits(Collections.singletonList(split(30L)));
        reader.handleNoMoreSplits();

        reader.applyGateCommand(org.apache.seatunnel.api.source.SourceGateCommand.OPEN);
        SourceGateState gateState = reader.snapshotGate(2L);

        Assertions.assertTrue(gateState.isGateOpen());
        Assertions.assertTrue(gateState.isNoMoreSplits());
        Assertions.assertEquals(1, gateState.getPreparedSplits().size());
        KafkaSourceSplit restoredSplit =
                KafkaSourceReader.deserializeSplit(
                        gateState.getPreparedSplits().get(0).getSerializedSplit());
        Assertions.assertEquals(30L, restoredSplit.getStartOffset());
    }

    /**
     * Verifies the production allowlist in {@code KafkaSourceReader.deserializeSplit}/{@code
     * KafkaGateObjectInputStream.resolveClass}.
     *
     * <p>Every staging round-trip also calls this production decoder, so widening or bypassing its
     * allowlist cannot be hidden by a plain test-side deserialization helper.
     */
    @Test
    void productionDeserializeSplitShouldRoundTripKafkaSourceSplit() throws Exception {
        KafkaSourceSplit split = split(42L);
        KafkaSourceSplit restored = KafkaSourceReader.deserializeSplit(serializePayload(split));

        Assertions.assertEquals(split.getTablePath(), restored.getTablePath());
        Assertions.assertEquals(split.getTopicPartition(), restored.getTopicPartition());
        Assertions.assertEquals(split.getStartOffset(), restored.getStartOffset());
        Assertions.assertEquals(split.getEndOffset(), restored.getEndOffset());
    }

    @Test
    void productionDeserializeSplitShouldRejectClassOutsideAllowlist() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(output)) {
            objectOutputStream.writeObject(new HashMap<String, String>());
        }
        byte[] disallowedPayload = output.toByteArray();

        IOException thrown =
                Assertions.assertThrows(
                        IOException.class,
                        () -> KafkaSourceReader.deserializeSplit(disallowedPayload));

        Assertions.assertEquals(
                "Rejected Kafka source gate split class: java.util.HashMap", thrown.getMessage());
    }

    /**
     * Distinguishes exact-name matching from prefix matching. {@link KafkaSourceSplitState} is a
     * serializable subclass of the allowlisted {@link KafkaSourceSplit}, lives in the same package,
     * and its class name even starts with the allowlisted name, so it passes any package-prefix or
     * class-name-prefix check and would also satisfy the {@code instanceof KafkaSourceSplit} guard.
     * Only the exact-name allowlist rejects it.
     */
    @Test
    void productionDeserializeSplitShouldRejectSamePackageSubclassOutsideAllowlist()
            throws Exception {
        byte[] lookalikePayload = serializePayload(new KafkaSourceSplitState(split(42L)));

        IOException thrown =
                Assertions.assertThrows(
                        IOException.class,
                        () -> KafkaSourceReader.deserializeSplit(lookalikePayload));

        Assertions.assertEquals(
                "Rejected Kafka source gate split class: " + KafkaSourceSplitState.class.getName(),
                thrown.getMessage());
    }

    @Test
    void productionDeserializeSplitShouldRejectArraysOutsideAllowlist() throws Exception {
        // Empty arrays prove rejection of the descriptor itself, without visiting any elements.
        // Even arrays of allowlisted component classes are outside this narrow Kafka state graph.
        for (Object array :
                new Object[] {
                    new HashMap<?, ?>[0], new HashMap<?, ?>[0][0], new String[0], new String[0][0]
                }) {
            byte[] payload = serializePayload(array);
            IOException thrown =
                    Assertions.assertThrows(
                            IOException.class, () -> KafkaSourceReader.deserializeSplit(payload));

            Assertions.assertEquals(
                    "Rejected Kafka source gate split class: " + array.getClass().getName(),
                    thrown.getMessage());
        }
    }

    @Test
    void productionDeserializeSplitShouldRejectProxyClassesBeforeTheirHandler() throws Exception {
        Object proxy =
                Proxy.newProxyInstance(
                        KafkaSourceReaderGateTest.class.getClassLoader(),
                        new Class<?>[] {Serializable.class},
                        new SerializableProxyHandler());
        byte[] payload = serializePayload(proxy);

        IOException thrown =
                Assertions.assertThrows(
                        IOException.class, () -> KafkaSourceReader.deserializeSplit(payload));

        Assertions.assertEquals(
                "Kafka source gate split state must not contain proxy classes",
                thrown.getMessage());
    }

    private static KafkaSourceReader newReader(boolean commitOnCheckpoint) {
        BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue =
                new ArrayBlockingQueue<>(4);
        @SuppressWarnings("unchecked")
        SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>
                fetcherManager = Mockito.mock(SingleThreadFetcherManager.class);
        @SuppressWarnings("unchecked")
        RecordEmitter<ConsumerRecord<byte[], byte[]>, SeaTunnelRow, KafkaSourceSplitState>
                recordEmitter = Mockito.mock(RecordEmitter.class);
        KafkaSourceConfig sourceConfig = Mockito.mock(KafkaSourceConfig.class);
        Mockito.when(sourceConfig.isCommitOnCheckpoint()).thenReturn(commitOnCheckpoint);
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getIndexOfSubtask()).thenReturn(0);
        return new KafkaSourceReader(
                elementsQueue,
                fetcherManager,
                recordEmitter,
                new SourceReaderOptions(ReadonlyConfig.fromMap(Collections.emptyMap())),
                sourceConfig,
                context);
    }

    private static KafkaSourceSplit split(long startOffset) {
        return new KafkaSourceSplit(
                TablePath.DEFAULT, new TopicPartition("topic", 0), startOffset, startOffset + 1);
    }

    private static SourceGateState.PreparedSplit preparedSplit(KafkaSourceSplit split)
            throws Exception {
        byte[] serializedSplit = serializePayload(split);
        return new SourceGateState.PreparedSplit(
                split.splitId(),
                serializedSplit,
                MessageDigest.getInstance("SHA-256").digest(serializedSplit));
    }

    private static byte[] serializePayload(Object value) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(output)) {
            objectOutputStream.writeObject(value);
        }
        return output.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private static SortedMap<Long, ?> checkpointOffsetMap(KafkaSourceReader reader)
            throws Exception {
        return (SortedMap<Long, ?>) getField(reader, "checkpointOffsetMap");
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Class<?> type = target.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /**
     * Supplies a serializable proxy payload without introducing any deserialization side effects.
     */
    private static final class SerializableProxyHandler implements InvocationHandler, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    }
}
