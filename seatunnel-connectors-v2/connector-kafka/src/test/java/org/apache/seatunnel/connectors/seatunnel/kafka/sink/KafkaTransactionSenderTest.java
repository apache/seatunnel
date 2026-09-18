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

import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaTransactionSenderTest {

    private static final String TRANSACTION_PREFIX = "SeaTunnel0001";
    private static final String TRANSACTION_ID = TRANSACTION_PREFIX + "-1";
    private static final String NEXT_TRANSACTION_ID = TRANSACTION_PREFIX + "-2";
    private static final String TOPIC = "test-topic";
    private static final long PRODUCER_ID = 1001L;
    private static final short EPOCH = 5;

    /**
     * Reproduces the reported race: Kafka reports the transaction as not started until the pending
     * AddPartitionsToTxn request has been acknowledged, which flush() forces. prepareCommit() must
     * flush first so the captured commit info carries txnStarted=true and the committer performs
     * EndTxn.
     */
    @Test
    void prepareCommitFlushesBeforeCapturingTransactionState() {
        ProducerStub producer = new ProducerStub();
        producer.startTransactionOnFlush();

        TestingKafkaTransactionSender sender = createSender(producer);
        sender.beginTransaction(TRANSACTION_ID);
        sender.send(record());

        Optional<KafkaCommitInfo> commitInfo = sender.prepareCommit();

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertTrue(
                commitInfo.get().isTxnStarted(),
                "txnStarted must be captured after flush so the committer performs EndTxn");
        Assertions.assertEquals(TRANSACTION_ID, commitInfo.get().getTransactionId());
        Assertions.assertEquals(PRODUCER_ID, commitInfo.get().getProducerId());
        Assertions.assertEquals(EPOCH, commitInfo.get().getEpoch());
        verify(producer.mock, times(1)).flush();
    }

    /**
     * When the transaction genuinely carries records but Kafka still reports it as not started even
     * after flushing, prepareCommit() must fail fast rather than emit a commit info that instructs
     * the committer to skip EndTxn and silently drop those records.
     */
    @Test
    void prepareCommitFailsWhenRecordsSentButTransactionNotStarted() {
        ProducerStub producer = new ProducerStub();

        TestingKafkaTransactionSender sender = createSender(producer);
        sender.beginTransaction(TRANSACTION_ID);
        sender.send(record());
        sender.send(record());

        KafkaConnectorException exception =
                Assertions.assertThrows(KafkaConnectorException.class, sender::prepareCommit);

        Assertions.assertEquals(
                KafkaConnectorErrorCode.TRANSACTION_NOT_STARTED.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        verify(producer.mock, times(1)).flush();
    }

    /**
     * A transaction can already be marked as started while a record still fails to be sent. The
     * failure is only reported once flush() completes the pending send, so prepareCommit() must
     * check for it after flushing instead of committing the remaining successful records.
     */
    @Test
    void prepareCommitFailsWhenAsyncSendFailsAfterTransactionStarted() {
        ProducerStub producer = new ProducerStub();
        producer.transactionStarted();
        RuntimeException asyncSendFailure = new RuntimeException("async send failed");
        producer.failPendingSendsWith(asyncSendFailure);

        TestingKafkaTransactionSender sender = createSender(producer);
        sender.beginTransaction(TRANSACTION_ID);
        sender.send(record());

        KafkaConnectorException exception =
                Assertions.assertThrows(KafkaConnectorException.class, sender::prepareCommit);

        Assertions.assertEquals(
                KafkaConnectorErrorCode.PRODUCE_DATA_FAILED.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertSame(asyncSendFailure, exception.getCause());
        verify(producer.mock, times(1)).flush();
    }

    /**
     * An empty transaction (no records sent) legitimately reports txnStarted=false and must not be
     * treated as an error; the commit info simply carries txnStarted=false.
     */
    @Test
    void prepareCommitAllowsEmptyTransactionWithoutRecords() {
        ProducerStub producer = new ProducerStub();

        TestingKafkaTransactionSender sender = createSender(producer);
        sender.beginTransaction(TRANSACTION_ID);

        Optional<KafkaCommitInfo> commitInfo = sender.prepareCommit();

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertFalse(commitInfo.get().isTxnStarted());
        verify(producer.mock, times(1)).flush();
    }

    /**
     * Once a failure has been recorded the current transaction can no longer be committed, so the
     * write path must reject further records instead of buffering them until the next checkpoint.
     */
    @Test
    void sendFailsFastAfterAsyncSendFailureIsRecorded() {
        ProducerStub producer = new ProducerStub();

        TestingKafkaTransactionSender sender = createSender(producer);
        sender.beginTransaction(TRANSACTION_ID);
        sender.send(record());
        producer.completePendingSends(new RuntimeException("async send failed"));

        KafkaConnectorException exception =
                Assertions.assertThrows(KafkaConnectorException.class, () -> sender.send(record()));

        Assertions.assertEquals(
                KafkaConnectorErrorCode.PRODUCE_DATA_FAILED.getCode(),
                exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertEquals(
                1, producer.sendCount, "no further record may be handed to the producer");
    }

    /**
     * An asynchronous send failure is scoped to the transaction that produced it. After the engine
     * aborts that transaction and opens a new one on the same sender, checkpoints must succeed
     * again; otherwise one transient broker error would block every later checkpoint.
     *
     * <p>The recovered transaction is deliberately empty and reports txnStarted=false, so it also
     * proves the record counter was reset: a stale counter would raise TRANSACTION_NOT_STARTED.
     */
    @Test
    void senderRecoversAfterFailedTransaction() {
        ProducerStub failingProducer = new ProducerStub();
        failingProducer.transactionStarted();
        failingProducer.failPendingSendsWith(new RuntimeException("async send failed"));
        ProducerStub healthyProducer = new ProducerStub();

        TestingKafkaTransactionSender sender = createSender(failingProducer, healthyProducer);
        sender.beginTransaction(TRANSACTION_ID);
        sender.send(record());
        Assertions.assertThrows(KafkaConnectorException.class, sender::prepareCommit);
        sender.abortTransaction();

        sender.beginTransaction(NEXT_TRANSACTION_ID);

        Optional<KafkaCommitInfo> commitInfo = Assertions.assertDoesNotThrow(sender::prepareCommit);
        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(NEXT_TRANSACTION_ID, commitInfo.get().getTransactionId());
        Assertions.assertFalse(commitInfo.get().isTxnStarted());
    }

    /**
     * Each transactional ID must be fenced by its own producer: changing the ID on a reused
     * producer can retain a non-zero epoch indefinitely and spin the cleanup loop forever.
     */
    @Test
    void abortTransactionUsesFreshProducerForEachTransactionalId() {
        KafkaInternalProducer<byte[], byte[]> existingTransaction =
                Mockito.mock(KafkaInternalProducer.class);
        KafkaInternalProducer<byte[], byte[]> unusedTransaction =
                Mockito.mock(KafkaInternalProducer.class);
        Mockito.when(existingTransaction.getEpoch()).thenReturn((short) 1);
        Mockito.when(unusedTransaction.getEpoch()).thenReturn((short) 0);

        TestingKafkaTransactionSender sender =
                new TestingKafkaTransactionSender(existingTransaction, unusedTransaction);

        sender.abortTransaction(7L);

        Assertions.assertEquals(
                Arrays.asList(TRANSACTION_PREFIX + "-7", TRANSACTION_PREFIX + "-8"),
                sender.createdTransactionIds);
        Mockito.verify(existingTransaction).close(Duration.ZERO);
        Mockito.verify(unusedTransaction).close(Duration.ZERO);
    }

    private static ProducerRecord<byte[], byte[]> record() {
        return new ProducerRecord<>(TOPIC, new byte[] {1});
    }

    /**
     * Builds a sender that hands out the given producer stubs, one per {@code beginTransaction}.
     */
    private TestingKafkaTransactionSender createSender(ProducerStub... producers) {
        KafkaInternalProducer<byte[], byte[]>[] mocks = new KafkaInternalProducer[producers.length];
        for (int i = 0; i < producers.length; i++) {
            mocks[i] = producers[i].mock;
        }
        return new TestingKafkaTransactionSender(mocks);
    }

    /**
     * A sender that returns pre-built producers instead of connecting to a broker, so the real
     * transaction lifecycle can be driven from a unit test.
     */
    private static class TestingKafkaTransactionSender
            extends KafkaTransactionSender<byte[], byte[]> {

        private final Deque<KafkaInternalProducer<byte[], byte[]>> producers;
        private final List<String> createdTransactionIds = new ArrayList<>();

        @SafeVarargs
        private TestingKafkaTransactionSender(KafkaInternalProducer<byte[], byte[]>... producers) {
            super(TRANSACTION_PREFIX, new Properties());
            this.producers = new ArrayDeque<>(Arrays.asList(producers));
        }

        @Override
        protected KafkaInternalProducer<byte[], byte[]> createTransactionProducer(
                String transactionId) {
            Assertions.assertFalse(
                    producers.isEmpty(), "unexpected producer creation for " + transactionId);
            createdTransactionIds.add(transactionId);
            return producers.removeFirst();
        }
    }

    /**
     * A mocked {@link KafkaInternalProducer} whose {@code flush()} completes the callbacks of
     * previously submitted sends, mirroring Kafka's guarantee that flush returns only once all
     * buffered records have completed either successfully or exceptionally.
     */
    private static final class ProducerStub {

        private final KafkaInternalProducer<byte[], byte[]> mock;
        private final List<Callback> pendingCallbacks = new ArrayList<>();
        private final AtomicBoolean txnStarted = new AtomicBoolean(false);

        private boolean startTransactionOnFlush;
        private Exception sendFailure;
        private int sendCount;

        @SuppressWarnings("unchecked")
        private ProducerStub() {
            this.mock = mock(KafkaInternalProducer.class);
            when(mock.getProducerId()).thenReturn(PRODUCER_ID);
            when(mock.getEpoch()).thenReturn(EPOCH);
            when(mock.isTxnStarted()).thenAnswer(invocation -> txnStarted.get());
            doAnswer(
                            invocation -> {
                                pendingCallbacks.add(invocation.getArgument(1));
                                sendCount++;
                                return null;
                            })
                    .when(mock)
                    .send(any(), any());
            doAnswer(
                            invocation -> {
                                completePendingSends(sendFailure);
                                if (startTransactionOnFlush) {
                                    txnStarted.set(true);
                                }
                                return null;
                            })
                    .when(mock)
                    .flush();
        }

        /** Reports the transaction as started from the beginning. */
        private void transactionStarted() {
            txnStarted.set(true);
        }

        /**
         * Defers the transaction registration to {@code flush()}, as the broker acknowledging
         * AddPartitionsToTxn does.
         */
        private void startTransactionOnFlush() {
            startTransactionOnFlush = true;
        }

        /** Makes pending and subsequent sends complete exceptionally when flushed. */
        private void failPendingSendsWith(Exception exception) {
            sendFailure = exception;
        }

        /** Invokes the pending send callbacks, as the producer's sender thread would. */
        private void completePendingSends(Exception exception) {
            List<Callback> callbacks = new ArrayList<>(pendingCallbacks);
            pendingCallbacks.clear();
            for (Callback callback : callbacks) {
                callback.onCompletion(null, exception);
            }
        }
    }
}
