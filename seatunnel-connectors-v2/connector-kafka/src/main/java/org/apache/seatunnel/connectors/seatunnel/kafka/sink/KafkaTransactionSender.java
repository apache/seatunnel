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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.connectors.seatunnel.kafka.KafkaClientUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.kafka.sink.KafkaSinkWriter.generateTransactionId;

/**
 * This sender will use kafka transaction to guarantee the data is sent to kafka at exactly-once.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
@Slf4j
public class KafkaTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private KafkaInternalProducer<K, V> kafkaProducer;
    private String transactionId;
    private final String transactionPrefix;
    private final Properties kafkaProperties;

    /**
     * Holds the first asynchronous send failure of the current transaction. It is written from the
     * producer's sender thread through the send callback and read by the task thread, so it must
     * stay thread-safe. It is scoped to a single transaction and therefore reset by {@link
     * #beginTransaction(String)}.
     */
    private final AtomicReference<Exception> asyncSendException = new AtomicReference<>();

    private int recordNumInTransaction = 0;

    public KafkaTransactionSender(String transactionPrefix, Properties kafkaProperties) {
        this.transactionPrefix = transactionPrefix;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        // Surface an already recorded asynchronous failure on the write path. The current
        // transaction can no longer be committed, so buffering and transmitting more records for it
        // would only waste producer memory and network bandwidth until the next checkpoint.
        checkAsyncSendException();
        kafkaProducer.send(producerRecord, this::onSendCompleted);
        recordNumInTransaction++;
    }

    /**
     * Records the first asynchronous send failure of the current transaction so that {@link
     * #prepareCommit()} can fail the checkpoint instead of committing a partial transaction.
     *
     * <p>Invoked on the producer's sender thread.
     */
    private void onSendCompleted(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            return;
        }
        if (!asyncSendException.compareAndSet(null, exception)) {
            // Only the first failure becomes the checkpoint failure cause. Log the later ones so a
            // broker-side incident affecting several partitions can still be diagnosed.
            log.warn(
                    "Suppressed an additional asynchronous send failure of Kafka transaction [{}]",
                    transactionId,
                    exception);
        }
    }

    @Override
    public void beginTransaction(String transactionId) {
        this.transactionId = transactionId;
        this.kafkaProducer = getTransactionProducer(transactionId);
        kafkaProducer.beginTransaction();
        // Reset the per-transaction state. A new transaction always runs on a newly created
        // producer, so a failure recorded for the previous transaction no longer applies. Keeping
        // it would turn a single transient send error into a permanent checkpoint failure loop.
        recordNumInTransaction = 0;
        asyncSendException.set(null);
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        // Flush pending async sends before capturing the transaction state. Kafka only marks the
        // transaction as started once the AddPartitionsToTxn request has been acknowledged by the
        // broker, and that request is issued asynchronously by the producer's sender thread. If a
        // checkpoint reaches this point before the first record's transaction registration
        // completes, isTxnStarted() would still return false and the resulting commit info would
        // instruct the committer to skip EndTxn, leaving the transaction to time out and its
        // records permanently invisible to read_committed consumers.
        kafkaProducer.flush();
        checkAsyncSendException();
        boolean txnStarted = kafkaProducer.isTxnStarted();
        if (recordNumInTransaction > 0 && !txnStarted) {
            // Records were sent in this transaction but Kafka still reports it as not started even
            // after flushing, meaning the transaction registration never completed. Committing with
            // txnStarted=false would make the committer skip EndTxn and drop these records, so fail
            // fast and let the checkpoint abort this transaction instead of silently producing a
            // lossy commit info.
            throw new KafkaConnectorException(
                    KafkaConnectorErrorCode.TRANSACTION_NOT_STARTED,
                    String.format(
                            "Kafka transaction [%s] has %d record(s) but is still reported as not "
                                    + "started after flushing pending sends. The transaction "
                                    + "registration did not complete. Refusing to commit to avoid "
                                    + "data loss.",
                            transactionId, recordNumInTransaction));
        }
        KafkaCommitInfo kafkaCommitInfo =
                new KafkaCommitInfo(
                        transactionId,
                        kafkaProperties,
                        this.kafkaProducer.getProducerId(),
                        this.kafkaProducer.getEpoch(),
                        txnStarted);
        return Optional.of(kafkaCommitInfo);
    }

    private void checkAsyncSendException() {
        Exception exception = asyncSendException.get();
        if (exception != null) {
            throw new KafkaConnectorException(
                    KafkaConnectorErrorCode.PRODUCE_DATA_FAILED,
                    String.format(
                            "Kafka transaction [%s] failed to send one or more of its %d record(s) "
                                    + "asynchronously.",
                            transactionId, recordNumInTransaction),
                    exception);
        }
    }

    @Override
    public void abortTransaction() {
        kafkaProducer.abortTransaction();
    }

    @Override
    public void abortTransaction(long checkpointId) {
        for (long i = checkpointId; ; i++) {
            String transactionId = generateTransactionId(this.transactionPrefix, i);
            KafkaInternalProducer<K, V> producer = createTransactionProducer(transactionId);
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Abort kafka transaction: {}", transactionId);
                }
                // Initializing a producer with the same transactional ID fences and aborts any
                // transaction left by a previous attempt. Each ID needs its own producer: changing
                // the ID on a reused producer can retain a non-zero epoch indefinitely.
                if (producer.getEpoch() == 0) {
                    return;
                }
            } finally {
                closeTemporaryProducer(producer);
            }
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        if (recordNumInTransaction == 0) {
            // KafkaSinkCommitter does not support emptyTransaction, so we commit here.
            kafkaProducer.commitTransaction();
        }
        return Lists.newArrayList(
                new KafkaSinkState(
                        transactionId, transactionPrefix, checkpointId, kafkaProperties));
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            KafkaClientUtils.runWithConnectorClassLoader(
                    () -> {
                        kafkaProducer.flush();
                        // kafkaProducer will abort the transaction if you call close() without a
                        // duration arg which will cause an exception when Committer commit the
                        // transaction later.
                        kafkaProducer.close(Duration.ZERO);
                    });
        }
    }

    private KafkaInternalProducer<K, V> getTransactionProducer(String transactionId) {
        close();
        return createTransactionProducer(transactionId);
    }

    /** Creates and initializes a producer used for one transactional ID. */
    protected KafkaInternalProducer<K, V> createTransactionProducer(String transactionId) {
        Properties transactionProperties = (Properties) kafkaProperties.clone();
        transactionProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        KafkaInternalProducer<K, V> transactionProducer =
                new KafkaInternalProducer<>(transactionProperties, transactionId);
        transactionProducer.initTransactions();
        return transactionProducer;
    }

    private void closeTemporaryProducer(KafkaInternalProducer<K, V> producer) {
        KafkaClientUtils.runWithConnectorClassLoader(() -> producer.close(Duration.ZERO));
    }
}
