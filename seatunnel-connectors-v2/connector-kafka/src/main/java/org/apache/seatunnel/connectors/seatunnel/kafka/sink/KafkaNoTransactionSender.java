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

import org.apache.seatunnel.connectors.seatunnel.kafka.KafkaClientUtils;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This sender will send the data to the Kafka, and will not guarantee the data is committed to the
 * Kafka exactly-once.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
@Slf4j
public class KafkaNoTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private final Producer<K, V> kafkaProducer;

    /**
     * Holds the first asynchronous send failure. It is written from the producer's sender thread
     * through the send callback and read by the task thread. Once set, the sender stays failed: the
     * task fails and restores from the last completed checkpoint, so the lost record is re-sent.
     */
    private final AtomicReference<Exception> asyncSendException = new AtomicReference<>();

    public KafkaNoTransactionSender(Properties properties) {
        this(new KafkaProducer<>(properties));
    }

    KafkaNoTransactionSender(Producer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        // Fail the write path as soon as a send has failed, instead of sending past the lost
        // record.
        checkAsyncSendException();
        kafkaProducer.send(producerRecord, this::onSendCompleted);
    }

    /** Invoked on the producer's sender thread. */
    private void onSendCompleted(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            return;
        }
        if (!asyncSendException.compareAndSet(null, exception)) {
            log.warn("Suppressed an additional asynchronous Kafka send failure", exception);
        }
    }

    @Override
    public void beginTransaction(String transactionId) {
        // no-op
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortTransaction() {
        // no-op
    }

    @Override
    public void abortTransaction(long checkpointId) {
        // no-op
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        // flush() waits for pending sends but does not throw for failed ones. Without this check
        // the
        // checkpoint would complete past records that never reached Kafka.
        kafkaProducer.flush();
        checkAsyncSendException();
        return Collections.emptyList();
    }

    private void checkAsyncSendException() {
        Exception exception = asyncSendException.get();
        if (exception != null) {
            throw new KafkaConnectorException(
                    KafkaConnectorErrorCode.PRODUCE_DATA_FAILED,
                    "Kafka failed to send one or more records asynchronously.",
                    exception);
        }
    }

    @Override
    public void close() {
        KafkaClientUtils.runWithConnectorClassLoader(
                () -> {
                    kafkaProducer.flush();
                    kafkaProducer.close();
                });
    }
}
