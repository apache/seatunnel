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

import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A Kafka producer sender that uses a shared KafkaProducer instance without transaction support.
 *
 * <p>This sender is designed for multi-table sink scenarios where multiple writers share a single
 * KafkaProducer instance to reduce resource consumption.
 *
 * <p>Note: The close() method only flushes the producer without closing it, as the producer
 * lifecycle is managed by {@link KafkaProducerManager}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KafkaSharedNoTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;

    public KafkaSharedNoTransactionSender(KafkaProducer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void beginTransaction(String transactionId) {
        // no-op for non-transaction mode
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortTransaction() {
        // no-op for non-transaction mode
    }

    @Override
    public void abortTransaction(long checkpointId) {
        // no-op for non-transaction mode
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        kafkaProducer.flush();
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // Only flush, do not close the producer
        // The producer is managed by KafkaProducerManager
        kafkaProducer.flush();
    }
}
