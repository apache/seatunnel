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

import org.apache.seatunnel.api.sink.SinkMetricsCalc;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.state.KafkaSinkState;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * This sender will send the data to the Kafka, and will not guarantee the data is committed to the
 * Kafka exactly-once.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
public class KafkaNoTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;

    private final SinkMetricsCalc sinkMetricsCalc;

    public KafkaNoTransactionSender(Properties properties, SinkMetricsCalc sinkMetricsCalc) {
        this.kafkaProducer = new KafkaProducer<>(properties);
        this.sinkMetricsCalc = sinkMetricsCalc;
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        try {
            kafkaProducer.send(producerRecord);
            sinkMetricsCalc.confirmMetrics();
        } catch (Exception e) {
            sinkMetricsCalc.cancelMetrics();
            throw new RuntimeException(e);
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
        kafkaProducer.flush();
        sinkMetricsCalc.confirmMetrics();
        return Collections.emptyList();
    }

    @Override
    public void close() {
        kafkaProducer.flush();
        sinkMetricsCalc.confirmMetrics();
        kafkaProducer.close();
    }
}
