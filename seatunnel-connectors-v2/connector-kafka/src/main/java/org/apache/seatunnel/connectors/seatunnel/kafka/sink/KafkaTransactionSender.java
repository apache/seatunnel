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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

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

    public KafkaTransactionSender(String transactionPrefix, Properties kafkaProperties) {
        this.transactionPrefix = transactionPrefix;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void beginTransaction(String transactionId) {
        this.transactionId = transactionId;
        this.kafkaProducer = getTransactionProducer(kafkaProperties, transactionId);
        kafkaProducer.beginTransaction();
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        KafkaCommitInfo kafkaCommitInfo =
                new KafkaCommitInfo(
                        transactionId,
                        kafkaProperties,
                        this.kafkaProducer.getProducerId(),
                        this.kafkaProducer.getEpoch());
        return Optional.of(kafkaCommitInfo);
    }

    @Override
    public void abortTransaction() {
        kafkaProducer.abortTransaction();
    }

    @Override
    public void abortTransaction(long checkpointId) {

        KafkaInternalProducer<K, V> producer;
        if (this.kafkaProducer != null) {
            producer = this.kafkaProducer;
        } else {
            producer =
                    getTransactionProducer(
                            this.kafkaProperties,
                            generateTransactionId(this.transactionPrefix, checkpointId));
        }

        for (long i = checkpointId; ; i++) {
            String transactionId = generateTransactionId(this.transactionPrefix, i);
            producer.setTransactionalId(transactionId);
            if (log.isDebugEnabled()) {
                log.debug("Abort kafka transaction: {}", transactionId);
            }
            producer.flush();
            if (producer.getEpoch() == 0) {
                break;
            }
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        return Lists.newArrayList(
                new KafkaSinkState(
                        transactionId, transactionPrefix, checkpointId, kafkaProperties));
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    private KafkaInternalProducer<K, V> getTransactionProducer(
            Properties properties, String transactionId) {
        close();
        Properties transactionProperties = (Properties) properties.clone();
        transactionProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        KafkaInternalProducer<K, V> transactionProducer =
                new KafkaInternalProducer<>(transactionProperties, transactionId);
        transactionProducer.initTransactions();
        return transactionProducer;
    }
}
