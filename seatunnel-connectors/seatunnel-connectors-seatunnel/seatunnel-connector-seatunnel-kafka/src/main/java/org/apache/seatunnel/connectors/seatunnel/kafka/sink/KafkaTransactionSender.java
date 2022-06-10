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

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * This sender will use kafka transaction to guarantee the data is sent to kafka at exactly-once.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
public class KafkaTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTransactionSender.class);

    private final KafkaProducer<K, V> kafkaProducer;
    private final String transactionId;
    private final Properties kafkaProperties;

    public KafkaTransactionSender(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.transactionId = getTransactionId();
        this.kafkaProducer = getTransactionProducer(kafkaProperties, transactionId);
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void beginTransaction() {
        kafkaProducer.beginTransaction();
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        // TODO kafka can't use transactionId to commit on different producer directly, we should find
        //  another way
        KafkaCommitInfo kafkaCommitInfo = new KafkaCommitInfo(transactionId, kafkaProperties);
        return Optional.of(kafkaCommitInfo);
    }

    @Override
    public void abortTransaction() {
        kafkaProducer.abortTransaction();
    }

    @Override
    public void abortTransaction(List<KafkaSinkState> kafkaStates) {
        if (kafkaStates.isEmpty()) {
            return;
        }
        for (KafkaSinkState kafkaState : kafkaStates) {
            // create the transaction producer
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Abort kafka transaction: {}", kafkaState.getTransactionId());
            }
            KafkaProducer<K, V> historyProducer = getTransactionProducer(kafkaProperties, kafkaState.getTransactionId());
            historyProducer.abortTransaction();
            historyProducer.close();
        }
    }

    @Override
    public List<KafkaSinkState> snapshotState() {
        return Lists.newArrayList(new KafkaSinkState(transactionId, kafkaProperties));
    }

    @Override
    public void close() {
        kafkaProducer.flush();
        try (KafkaProducer<?, ?> closedProducer = kafkaProducer) {
            // no-op
        }
    }

    private KafkaProducer<K, V> getTransactionProducer(Properties properties, String transactionId) {
        Properties transactionProperties = (Properties) properties.clone();
        transactionProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        KafkaProducer<K, V> transactionProducer = new KafkaProducer<>(transactionProperties);
        transactionProducer.initTransactions();
        return transactionProducer;
    }

    // todo: use a better way to generate the transaction id
    private String getTransactionId() {
        return "SeaTunnel-" + System.currentTimeMillis();
    }
}
