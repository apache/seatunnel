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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages shared KafkaProducer instances for multi-table sink.
 *
 * <p>For non-transaction mode: All writers share a single KafkaProducer instance.
 *
 * <p>For exactly-once mode: Each queue index has its own KafkaProducer with unique
 * transactional.id.
 */
@Slf4j
@Getter
public class KafkaProducerManager {

    private final Properties kafkaProperties;
    private final boolean isExactlyOnce;

    /** Shared producer for non-transaction mode */
    private volatile KafkaProducer<byte[], byte[]> sharedProducer;

    /** Transaction producers for exactly-once mode, keyed by queue index */
    private final Map<Integer, KafkaProducer<byte[], byte[]>> transactionProducers;

    public KafkaProducerManager(Properties kafkaProperties, boolean isExactlyOnce) {
        this.kafkaProperties = kafkaProperties;
        this.isExactlyOnce = isExactlyOnce;
        this.transactionProducers = new ConcurrentHashMap<>();

        if (!isExactlyOnce) {
            log.info("Creating shared KafkaProducer for non-transaction mode");
            this.sharedProducer = new KafkaProducer<>(kafkaProperties);
        }
    }

    /**
     * Get a KafkaProducer instance.
     *
     * @param queueIndex the queue index for transaction mode
     * @param transactionPrefix the transaction id prefix for exactly-once mode
     * @return KafkaProducer instance
     */
    public KafkaProducer<byte[], byte[]> getProducer(int queueIndex, String transactionPrefix) {
        if (!isExactlyOnce) {
            return sharedProducer;
        }

        return transactionProducers.computeIfAbsent(
                queueIndex,
                idx -> {
                    Properties props = new Properties();
                    props.putAll(kafkaProperties);
                    String transactionalId = transactionPrefix + "-shared-" + idx;
                    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
                    log.info(
                            "Creating transactional KafkaProducer for queue index {} with transactional.id: {}",
                            idx,
                            transactionalId);
                    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
                    producer.initTransactions();
                    return producer;
                });
    }

    /**
     * Check if the producer for the given queue index exists.
     *
     * @param queueIndex the queue index
     * @return true if the producer exists
     */
    public boolean containsProducer(int queueIndex) {
        if (!isExactlyOnce) {
            return sharedProducer != null;
        }
        return transactionProducers.containsKey(queueIndex);
    }

    /** Close all KafkaProducer instances. */
    public void close() {
        if (sharedProducer != null) {
            log.info("Closing shared KafkaProducer");
            try {
                sharedProducer.flush();
                sharedProducer.close();
            } catch (Exception e) {
                log.warn("Failed to close shared KafkaProducer", e);
            }
            sharedProducer = null;
        }

        transactionProducers.forEach(
                (idx, producer) -> {
                    log.info("Closing transactional KafkaProducer for queue index {}", idx);
                    try {
                        producer.flush();
                        producer.close();
                    } catch (Exception e) {
                        log.warn(
                                "Failed to close transactional KafkaProducer for queue index {}",
                                idx,
                                e);
                    }
                });
        transactionProducers.clear();
    }
}
