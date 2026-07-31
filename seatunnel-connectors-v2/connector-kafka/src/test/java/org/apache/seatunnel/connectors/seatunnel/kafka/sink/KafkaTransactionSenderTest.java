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

import org.apache.kafka.clients.producer.ProducerConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class KafkaTransactionSenderTest {

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
                Arrays.asList("test-prefix-7", "test-prefix-8"), sender.createdTransactionIds);
        Mockito.verify(existingTransaction).close(Duration.ZERO);
        Mockito.verify(unusedTransaction).close(Duration.ZERO);
    }

    private static class TestingKafkaTransactionSender
            extends KafkaTransactionSender<byte[], byte[]> {

        private final ArrayDeque<KafkaInternalProducer<byte[], byte[]>> producers;
        private final List<String> createdTransactionIds = new java.util.ArrayList<>();

        @SafeVarargs
        private TestingKafkaTransactionSender(KafkaInternalProducer<byte[], byte[]>... producers) {
            super("test-prefix", kafkaProperties());
            this.producers = new ArrayDeque<>(Arrays.asList(producers));
        }

        @Override
        protected KafkaInternalProducer<byte[], byte[]> createTransactionProducer(
                String transactionId) {
            createdTransactionIds.add(transactionId);
            return producers.removeFirst();
        }

        private static Properties kafkaProperties() {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
            return properties;
        }
    }
}
