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

import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class KafkaConsumerThread implements Runnable {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private static final String CLIENT_ID_PREFIX = "seatunnel";
    private final ConsumerMetadata metadata;

    private final LinkedBlockingQueue<Consumer<KafkaConsumer<byte[], byte[]>>> tasks;

    public KafkaConsumerThread(ConsumerMetadata metadata) {
        this.metadata = metadata;
        this.tasks = new LinkedBlockingQueue<>();
        this.consumer = initConsumer(this.metadata.getBootstrapServers(), this.metadata.getConsumerGroup(),
            this.metadata.getProperties(), !this.metadata.isCommitOnCheckpoint());
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Consumer<KafkaConsumer<byte[], byte[]>> task = tasks.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    task.accept(consumer);
                }
            } catch (InterruptedException e) {
                throw new KafkaConnectorException(KafkaConnectorErrorCode.CONSUME_THREAD_RUN_ERROR, e);
            }
        }
    }

    public LinkedBlockingQueue<Consumer<KafkaConsumer<byte[], byte[]>>> getTasks() {
        return tasks;
    }

    private KafkaConsumer<byte[], byte[]> initConsumer(String bootstrapServer, String consumerGroup,
                                                       Properties properties, boolean autoCommit) {
        Properties props = new Properties();
        properties.forEach((key, value) -> props.setProperty(String.valueOf(key), String.valueOf(value)));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_PREFIX + "-enumerator-consumer-" + this.hashCode());

        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));

        // Disable auto create topics feature
        props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }
}
