/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.kafka;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.kafka.bean.IMapDataStruct;
import org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfiguration;
import org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfigurationConstants;
import org.apache.seatunnel.engine.imap.storage.kafka.utils.TopicAdmin;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import org.apache.commons.lang3.ClassUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfigurationConstants.BUSINESS_KEY;

@Slf4j
public class IMapKafkaStorage implements IMapStorage {
    private KafkaConfiguration kafkaConfiguration;
    private Producer<byte[], byte[]> producer;
    private Serializer serializer;

    private String businessName;

    @Override
    public void initialize(Map<String, Object> config) {
        String bootstrapServers =
                config.get(KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS).toString();
        String compactTopicPrefix =
                config.get(KafkaConfigurationConstants.KAFKA_STORAGE_COMPACT_TOPIC_PREFIX)
                        .toString();
        this.businessName = (String) config.get(BUSINESS_KEY);
        String compactTopic = compactTopicPrefix.concat(businessName);
        Integer topicReplicationFactor =
                Integer.parseInt(
                        config.getOrDefault(
                                        KafkaConfigurationConstants
                                                .KAFKA_STORAGE_COMPACT_TOPIC_REPLICATION_FACTOR,
                                        3)
                                .toString());
        Integer topicPartition =
                Integer.parseInt(
                        config.getOrDefault(
                                        KafkaConfigurationConstants
                                                .KAFKA_STORAGE_COMPACT_TOPIC_PARTITION,
                                        1)
                                .toString());

        kafkaConfiguration =
                KafkaConfiguration.builder()
                        .bootstrapServers(bootstrapServers)
                        .storageTopic(compactTopic)
                        .storageTopicPartition(topicPartition)
                        .storageTopicReplicationFactor(topicReplicationFactor)
                        .consumerConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        config,
                                        KafkaConfigurationConstants.KAFKA_CONSUMER_CONFIGS_PREFIX))
                        .producerConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        config,
                                        KafkaConfigurationConstants.KAFKA_PRODUCER_CONFIGS_PREFIX))
                        .adminConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        config,
                                        KafkaConfigurationConstants.KAFKA_ADMIN_CONFIGS_PREFIX))
                        .topicConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        config,
                                        KafkaConfigurationConstants.KAFKA_TOPIC_CONFIGS_PREFIX))
                        .build();

        // Init serializer, default ProtoStuffSerializer
        this.serializer = new ProtoStuffSerializer();
        maybeCreateTopicAndValidateCompactConfig();
        this.producer = createProducer();
    }

    private void maybeCreateTopicAndValidateCompactConfig() {
        // create admin client
        TopicAdmin topicAdmin = new TopicAdmin(kafkaConfiguration);
        try {
            // It must be compact topic
            topicAdmin.maybeCreateTopic(kafkaConfiguration.getStorageTopic());
            topicAdmin.verifyTopicCleanupPolicyOnlyCompact(kafkaConfiguration.getStorageTopic());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                topicAdmin.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Create producer
     *
     * @return
     */
    private Producer<byte[], byte[]> createProducer() {
        Map<String, Object> producerConfigs = kafkaConfiguration.getProducerConfigs();
        producerConfigs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBootstrapServers());
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerConfigs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerConfigs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(producerConfigs);
    }

    /**
     * Create consumer
     *
     * @return
     */
    private Consumer<byte[], byte[]> createConsumer() {
        // create topic
        Map<String, Object> consumerConfigs = kafkaConfiguration.getConsumerConfigs();
        consumerConfigs.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBootstrapServers());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfigs.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        consumerConfigs.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs);
        return consumer;
    }

    @Override
    public boolean store(Object key, Object value) {
        try {
            Map.Entry<byte[], byte[]> data = convertToMapEntry(key, value);
            Future<RecordMetadata> callback =
                    producer.send(
                            new ProducerRecord<>(
                                    kafkaConfiguration.getStorageTopic(),
                                    data.getKey(),
                                    data.getValue()));
            return Objects.nonNull(callback.get());
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Object> storeAll(Map<Object, Object> all) {
        for (Map.Entry<Object, Object> item : all.entrySet()) {
            store(item.getKey(), item.getValue());
        }
        return all.keySet();
    }

    @Override
    public boolean delete(Object key) {
        try {
            byte[] bKey = convertToBytes(key);
            // Sending tombstone messages will be cleared during topic compact
            Future<RecordMetadata> callback =
                    producer.send(
                            new ProducerRecord<>(kafkaConfiguration.getStorageTopic(), bKey, null));
            return Objects.nonNull(callback.get());
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<Object> deleteAll(Collection<Object> keys) {
        for (Object key : keys) {
            delete(key);
        }
        return new HashSet<>(keys);
    }

    @Override
    public Map<Object, Object> loadAll() throws IOException {
        Map<Object, Object> result = Maps.newConcurrentMap();
        try (Consumer<byte[], byte[]> consumer = createConsumer()) {
            List<TopicPartition> partitions = new ArrayList<>();
            List<PartitionInfo> partitionInfos =
                    consumer.partitionsFor(kafkaConfiguration.getStorageTopic());
            if (partitionInfos == null) {
                throw new RuntimeException(
                        "Could not look up partition metadata for offset backing store topic in"
                                + " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if"
                                + " this is your first use of the topic it may have taken too long to create.");
            }
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
            consumer.assign(partitions);
            // Always consume from the beginning of all partitions.
            consumer.seekToBeginning(partitions);
            // Start to read data
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            log.info("Reading to end of log offsets {}", endOffsets);
            while (!endOffsets.isEmpty()) {
                Iterator<Map.Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<TopicPartition, Long> entry = it.next();
                    TopicPartition topicPartition = entry.getKey();
                    long endOffset = entry.getValue();
                    long lastConsumedOffset = consumer.position(topicPartition);
                    if (lastConsumedOffset >= endOffset) {
                        log.info("Read to end offset {} for {}", endOffset, topicPartition);
                        it.remove();
                    } else {
                        log.info(
                                "Behind end offset {} for {}; last-read offset is {}",
                                endOffset,
                                topicPartition,
                                lastConsumedOffset);
                        poll(
                                consumer,
                                Integer.MAX_VALUE,
                                new java.util.function.Consumer<ConsumerRecord<byte[], byte[]>>() {
                                    @Override
                                    public void accept(ConsumerRecord<byte[], byte[]> record) {
                                        try {
                                            IMapDataStruct key =
                                                    serializer.deserialize(
                                                            record.key(), IMapDataStruct.class);
                                            Class<?> keyClazz =
                                                    ClassUtils.getClass(key.getClassName());
                                            Object originalKey =
                                                    serializer.deserialize(
                                                            key.getValue(), keyClazz);

                                            if (record.value() == null) {
                                                result.remove(originalKey);
                                            } else {
                                                IMapDataStruct value =
                                                        serializer.deserialize(
                                                                record.value(),
                                                                IMapDataStruct.class);
                                                Class<?> valueClazz =
                                                        ClassUtils.getClass(value.getClassName());
                                                Object originalValue =
                                                        serializer.deserialize(
                                                                value.getValue(), valueClazz);
                                                result.put(originalKey, originalValue);
                                            }

                                        } catch (IOException | ClassNotFoundException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                                });
                        break;
                    }
                }
            }
        }
        return result;
    }

    private void poll(
            Consumer<byte[], byte[]> consumer,
            long timeoutMs,
            java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> accepter) {
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(timeoutMs));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                accepter.accept(record);
            }
        } catch (WakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            log.error("Error polling: " + e);
            throw e;
        }
    }

    @Override
    public Set<Object> loadAllKeys() {
        try {
            return loadAll().keySet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(boolean deleteAllFileFlag) {
        if (this.producer != null) {
            this.producer.close();
        }
        log.info("start destroy IMapKafkaStorage, businessName is {}", businessName);
        if (deleteAllFileFlag) {
            // Delete compact topic
            TopicAdmin admin = new TopicAdmin(kafkaConfiguration);
            try {
                admin.deleteTopic(kafkaConfiguration.getStorageTopic());
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    admin.close();
                } catch (Exception e) {
                    // Do nothing
                }
            }
        }
    }

    public Map.Entry<byte[], byte[]> convertToMapEntry(Object key, Object value)
            throws IOException {
        byte[] bKey = convertToBytes(key);
        byte[] bValue = convertToBytes(value);
        return new Map.Entry<byte[], byte[]>() {
            @Override
            public byte[] getKey() {
                return bKey;
            }

            @Override
            public byte[] getValue() {
                return bValue;
            }

            @Override
            public byte[] setValue(byte[] value) {
                return new byte[0];
            }
        };
    }

    private byte[] convertToBytes(Object data) throws IOException {
        IMapDataStruct struct =
                IMapDataStruct.builder()
                        .value(serializer.serialize(data))
                        .className(data.getClass().getName())
                        .build();
        return serializer.serialize(struct);
    }
}
