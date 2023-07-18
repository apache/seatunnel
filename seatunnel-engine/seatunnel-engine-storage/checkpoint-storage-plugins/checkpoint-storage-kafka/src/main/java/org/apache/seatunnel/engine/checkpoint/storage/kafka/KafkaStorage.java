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

package org.apache.seatunnel.engine.checkpoint.storage.kafka;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.kafka.common.Table;
import org.apache.seatunnel.engine.checkpoint.storage.kafka.common.TopicAdmin;
import org.apache.seatunnel.engine.checkpoint.storage.kafka.config.KafkaConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.kafka.config.KafkaConfigurationConstants;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/** Checkpoint storage by kafka */
@Slf4j
public class KafkaStorage extends AbstractCheckpointStorage {
    public static final String KEY_NAME_SPLIT = "_";
    private Table<String, String, PipelineState> cache = new Table<>();
    private Producer<byte[], byte[]> producer;
    private Consumer<byte[], byte[]> consumer;
    private KafkaConfiguration kafkaConfiguration;

    public KafkaStorage(Map<String, String> configuration) throws CheckpointStorageException {
        this.initStorage(configuration);
    }

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        String bootstrapServers =
                configuration.get(KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS).toString();
        String compactTopic =
                configuration
                        .get(KafkaConfigurationConstants.KAFKA_CHECKPOINT_STORAGE_COMPACT_TOPIC)
                        .toString();
        Integer topicReplicationFactor =
                Integer.parseInt(
                        configuration.getOrDefault(
                                KafkaConfigurationConstants
                                        .KAFKA_STORAGE_COMPACT_TOPIC_REPLICATION_FACTOR,
                                "3"));
        Integer topicPartition =
                Integer.parseInt(
                        configuration.getOrDefault(
                                KafkaConfigurationConstants.KAFKA_STORAGE_COMPACT_TOPIC_PARTITION,
                                "3"));

        kafkaConfiguration =
                KafkaConfiguration.builder()
                        .bootstrapServers(bootstrapServers)
                        .storageTopic(compactTopic)
                        .storageTopicPartition(topicPartition)
                        .storageTopicReplicationFactor(topicReplicationFactor)
                        .consumerConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        configuration,
                                        KafkaConfigurationConstants.KAFKA_CONSUMER_CONFIGS_PREFIX))
                        .producerConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        configuration,
                                        KafkaConfigurationConstants.KAFKA_PRODUCER_CONFIGS_PREFIX))
                        .adminConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        configuration,
                                        KafkaConfigurationConstants.KAFKA_ADMIN_CONFIGS_PREFIX))
                        .topicConfigs(
                                KafkaConfiguration.setExtraConfiguration(
                                        configuration,
                                        KafkaConfigurationConstants.KAFKA_TOPIC_CONFIGS_PREFIX))
                        .build();

        maybeCreateTopicAndValidateCompactConfig();
        // init producer
        this.producer = createProducer();
        // init consumer
        this.consumer = createConsumer();
        // load all data
        readToEnd();
        // Start consumer data
        WorkThread workThread = new WorkThread();
        workThread.start();
    }

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

    /** Create kafka topic by admin */
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

    private Map<Object, Object> readToEnd() {
        Map<Object, Object> result = Maps.newConcurrentMap();
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
                    poll(consumer, processor(), Integer.MAX_VALUE);
                    break;
                }
            }
        }
        return result;
    }

    private java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> processor() {
        return new java.util.function.Consumer<ConsumerRecord<byte[], byte[]>>() {
            @Override
            public void accept(ConsumerRecord<byte[], byte[]> record) {
                // des data
                byte[] key = record.key();
                byte[] value = record.value();
                try {
                    String checkpointNameIncludeJobId = serializer.deserialize(key, String.class);
                    String[] hasParseKey = parseUniqueKeyForKafka(checkpointNameIncludeJobId);
                    if (value == null) {
                        cache.remove(hasParseKey[0], hasParseKey[1]);
                    } else {
                        PipelineState state = deserializeCheckPointData(value);
                        cache.put(hasParseKey[0], hasParseKey[1], state);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private void poll(
            Consumer<byte[], byte[]> consumer,
            java.util.function.Consumer<ConsumerRecord<byte[], byte[]>> accepter,
            long timeoutMs) {
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
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        String checkpointName = getCheckPointName(state);
        String checkPointUniqueName = generateUniqueKeyForKafka(state.getJobId(), checkpointName);
        try {
            byte[] key = serializer.serialize(checkPointUniqueName);
            byte[] value = serializeCheckPointData(state);
            // update cache
            cache.put(state.getJobId(), checkpointName, state);
            // Sync send data
            this.producer
                    .send(new ProducerRecord<>(kafkaConfiguration.getStorageTopic(), key, value))
                    .get();
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to serialize checkpoint data,state is :" + state, e);
        } catch (ExecutionException | InterruptedException e) {
            throw new CheckpointStorageException(
                    "Failed to flush checkpoint data to kafka,state is :" + state, e);
        }
        return checkPointUniqueName;
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        return new ArrayList<>(pipelineStates.values());
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            log.info("No checkpoint found for this  job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        Set<String> latestPipelineNames =
                getLatestPipelineNames(new ArrayList<>(pipelineStates.keySet()));
        List<PipelineState> latestPipelineStates = new ArrayList<>();
        latestPipelineNames.forEach(
                pipelineName -> {
                    latestPipelineStates.add(pipelineStates.get(pipelineName));
                });

        if (latestPipelineStates.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id:{} " + jobId);
        }
        return latestPipelineStates;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            log.info("No checkpoint found for job, job id is: " + jobId);
            return null;
        }
        String latestFileName =
                getLatestCheckpointFileNameByJobIdAndPipelineId(
                        new ArrayList<>(pipelineStates.keySet()), pipelineId);
        if (latestFileName == null) {
            log.info(
                    "No checkpoint found for this job, the job id is: "
                            + jobId
                            + ", pipeline id is: "
                            + pipelineId);
            return null;
        }
        return pipelineStates.get(latestFileName);
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        List<PipelineState> pipelineStateResult = new ArrayList<>();
        for (Map.Entry<String, PipelineState> entry : pipelineStates.entrySet()) {
            String uniqueKey = entry.getKey();
            String filePipelineId = getPipelineIdByFileName(uniqueKey);
            if (pipelineId.equals(filePipelineId)) {
                try {
                    pipelineStateResult.add(entry.getValue());
                } catch (Exception e) {
                    log.error("Failed to read checkpoint data from file " + uniqueKey, e);
                }
            }
        }
        return pipelineStateResult;
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            return;
        }
        for (Map.Entry<String, PipelineState> entry : pipelineStates.entrySet()) {
            String checkpointName = entry.getKey();
            try {
                String storeUniqueKey = generateUniqueKeyForKafka(jobId, checkpointName);
                byte[] key = serializer.serialize(storeUniqueKey);
                // update cache
                cache.remove(jobId, checkpointName);
                // persist data
                producer.send(
                        new ProducerRecord<>(kafkaConfiguration.getStorageTopic(), key, null));
            } catch (Exception e) {
                log.error("Failed to delete checkpoint for job {}", jobId, e);
                throw new RuntimeException(
                        String.format("Failed to delete checkpoint for job %s", jobId), e);
            }
        }
        try {
            flush();
        } catch (CheckpointStorageException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        for (Map.Entry<String, PipelineState> entry : pipelineStates.entrySet()) {
            String uniqueKey = entry.getKey();
            if (pipelineId.equals(getPipelineIdByFileName(uniqueKey))
                    && getCheckpointIdByFileName(uniqueKey).equals(checkpointId)) {
                return entry.getValue();
            }
        }
        throw new CheckpointStorageException(
                "No checkpoint found for job, job id is: "
                        + jobId
                        + ", pipeline id is:"
                        + pipelineId
                        + ", checkpoint"
                        + " id is: "
                        + checkpointId);
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        for (Map.Entry<String, PipelineState> entry : pipelineStates.entrySet()) {
            String checkpointName = entry.getKey();
            String checkpointIdByUniqueKey = getCheckpointIdByFileName(checkpointName);
            if (pipelineId.equals(getPipelineIdByFileName(checkpointName))
                    && checkpointIdByUniqueKey.equals(checkpointId)) {
                try {
                    String storeUniqueKey = generateUniqueKeyForKafka(jobId, checkpointName);
                    byte[] key = serializer.serialize(storeUniqueKey);
                    // update cache
                    cache.remove(jobId, checkpointName);
                    // persist data
                    producer.send(
                            new ProducerRecord<>(kafkaConfiguration.getStorageTopic(), key, null));

                } catch (Exception e) {
                    log.error(
                            "Failed to delete checkpoint {} for job {}, pipeline {}",
                            checkpointId,
                            jobId,
                            pipelineId,
                            e);
                    throw new CheckpointStorageException(
                            String.format(
                                    "Failed to delete checkpoint %s for job %s, " + "pipeline %s",
                                    checkpointId, jobId, pipelineId),
                            e);
                }
            }
        }
        // flush data
        flush();
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        Map<String, PipelineState> pipelineStates = cache.row(jobId);
        if (pipelineStates.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        for (Map.Entry<String, PipelineState> row : pipelineStates.entrySet()) {
            String checkpointName = row.getKey();
            String checkpointId = getCheckpointIdByFileName(checkpointName);
            if (pipelineId.equals(getPipelineIdByFileName(checkpointName))
                    && checkpointIdList.contains(checkpointId)) {
                try {
                    String storeUniqueKey = generateUniqueKeyForKafka(jobId, checkpointName);
                    byte[] key = serializer.serialize(storeUniqueKey);
                    // update cache
                    cache.remove(jobId, checkpointName);
                    // persist data
                    producer.send(
                            new ProducerRecord<>(kafkaConfiguration.getStorageTopic(), key, null));
                } catch (Exception e) {
                    log.error(
                            "Failed to delete checkpoint {} for job {}, pipeline {}",
                            checkpointId,
                            jobId,
                            pipelineId,
                            e);
                    throw new CheckpointStorageException(
                            String.format(
                                    "Failed to delete checkpoint %s for job %s, " + "pipeline %s",
                                    checkpointId, jobId, pipelineId),
                            e);
                }
            }
        }
        flush();
    }

    public void flush() throws CheckpointStorageException {
        try {
            producer.flush();
        } catch (Exception ex) {
            throw new CheckpointStorageException(
                    "Flush checkpoint data to kafka storage failed,", ex);
        }
    }

    private String generateUniqueKeyForKafka(String jobId, String checkpointName) {
        return jobId + KEY_NAME_SPLIT + checkpointName;
    }

    private String[] parseUniqueKeyForKafka(String key) {
        return key.split(KEY_NAME_SPLIT);
    }

    private class WorkThread extends Thread {
        public WorkThread() {
            super("KafkaStorage Work Thread - " + kafkaConfiguration.getStorageTopic());
        }

        @Override
        public void run() {
            try {
                log.trace("{} started process record ", this);
                while (true) {
                    try {
                        poll(consumer, processor(), Integer.MAX_VALUE);
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by
                        // starting this loop again
                    }
                }
            } catch (Throwable t) {
                log.error("Unexpected exception in {}", this, t);
            } finally {
                consumer.close();
            }
        }
    }
}
