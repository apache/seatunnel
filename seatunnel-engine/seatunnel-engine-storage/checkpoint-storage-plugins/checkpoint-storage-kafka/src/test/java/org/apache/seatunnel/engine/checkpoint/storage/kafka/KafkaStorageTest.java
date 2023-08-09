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
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.kafka.config.KafkaConfigurationConstants;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
@Slf4j
public class KafkaStorageTest {

    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static String KAFKA_BOOTSTRAP_SERVERS;
    private static KafkaContainer kafkaContainer;
    private static KafkaStorage STORAGE;
    private static final String JOB_ID = "kafkaJobTest";

    @SneakyThrows
    @BeforeAll
    public static void setup() throws CheckpointStorageException {

        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");

        Map<String, String> configuration = new HashMap<>();
        // init configuration
        KAFKA_BOOTSTRAP_SERVERS = kafkaContainer.getBootstrapServers();
        configuration.put(
                KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS);
        configuration.put(
                KafkaConfigurationConstants.KAFKA_CHECKPOINT_STORAGE_COMPACT_TOPIC,
                "checkpoint-storage-config");
        configuration.put(
                KafkaConfigurationConstants.KAFKA_STORAGE_COMPACT_TOPIC_REPLICATION_FACTOR, "1");

        STORAGE = new KafkaStorage(configuration);
        PipelineState pipelineState =
                PipelineState.builder()
                        .jobId(JOB_ID)
                        .pipelineId(1)
                        .checkpointId(1)
                        .states(new byte[0])
                        .build();
        STORAGE.storeCheckPoint(pipelineState);
        pipelineState.setCheckpointId(2);
        STORAGE.storeCheckPoint(pipelineState);
        pipelineState.setPipelineId(2);
        pipelineState.setCheckpointId(3);
        STORAGE.storeCheckPoint(pipelineState);
        // Waiting for Kafka consumers to receive all records
        Thread.sleep(60000);
    }

    @Test
    public void testGetAllCheckpoints() throws CheckpointStorageException {
        List<PipelineState> pipelineStates = STORAGE.getAllCheckpoints(JOB_ID);
        Assertions.assertEquals(3, pipelineStates.size());
    }

    @Test
    public void testGetLatestCheckpoints() throws CheckpointStorageException {
        List<PipelineState> pipelineStates = STORAGE.getLatestCheckpoint(JOB_ID);
        Assertions.assertEquals(2, pipelineStates.size());
    }

    @Test
    public void testGetLatestCheckpointByJobIdAndPipelineId() throws CheckpointStorageException {
        PipelineState state = STORAGE.getLatestCheckpointByJobIdAndPipelineId(JOB_ID, "1");
        Assertions.assertEquals(2, state.getCheckpointId());
    }

    @Test
    public void testGetCheckpointsByJobIdAndPipelineId() throws CheckpointStorageException {
        List<PipelineState> state = STORAGE.getCheckpointsByJobIdAndPipelineId(JOB_ID, "1");
        Assertions.assertEquals(2, state.size());
    }

    @AfterAll
    public static void teardown() {
        STORAGE.deleteCheckpoint(JOB_ID);
        kafkaContainer.close();
    }
}
