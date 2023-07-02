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

import org.apache.seatunnel.engine.imap.storage.kafka.config.KafkaConfigurationConstants;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.MAC;

@EnabledOnOs({LINUX, MAC})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
public class IMapKafkaStorageTest {
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.9";
    private static String KAFKA_BOOTSTRAP_SERVERS;
    private static final Map<String, Object> config;
    private static final IMapKafkaStorage storage;
    private static KafkaContainer kafkaContainer;

    static {
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(KAFKA_IMAGE_NAME)));
        Startables.deepStart(Stream.of(kafkaContainer)).join();
        log.info("Kafka container started");

        KAFKA_BOOTSTRAP_SERVERS = kafkaContainer.getBootstrapServers();
        config = new HashMap<>();
        config.put(KafkaConfigurationConstants.KAFKA_BOOTSTRAP_SERVERS, KAFKA_BOOTSTRAP_SERVERS);
        config.put(KafkaConfigurationConstants.BUSINESS_KEY, "storage-test");
        config.put(KafkaConfigurationConstants.KAFKA_STORAGE_COMPACT_TOPIC_PREFIX, "imap-");
        config.put(KafkaConfigurationConstants.KAFKA_STORAGE_COMPACT_TOPIC_REPLICATION_FACTOR, 1);
        storage = new IMapKafkaStorage();
        storage.initialize(config);
    }

    @Test
    void testAll() throws IOException {

        List<Object> keys = new ArrayList<>();
        String key1Index = "key1";
        String key2Index = "key2";
        String key50Index = "key50";

        AtomicInteger dataSize = new AtomicInteger();
        Long keyValue = 123456789L;
        for (int i = 0; i < 100; i++) {
            String key = "key" + i;
            Long value = System.currentTimeMillis();

            if (i == 50) {
                // delete
                storage.delete(key1Index);
                keys.remove(key1Index);
                // update
                storage.store(key2Index, keyValue);
                keys.add(key2Index);
                value = keyValue;
                new Thread(
                                () -> {
                                    try {
                                        dataSize.set(storage.loadAll().size());
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .start();
            }
            storage.store(key, value);
            keys.add(key);
            storage.delete(key1Index);
            keys.remove(key1Index);
        }

        await().atMost(1, TimeUnit.SECONDS).until(dataSize::get, size -> size > 0);
        Map<Object, Object> loadAllDatas = storage.loadAll();
        Assertions.assertTrue(dataSize.get() >= 50);
        Assertions.assertEquals(keyValue, loadAllDatas.get(key50Index));
        Assertions.assertEquals(keyValue, loadAllDatas.get(key2Index));
        Assertions.assertNull(loadAllDatas.get(key1Index));

        storage.deleteAll(keys);
    }

    @AfterAll
    public void destroy() {
        this.storage.destroy(true);
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}
