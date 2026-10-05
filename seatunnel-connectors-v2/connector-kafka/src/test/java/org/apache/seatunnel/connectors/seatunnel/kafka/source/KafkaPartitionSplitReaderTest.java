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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Tests the Kafka source reader consumer property mapping.
 *
 * <p>This guards the offset commit contract between SeaTunnel checkpoint commits and Kafka auto
 * commit.
 */
public class KafkaPartitionSplitReaderTest {

    /** Verifies that checkpoint commits disable Kafka auto commit. */
    @Test
    void testBuildConsumerPropertiesDisablesAutoCommitWhenCheckpointCommitEnabled() {
        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(createConfig(true));

        Properties properties =
                KafkaPartitionSplitReader.buildConsumerProperties(
                        kafkaSourceConfig, 1, "seatunnel-test");

        Assertions.assertEquals(
                "false", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    /** Verifies that Kafka auto commit stays enabled when checkpoint commits are disabled. */
    @Test
    void testBuildConsumerPropertiesEnablesAutoCommitWhenCheckpointCommitDisabled() {
        KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(createConfig(false));

        Properties properties =
                KafkaPartitionSplitReader.buildConsumerProperties(
                        kafkaSourceConfig, 2, "seatunnel-test");

        Assertions.assertEquals(
                "true", properties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    /** Creates the minimum Kafka source config used by the tests. */
    private ReadonlyConfig createConfig(boolean commitOnCheckpoint) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bootstrap.servers", "localhost:9092");
        configMap.put("group.id", "test-group");
        configMap.put("topic", "test-topic");
        configMap.put("schema", createSchema());
        configMap.put("format", "text");
        configMap.put("commit_on_checkpoint", commitOnCheckpoint);
        return ReadonlyConfig.fromMap(configMap);
    }

    /** Creates a minimal text schema for constructing {@link KafkaSourceConfig}. */
    private Map<String, Object> createSchema() {
        Map<String, Object> schemaFields = new HashMap<>();
        schemaFields.put("id", "int");

        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", schemaFields);
        return schema;
    }
}
