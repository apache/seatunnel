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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit tests for DebeziumEventDispatcherConfig */
public class DebeziumEventDispatcherConfigTest {

    @Test
    public void testBuilderWithAllFields() {
        Object connectorConfig = new Object();
        MockTopicNaming topicNaming = new MockTopicNaming();
        Object databaseSchema = new Object();
        Object queue = new Object();
        Object dataCollectionFilter = new Object();
        Object changeEventCreator = new Object();
        Object metadataProvider = new Object();
        Object heartbeatFactory = new Object();
        Object schemaNameAdjuster = new Object();

        DebeziumEventDispatcherConfig config =
                DebeziumEventDispatcherConfig.builder()
                        .connectorConfig(connectorConfig)
                        .topicNaming(topicNaming)
                        .databaseSchema(databaseSchema)
                        .queue(queue)
                        .dataCollectionFilter(dataCollectionFilter)
                        .changeEventCreator(changeEventCreator)
                        .metadataProvider(metadataProvider)
                        .heartbeatFactory(heartbeatFactory)
                        .schemaNameAdjuster(schemaNameAdjuster)
                        .build();

        assertNotNull(config);
        assertEquals(connectorConfig, config.getConnectorConfig());
        assertEquals(topicNaming, config.getTopicNaming());
        assertEquals(databaseSchema, config.getDatabaseSchema());
        assertEquals(queue, config.getQueue());
        assertEquals(dataCollectionFilter, config.getDataCollectionFilter());
        assertEquals(changeEventCreator, config.getChangeEventCreator());
        assertEquals(metadataProvider, config.getMetadataProvider());
        assertEquals(heartbeatFactory, config.getHeartbeatFactory());
        assertEquals(schemaNameAdjuster, config.getSchemaNameAdjuster());
    }

    @Test
    public void testBuilderWithPartialFields() {
        MockTopicNaming topicNaming = new MockTopicNaming();
        Object queue = new Object();

        DebeziumEventDispatcherConfig config =
                DebeziumEventDispatcherConfig.builder()
                        .topicNaming(topicNaming)
                        .queue(queue)
                        .build();

        assertNotNull(config);
        assertNull(config.getConnectorConfig());
        assertEquals(topicNaming, config.getTopicNaming());
        assertNull(config.getDatabaseSchema());
        assertEquals(queue, config.getQueue());
        assertNull(config.getDataCollectionFilter());
        assertNull(config.getChangeEventCreator());
        assertNull(config.getMetadataProvider());
        assertNull(config.getHeartbeatFactory());
        assertNull(config.getSchemaNameAdjuster());
    }

    @Test
    public void testBuilderReuse() {
        MockTopicNaming topicNaming1 = new MockTopicNaming();
        MockTopicNaming topicNaming2 = new MockTopicNaming();

        DebeziumEventDispatcherConfig.Builder builder =
                DebeziumEventDispatcherConfig.builder().topicNaming(topicNaming1);

        DebeziumEventDispatcherConfig config1 = builder.build();
        assertEquals(topicNaming1, config1.getTopicNaming());

        // Reuse builder with different value
        DebeziumEventDispatcherConfig config2 = builder.topicNaming(topicNaming2).build();
        assertEquals(topicNaming2, config2.getTopicNaming());
    }

    @Test
    public void testEmptyBuilder() {
        DebeziumEventDispatcherConfig config = DebeziumEventDispatcherConfig.builder().build();

        assertNotNull(config);
        assertNull(config.getConnectorConfig());
        assertNull(config.getTopicNaming());
        assertNull(config.getDatabaseSchema());
        assertNull(config.getQueue());
        assertNull(config.getDataCollectionFilter());
        assertNull(config.getChangeEventCreator());
        assertNull(config.getMetadataProvider());
        assertNull(config.getHeartbeatFactory());
        assertNull(config.getSchemaNameAdjuster());
    }

    /** Mock implementation of DebeziumTopicNaming for testing */
    private static class MockTopicNaming implements DebeziumTopicNaming<Object> {
        @Override
        public String getPrimaryTopic() {
            return "primary-topic";
        }

        @Override
        public String getHeartbeatTopic() {
            return "heartbeat-topic";
        }

        @Override
        public String dataChangeTopicName(Object tableId) {
            return "data-change-topic";
        }
    }
}
