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

/** Configuration for creating event dispatchers. */
public class DebeziumEventDispatcherConfig {

    private final Object connectorConfig;
    private final DebeziumTopicNaming<?> topicNaming;
    private final Object databaseSchema;
    private final Object queue;
    private final Object dataCollectionFilter;
    private final Object changeEventCreator;
    private final Object metadataProvider;
    private final Object heartbeatFactory;
    private final Object schemaNameAdjuster;

    private DebeziumEventDispatcherConfig(Builder builder) {
        this.connectorConfig = builder.connectorConfig;
        this.topicNaming = builder.topicNaming;
        this.databaseSchema = builder.databaseSchema;
        this.queue = builder.queue;
        this.dataCollectionFilter = builder.dataCollectionFilter;
        this.changeEventCreator = builder.changeEventCreator;
        this.metadataProvider = builder.metadataProvider;
        this.heartbeatFactory = builder.heartbeatFactory;
        this.schemaNameAdjuster = builder.schemaNameAdjuster;
    }

    public Object getConnectorConfig() {
        return connectorConfig;
    }

    public DebeziumTopicNaming<?> getTopicNaming() {
        return topicNaming;
    }

    public Object getDatabaseSchema() {
        return databaseSchema;
    }

    public Object getQueue() {
        return queue;
    }

    public Object getDataCollectionFilter() {
        return dataCollectionFilter;
    }

    public Object getChangeEventCreator() {
        return changeEventCreator;
    }

    public Object getMetadataProvider() {
        return metadataProvider;
    }

    public Object getHeartbeatFactory() {
        return heartbeatFactory;
    }

    public Object getSchemaNameAdjuster() {
        return schemaNameAdjuster;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for DebeziumEventDispatcherConfig */
    public static class Builder {
        private Object connectorConfig;
        private DebeziumTopicNaming<?> topicNaming;
        private Object databaseSchema;
        private Object queue;
        private Object dataCollectionFilter;
        private Object changeEventCreator;
        private Object metadataProvider;
        private Object heartbeatFactory;
        private Object schemaNameAdjuster;

        public Builder connectorConfig(Object connectorConfig) {
            this.connectorConfig = connectorConfig;
            return this;
        }

        public Builder topicNaming(DebeziumTopicNaming<?> topicNaming) {
            this.topicNaming = topicNaming;
            return this;
        }

        public Builder databaseSchema(Object databaseSchema) {
            this.databaseSchema = databaseSchema;
            return this;
        }

        public Builder queue(Object queue) {
            this.queue = queue;
            return this;
        }

        public Builder dataCollectionFilter(Object dataCollectionFilter) {
            this.dataCollectionFilter = dataCollectionFilter;
            return this;
        }

        public Builder changeEventCreator(Object changeEventCreator) {
            this.changeEventCreator = changeEventCreator;
            return this;
        }

        public Builder metadataProvider(Object metadataProvider) {
            this.metadataProvider = metadataProvider;
            return this;
        }

        public Builder heartbeatFactory(Object heartbeatFactory) {
            this.heartbeatFactory = heartbeatFactory;
            return this;
        }

        public Builder schemaNameAdjuster(Object schemaNameAdjuster) {
            this.schemaNameAdjuster = schemaNameAdjuster;
            return this;
        }

        public DebeziumEventDispatcherConfig build() {
            return new DebeziumEventDispatcherConfig(this);
        }
    }
}
