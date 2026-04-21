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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.adapter;

import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.debezium.DebeziumEventDispatcherConfig;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.util.Map;

public class MySqlEventDispatcherAdapter implements DebeziumEventDispatcher<MySqlPartition> {

    private final JdbcSourceEventDispatcher<MySqlPartition> delegate;

    @SuppressWarnings("unchecked")
    public MySqlEventDispatcherAdapter(DebeziumEventDispatcherConfig config) {
        CommonConnectorConfig connectorConfig = (CommonConnectorConfig) config.getConnectorConfig();
        TopicSelector<TableId> topicSelector = (TopicSelector<TableId>) config.getTopicNaming();
        DatabaseSchema<TableId> schema = (DatabaseSchema<TableId>) config.getDatabaseSchema();
        ChangeEventQueue<DataChangeEvent> queue =
                (ChangeEventQueue<DataChangeEvent>) config.getQueue();
        DataCollectionFilter<TableId> filter =
                (DataCollectionFilter<TableId>) config.getDataCollectionFilter();
        ChangeEventCreator changeEventCreator = (ChangeEventCreator) config.getChangeEventCreator();
        EventMetadataProvider metadataProvider =
                (EventMetadataProvider) config.getMetadataProvider();
        HeartbeatFactory<TableId> heartbeatFactory =
                (HeartbeatFactory<TableId>) config.getHeartbeatFactory();
        SchemaNameAdjuster schemaNameAdjuster = (SchemaNameAdjuster) config.getSchemaNameAdjuster();

        this.delegate =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        schema,
                        queue,
                        filter,
                        changeEventCreator,
                        metadataProvider,
                        heartbeatFactory,
                        schemaNameAdjuster);
    }

    @Override
    public void dispatchWatermarkEvent(
            Map<String, ?> sourcePartition,
            String splitId,
            WatermarkKind watermarkKind,
            Offset offset)
            throws InterruptedException {
        SourceRecord sourceRecord =
                WatermarkEvent.create(
                        sourcePartition, getPrimaryTopic(), splitId, watermarkKind, offset);
        ChangeEventQueue<DataChangeEvent> queue = (ChangeEventQueue<DataChangeEvent>) getQueue();
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    @Override
    public Object getQueue() {
        return delegate.getQueue();
    }

    @Override
    public String getPrimaryTopic() {
        return delegate.getPrimaryTopic();
    }

    @Override
    public void close() {}

    public JdbcSourceEventDispatcher<MySqlPartition> getDelegate() {
        return delegate;
    }
}
