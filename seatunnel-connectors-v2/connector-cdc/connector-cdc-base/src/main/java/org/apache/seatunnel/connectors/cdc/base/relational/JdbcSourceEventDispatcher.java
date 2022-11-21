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

package org.apache.seatunnel.connectors.cdc.base.relational;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/**
 * A subclass implementation of {@link EventDispatcher}.
 *
 * <pre>
 *  1. This class shares one {@link ChangeEventQueue} between multiple readers.
 *  2. This class override some methods for dispatching {@link HistoryRecord} directly,
 *     this is useful for downstream to deserialize the {@link HistoryRecord} back.
 * </pre>
 */
public class JdbcSourceEventDispatcher extends EventDispatcher<TableId> {

    private final ChangeEventQueue<DataChangeEvent> queue;

    private final String topic;

    public JdbcSourceEventDispatcher(
            CommonConnectorConfig connectorConfig,
            TopicSelector<TableId> topicSelector,
            DatabaseSchema<TableId> schema,
            ChangeEventQueue<DataChangeEvent> queue,
            DataCollectionFilters.DataCollectionFilter<TableId> filter,
            ChangeEventCreator changeEventCreator,
            EventMetadataProvider metadataProvider,
            SchemaNameAdjuster schemaNameAdjuster) {
        super(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                schemaNameAdjuster);
        this.queue = queue;
        this.topic = topicSelector.getPrimaryTopic();
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    public void dispatchWatermarkEvent(
        Map<String, ?> sourcePartition,
        SourceSplitBase sourceSplit,
        Offset watermark,
        WatermarkKind watermarkKind)
        throws InterruptedException {

        SourceRecord sourceRecord =
            WatermarkEvent.create(
                sourcePartition, topic, sourceSplit.splitId(), watermarkKind, watermark);
        queue.enqueue(new DataChangeEvent(sourceRecord));
    }
}
