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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import org.apache.rocketmq.common.message.MessageExt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RocketMqRecordEmitter
        implements RecordEmitter<MessageExt, SeaTunnelRow, RocketMQPartitionSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(RocketMqRecordEmitter.class);
    private final OutputCollector<SeaTunnelRow> outputCollector;
    protected final SourceReader.Context context;
    protected final Counter maxRecordFetchDelayOffset;
    protected final Counter maxRecordFetchDelay;
    // partition,maxDelay
    private final Map<Integer, Long> maxDelayOffsets;

    private final Map<Integer, Long> maxDelay;

    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public final String RECORD_FETCH_DELAY = "RecordFetchDelay";

    public final String RECORD_FETCH_DELAY_OFFSET = "RecordFetchDelayOffset";

    public RocketMqRecordEmitter(
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            SourceReader.Context context) {
        this.deserializationSchema = deserializationSchema;
        this.context = context;
        this.outputCollector = new OutputCollector<>();
        this.maxRecordFetchDelayOffset =
                context.getMetricsContext().counter(RECORD_FETCH_DELAY_OFFSET);
        this.maxRecordFetchDelay = context.getMetricsContext().counter(RECORD_FETCH_DELAY);
        this.maxDelayOffsets = new HashMap<>();
        this.maxDelay = new HashMap<>();
    }

    @Override
    public void emitRecord(
            MessageExt consumerRecord,
            Collector<SeaTunnelRow> collector,
            RocketMQPartitionSplitState splitState)
            throws Exception {
        outputCollector.output = collector;
        reportMetrics(consumerRecord);
        deserializationSchema.deserialize(consumerRecord.getBody(), outputCollector);
        // consumerRecord.offset + 1 is the offset commit to Kafka and also the start offset
        // for the next run
        splitState.setCurrentOffset(consumerRecord.getQueueOffset() + 1);
    }

    protected void reportMetrics(MessageExt consumerRecord) {
        long now = System.currentTimeMillis();
        // record process time
        if (consumerRecord.getStoreTimestamp() > 0L) {
            // report fetch delay
            long fetchDelay = now - consumerRecord.getStoreTimestamp();
            long currnetDelay = fetchDelay > 0 ? fetchDelay : 0;
            maxDelay.put(consumerRecord.getQueueId(), currnetDelay);
            maxRecordFetchDelay.set(maxDelay.values().stream().max(Long::compareTo).get());
        }
        // report max offset
        maxDelayOffsets.put(consumerRecord.getQueueId(), consumerRecord.getQueueOffset());
        maxRecordFetchDelayOffset.set(maxDelayOffsets.values().stream().max(Long::compareTo).get());
    }

    private static class OutputCollector<T> implements Collector<T> {
        private Collector<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void collect(SchemaChangeEvent event) {
            output.collect(event);
        }

        @Override
        public void markSchemaChangeBeforeCheckpoint() {
            output.markSchemaChangeBeforeCheckpoint();
        }

        @Override
        public void markSchemaChangeAfterCheckpoint() {
            output.markSchemaChangeAfterCheckpoint();
        }

        @Override
        public Object getCheckpointLock() {
            return output.getCheckpointLock();
        }
    }
}
