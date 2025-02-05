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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.kafka.config.MessageFormatErrorHandleWay;
import org.apache.seatunnel.format.compatible.kafka.connect.json.CompatibleKafkaConnectDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaRecordEmitter
        implements RecordEmitter<
                ConsumerRecord<byte[], byte[]>, SeaTunnelRow, KafkaSourceSplitState> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordEmitter.class);
    private final Map<TablePath, ConsumerMetadata> mapMetadata;
    private final OutputCollector<SeaTunnelRow> outputCollector;
    private final MessageFormatErrorHandleWay messageFormatErrorHandleWay;

    public KafkaRecordEmitter(
            Map<TablePath, ConsumerMetadata> mapMetadata,
            MessageFormatErrorHandleWay messageFormatErrorHandleWay) {
        this.mapMetadata = mapMetadata;
        this.messageFormatErrorHandleWay = messageFormatErrorHandleWay;
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord,
            Collector<SeaTunnelRow> collector,
            KafkaSourceSplitState splitState)
            throws Exception {
        outputCollector.output = collector;
        // todo there is an additional loss in this place for non-multi-table scenarios
        DeserializationSchema<SeaTunnelRow> deserializationSchema =
                mapMetadata.get(splitState.getTablePath()).getDeserializationSchema();
        try {
            if (deserializationSchema instanceof CompatibleKafkaConnectDeserializationSchema) {
                ((CompatibleKafkaConnectDeserializationSchema) deserializationSchema)
                        .deserialize(consumerRecord, outputCollector);
            } else {
                deserializationSchema.deserialize(consumerRecord.value(), outputCollector);
            }
        } catch (Exception e) {
            if (this.messageFormatErrorHandleWay == MessageFormatErrorHandleWay.SKIP) {
                logger.warn(
                        "Deserialize message failed, skip this message, message: {}",
                        new String(consumerRecord.value()));
            } else {
                throw e;
            }
        }
        // consumerRecord.offset + 1 is the offset commit to Kafka and also the start offset
        // for the next run
        splitState.setCurrentOffset(consumerRecord.offset() + 1);
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
