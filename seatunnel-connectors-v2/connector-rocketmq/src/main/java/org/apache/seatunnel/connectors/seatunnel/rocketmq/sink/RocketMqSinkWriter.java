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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.serialize.SeaTunnelRowSerializer;

import org.apache.rocketmq.common.message.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RocketMqSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final ProducerMetadata producerMetadata;
    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final RocketMqProducerSender rocketMqProducerSender;

    public RocketMqSinkWriter(
            ProducerMetadata producerMetadata, SeaTunnelRowType seaTunnelRowType) {
        this.producerMetadata = producerMetadata;
        this.seaTunnelRowSerializer = getSerializer(seaTunnelRowType);
        if (producerMetadata.isExactlyOnce()) {
            this.rocketMqProducerSender =
                    new RocketMqTransactionSender(producerMetadata.getConfiguration());
        } else {
            this.rocketMqProducerSender =
                    new RocketMqNoTransactionSender(
                            producerMetadata.getConfiguration(), producerMetadata.isSync());
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Message message = seaTunnelRowSerializer.serializeRow(element);
        rocketMqProducerSender.send(message);
    }

    @Override
    public void close() throws IOException {
        if (this.rocketMqProducerSender != null) {
            try {
                this.rocketMqProducerSender.close();
            } catch (Exception e) {
                throw new RocketMqConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED,
                        "Close RocketMq sink writer error",
                        e);
            }
        }
    }

    private SeaTunnelRowSerializer<byte[], byte[]> getSerializer(
            SeaTunnelRowType seaTunnelRowType) {
        return new DefaultSeaTunnelRowSerializer(
                producerMetadata.getTopic(),
                getPartitionKeyFields(seaTunnelRowType),
                seaTunnelRowType,
                producerMetadata.getFormat(),
                producerMetadata.getFieldDelimiter());
    }

    private List<String> getPartitionKeyFields(SeaTunnelRowType seaTunnelRowType) {
        if (producerMetadata.getPartitionKeyFields() == null) {
            return Collections.emptyList();
        }
        List<String> partitionKeyFields = producerMetadata.getPartitionKeyFields();
        // Check whether the key exists
        List<String> rowTypeFieldNames = Arrays.asList(seaTunnelRowType.getFieldNames());
        for (String partitionKeyField : partitionKeyFields) {
            if (!rowTypeFieldNames.contains(partitionKeyField)) {
                throw new RocketMqConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format(
                                "Partition key field not found: %s, rowType: %s",
                                partitionKeyField, rowTypeFieldNames));
            }
        }
        return partitionKeyFields;
    }
}
