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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqMessageFormat;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.protobuf.ProtobufSerializationSchema;

import java.util.Optional;

public class RabbitmqSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private RabbitmqClient rabbitMQClient;
    private final SerializationSchema serializationSchema;

    public RabbitmqSinkWriter(RabbitmqConfig config, SeaTunnelRowType seaTunnelRowType) {
        this.rabbitMQClient = new RabbitmqClient(config);
        try {
            this.rabbitMQClient.setupQueue();
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup RabbitMQ queue", e);
        }
        this.serializationSchema = createSerializationSchema(config, seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) {
        rabbitMQClient.write(serializationSchema.serialize(element));
    }

    @Override
    public Optional prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void close() {
        if (rabbitMQClient != null) {
            rabbitMQClient.close();
        }
    }

    private SerializationSchema createSerializationSchema(
            RabbitmqConfig config, SeaTunnelRowType seaTunnelRowType) {
        RabbitmqMessageFormat format = config.getFormat();
        if (format == null) {
            format = RabbitmqMessageFormat.JSON;
        }
        switch (format) {
            case JSON:
                return new JsonSerializationSchema(seaTunnelRowType);
            case PROTOBUF:
                return new ProtobufSerializationSchema(
                        seaTunnelRowType,
                        config.getProtobufMessageName(),
                        config.getProtobufSchema());
            default:
                throw new IllegalArgumentException(
                        "Unsupported RabbitMQ message format: " + format);
        }
    }
}
