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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.util.Optional;

public class RabbitmqSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private RabbitmqClient rabbitMQClient;
    private final JsonSerializationSchema jsonSerializationSchema;

    public RabbitmqSinkWriter(RabbitmqConfig config, SeaTunnelRowType seaTunnelRowType) {
        this.rabbitMQClient = new RabbitmqClient(config);
        this.jsonSerializationSchema = new JsonSerializationSchema(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow element) {
        rabbitMQClient.write(jsonSerializationSchema.serialize(element));
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
}
