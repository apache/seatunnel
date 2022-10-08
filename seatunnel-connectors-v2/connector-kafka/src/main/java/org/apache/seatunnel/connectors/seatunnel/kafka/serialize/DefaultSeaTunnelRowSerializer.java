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

package org.apache.seatunnel.connectors.seatunnel.kafka.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer<byte[], byte[]> {

    private int partation = -1;
    private final String topic;
    private final JsonSerializationSchema jsonSerializationSchema;

    public DefaultSeaTunnelRowSerializer(String topic, SeaTunnelRowType seaTunnelRowType) {
        this.topic = topic;
        this.jsonSerializationSchema = new JsonSerializationSchema(seaTunnelRowType);
    }

    public DefaultSeaTunnelRowSerializer(String topic, int partation, SeaTunnelRowType seaTunnelRowType) {
        this(topic, seaTunnelRowType);
        this.partation = partation;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serializeRow(SeaTunnelRow row) {
        if (this.partation != -1) {
            return new ProducerRecord<>(topic, this.partation, null, jsonSerializationSchema.serialize(row));
        }
        else {
            return new ProducerRecord<>(topic, null, jsonSerializationSchema.serialize(row));
        }
    }
}
