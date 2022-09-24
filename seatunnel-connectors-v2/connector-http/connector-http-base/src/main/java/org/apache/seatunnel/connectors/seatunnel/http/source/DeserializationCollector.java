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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
public class DeserializationCollector {

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    public void collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        if (deserializationSchema instanceof JsonDeserializationSchema) {
            collectJson(message, (JsonDeserializationSchema) deserializationSchema, out);
        } else {
            SeaTunnelRow deserialize = deserializationSchema.deserialize(message);
            out.collect(deserialize);
        }
    }

    private void collectJson(byte[] message,
                             JsonDeserializationSchema jsonDeserializationSchema,
                             Collector<SeaTunnelRow> out) throws IOException {
        JsonNode jsonNode = jsonDeserializationSchema.convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow deserialize = jsonDeserializationSchema.convertJsonNode(arrayNode.get(i));
                out.collect(deserialize);
            }
        } else {
            SeaTunnelRow deserialize = jsonDeserializationSchema.convertJsonNode(jsonNode);
            out.collect(deserialize);
        }
    }
}
