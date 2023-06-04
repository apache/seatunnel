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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.format;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.canal.CanalJsonDeserializationSchema;

import java.io.IOException;
import java.util.Iterator;

/**
 * for pulsar-connector, the data format is
 *
 * <p>{ "id":0, "message":"[{pulsar-data based on canal}]", "timestamp":"" }
 */
public class PulsarCanalDecorator implements DeserializationSchema<SeaTunnelRow> {

    private static final String MESSAGE = "message";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    public static final String COLUMN_NAME = "columnName";
    public static final String COLUMN_VALUE = "columnValue";
    public static final String COLUMN_INDEX = "index";

    private final CanalJsonDeserializationSchema canalJsonDeserializationSchema;

    public PulsarCanalDecorator(CanalJsonDeserializationSchema canalJsonDeserializationSchema) {
        this.canalJsonDeserializationSchema = canalJsonDeserializationSchema;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deserialize(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        ObjectNode pulsarCanal = JsonUtils.parseObject(message);
        ArrayNode canalList = JsonUtils.parseArray(pulsarCanal.get(MESSAGE).asText());
        Iterator<JsonNode> canalIterator = canalList.elements();
        while (canalIterator.hasNext()) {
            JsonNode next = canalIterator.next();
            // reconvert pulsar handler, reference to
            // https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/java/org/apache/pulsar/io/canal/MessageUtils.java
            ObjectNode root = reconvertPulsarData((ObjectNode) next);
            canalJsonDeserializationSchema.deserialize(root, out);
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return canalJsonDeserializationSchema.getProducedType();
    }

    private ObjectNode reconvertPulsarData(ObjectNode root) {
        root.replace(FIELD_DATA, reconvert(root.get(FIELD_DATA)));
        root.replace(FIELD_OLD, reconvert(root.get(FIELD_OLD)));
        return root;
    }

    private JsonNode reconvert(JsonNode node) {
        if (!(node instanceof ArrayNode) || node.size() <= 0) {
            return node;
        }
        long firstColumn = node.get(0).get(COLUMN_INDEX).asLong();
        ArrayNode arrayNode = JsonUtils.createArrayNode();
        ObjectNode rowMap = JsonUtils.createObjectNode();
        for (int i = 0; i < node.size(); i++) {
            ObjectNode columnNode = (ObjectNode) node.get(i);
            if (firstColumn == columnNode.get(COLUMN_INDEX).asLong()) {
                arrayNode.add(rowMap);
                rowMap = JsonUtils.createObjectNode();
            }
            rowMap.set(columnNode.get(COLUMN_NAME).asText(), columnNode.get(COLUMN_VALUE));
        }
        arrayNode.add(rowMap);
        arrayNode.remove(0);
        return arrayNode;
    }
}
