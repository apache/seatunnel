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

package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;

import org.bson.Document;

public class RowDataDocumentSerializer implements DocumentSerializer<SeaTunnelRow> {

    private final RowDataToJsonConverters.RowDataToJsonConverter jsonConverter;

    private transient ObjectNode node;
    private final ObjectMapper mapper = new ObjectMapper();

    public RowDataDocumentSerializer(SeaTunnelDataType<?> type) {
        this.jsonConverter = new RowDataToJsonConverters().createConverter(type);
    }

    @Override
    public Document serialize(SeaTunnelRow row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }
        try {
            jsonConverter.convert(mapper, node, row);
            String s = mapper.writeValueAsString(node);
            return Document.parse(s);
        } catch (JsonProcessingException e) {
            throw new MongodbConnectorException(
                    CommonErrorCode.SERIALIZE_OPERATION_FAILED,
                    "can not serialize row '" + row + "'. ",
                    e);
        }
    }
}
