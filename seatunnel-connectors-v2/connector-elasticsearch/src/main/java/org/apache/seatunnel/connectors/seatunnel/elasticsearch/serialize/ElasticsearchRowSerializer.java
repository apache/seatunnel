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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializerFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializerFactory;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** use in elasticsearch version >= 2.x and <= 8.x */
public class ElasticsearchRowSerializer implements SeaTunnelRowSerializer {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final JsonSerializationSchema serializationSchema;

    private final IndexSerializer indexSerializer;

    private final IndexTypeSerializer indexTypeSerializer;
    private final Function<SeaTunnelRow, String> keyExtractor;

    public ElasticsearchRowSerializer(
            ElasticsearchClusterInfo elasticsearchClusterInfo,
            IndexInfo indexInfo,
            SeaTunnelRowType seaTunnelRowType) {
        this.indexTypeSerializer =
                IndexTypeSerializerFactory.getIndexTypeSerializer(
                        elasticsearchClusterInfo, indexInfo.getType());
        this.indexSerializer =
                IndexSerializerFactory.getIndexSerializer(indexInfo.getIndex(), seaTunnelRowType);
        this.seaTunnelRowType = seaTunnelRowType;
        this.keyExtractor =
                KeyExtractor.createKeyExtractor(
                        seaTunnelRowType, indexInfo.getPrimaryKeys(), indexInfo.getKeyDelimiter());
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
    }

    @Override
    public String serializeRow(SeaTunnelRow row) {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                return serializeUpsert(row);
            case UPDATE_BEFORE:
            case DELETE:
                return serializeDelete(row);
            default:
                throw new ElasticsearchConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported write row kind: " + row.getRowKind());
        }
    }

    private String serializeUpsert(SeaTunnelRow row) {
        String key = keyExtractor.apply(row);
        String document = new String(serializationSchema.serialize(row));

        try {
            if (key != null) {
                Map<String, String> upsertMetadata = createMetadata(row, key);
                /**
                 * format example: { "update" : {"_index" : "${your_index}", "_id" :
                 * "${your_document_id}"} }\n { "doc" : ${your_document_json}, "doc_as_upsert" :
                 * true }
                 */
                return new StringBuilder()
                        .append("{ \"update\" :")
                        .append(objectMapper.writeValueAsString(upsertMetadata))
                        .append("}")
                        .append("\n")
                        .append("{ \"doc\" :")
                        .append(document)
                        .append(", \"doc_as_upsert\" : true }")
                        .toString();
            } else {
                Map<String, String> indexMetadata = createMetadata(row);
                /**
                 * format example: { "index" : {"_index" : "${your_index}", "_id" :
                 * "${your_document_id}"} }\n ${your_document_json}
                 */
                return new StringBuilder()
                        .append("{ \"index\" :")
                        .append(objectMapper.writeValueAsString(indexMetadata))
                        .append("}")
                        .append("\n")
                        .append(document)
                        .toString();
            }
        } catch (JsonProcessingException e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.JSON_OPERATION_FAILED,
                    "Object json deserialization exception.",
                    e);
        }
    }

    private String serializeDelete(SeaTunnelRow row) {
        String key = keyExtractor.apply(row);
        Map<String, String> deleteMetadata = createMetadata(row, key);
        try {
            /**
             * format example: { "delete" : {"_index" : "${your_index}", "_id" :
             * "${your_document_id}"} }
             */
            return new StringBuilder()
                    .append("{ \"delete\" :")
                    .append(objectMapper.writeValueAsString(deleteMetadata))
                    .append("}")
                    .toString();
        } catch (JsonProcessingException e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.JSON_OPERATION_FAILED,
                    "Object json deserialization exception.",
                    e);
        }
    }

    private Map<String, String> createMetadata(@NonNull SeaTunnelRow row, @NonNull String key) {
        Map<String, String> actionMetadata = createMetadata(row);
        actionMetadata.put("_id", key);
        return actionMetadata;
    }

    private Map<String, String> createMetadata(@NonNull SeaTunnelRow row) {
        Map<String, String> actionMetadata = new HashMap<>(2);
        actionMetadata.put("_index", indexSerializer.serialize(row));
        indexTypeSerializer.fillType(actionMetadata);
        return actionMetadata;
    }
}
