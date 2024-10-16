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
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializerFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializerFactory;

import lombok.NonNull;

import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** use in elasticsearch version >= 2.x and <= 8.x */
public class ElasticsearchRowSerializer implements SeaTunnelRowSerializer {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
        Map<String, Object> document = toDocumentMap(row, seaTunnelRowType);
        String documentStr;

        try {
            documentStr = objectMapper.writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError(
                    "Elasticsearch", "document:" + document.toString(), e);
        }

        if (key != null) {
            Map<String, String> upsertMetadata = createMetadata(row, key);
            String upsertMetadataStr;
            try {
                upsertMetadataStr = objectMapper.writeValueAsString(upsertMetadata);
            } catch (JsonProcessingException e) {
                throw CommonError.jsonOperationError(
                        "Elasticsearch", "upsertMetadata:" + upsertMetadata.toString(), e);
            }

            /**
             * format example: { "update" : {"_index" : "${your_index}", "_id" :
             * "${your_document_id}"} }\n { "doc" : ${your_document_json}, "doc_as_upsert" : true }
             */
            return new StringBuilder()
                    .append("{ \"update\" :")
                    .append(upsertMetadataStr)
                    .append(" }")
                    .append("\n")
                    .append("{ \"doc\" :")
                    .append(documentStr)
                    .append(", \"doc_as_upsert\" : true }")
                    .toString();
        }

        Map<String, String> indexMetadata = createMetadata(row);
        String indexMetadataStr;
        try {
            indexMetadataStr = objectMapper.writeValueAsString(indexMetadata);
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError(
                    "Elasticsearch", "indexMetadata:" + indexMetadata.toString(), e);
        }

        /**
         * format example: { "index" : {"_index" : "${your_index}", "_id" : "${your_document_id}"}
         * }\n ${your_document_json}
         */
        return new StringBuilder()
                .append("{ \"index\" :")
                .append(indexMetadataStr)
                .append(" }")
                .append("\n")
                .append(documentStr)
                .toString();
    }

    private String serializeDelete(SeaTunnelRow row) {
        String key = keyExtractor.apply(row);
        Map<String, String> deleteMetadata = createMetadata(row, key);
        String deleteMetadataStr;
        try {
            deleteMetadataStr = objectMapper.writeValueAsString(deleteMetadata);
        } catch (JsonProcessingException e) {
            throw CommonError.jsonOperationError(
                    "Elasticsearch", "deleteMetadata:" + deleteMetadata.toString(), e);
        }

        /**
         * format example: { "delete" : {"_index" : "${your_index}", "_id" : "${your_document_id}"}
         * }
         */
        return new StringBuilder()
                .append("{ \"delete\" :")
                .append(deleteMetadataStr)
                .append(" }")
                .toString();
    }

    private Map<String, Object> toDocumentMap(SeaTunnelRow row, SeaTunnelRowType rowType) {
        String[] fieldNames = rowType.getFieldNames();
        Map<String, Object> doc = new HashMap<>(fieldNames.length);
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = fields[i];
            if (value == null) {
                doc.put(fieldNames[i], null);
            } else if (value instanceof SeaTunnelRow) {
                doc.put(
                        fieldNames[i],
                        toDocumentMap(
                                (SeaTunnelRow) value, (SeaTunnelRowType) rowType.getFieldType(i)));
            } else {
                doc.put(fieldNames[i], convertValue(value));
            }
        }
        return doc;
    }

    private Object convertValue(Object value) {
        if (value instanceof Temporal) {
            // jackson not support jdk8 new time api
            return value.toString();
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                ((Map) value).put(entry.getKey(), convertValue(entry.getValue()));
            }
            return value;
        } else if (value instanceof List) {
            for (int i = 0; i < ((List) value).size(); i++) {
                ((List) value).set(i, convertValue(((List) value).get(i)));
            }
            return value;
        } else {
            return value;
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
