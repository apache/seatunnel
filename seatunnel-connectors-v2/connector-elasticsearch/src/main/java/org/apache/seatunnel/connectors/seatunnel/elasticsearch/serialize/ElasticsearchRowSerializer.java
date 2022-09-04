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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchVersion;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.index.IndexSerializerFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.type.IndexTypeSerializerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * use in elasticsearch version >= 7.*
 */
public class ElasticsearchRowSerializer implements SeaTunnelRowSerializer {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final IndexSerializer indexSerializer;

    private final IndexTypeSerializer indexTypeSerializer;

    public ElasticsearchRowSerializer(ElasticsearchVersion elasticsearchVersion, IndexInfo indexInfo, SeaTunnelRowType seaTunnelRowType) {
        this.indexTypeSerializer = IndexTypeSerializerFactory.getIndexTypeSerializer(elasticsearchVersion, indexInfo.getType());
        this.indexSerializer = IndexSerializerFactory.getIndexSerializer(indexInfo.getIndex(), seaTunnelRowType);
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public String serializeRow(SeaTunnelRow row) {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Map<String, Object> doc = new HashMap<>(fieldNames.length);
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldNames.length; i++) {
            doc.put(fieldNames[i], fields[i]);
        }

        StringBuilder sb = new StringBuilder();

        Map<String, String> indexInner = new HashMap<>();
        String index = indexSerializer.serialize(row);
        indexInner.put("_index", index);
        indexTypeSerializer.fillType(indexInner);

        Map<String, Map<String, String>> indexParam = new HashMap<>();
        indexParam.put("index", indexInner);
        try {
            sb.append(objectMapper.writeValueAsString(indexParam));
            sb.append("\n");
            String indexDoc = objectMapper.writeValueAsString(doc);
            sb.append(indexDoc);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Object json deserialization exception.", e);
        }

        return sb.toString();
    }
}
