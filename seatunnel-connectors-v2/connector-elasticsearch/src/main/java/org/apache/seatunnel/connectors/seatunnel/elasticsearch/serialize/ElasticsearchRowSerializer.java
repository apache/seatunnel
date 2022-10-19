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

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
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
import org.apache.commons.collections4.CollectionUtils;

import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * use in elasticsearch version >= 7.*
 */
public class ElasticsearchRowSerializer implements SeaTunnelRowSerializer {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final IndexInfo indexInfo;
    private final IndexSerializer indexSerializer;
    private final IndexTypeSerializer indexTypeSerializer;
    private boolean hasKeys;

    public ElasticsearchRowSerializer(ElasticsearchVersion elasticsearchVersion, IndexInfo indexInfo, SeaTunnelRowType seaTunnelRowType) {
        this.indexTypeSerializer = IndexTypeSerializerFactory.getIndexTypeSerializer(elasticsearchVersion, indexInfo.getType());
        this.indexSerializer = IndexSerializerFactory.getIndexSerializer(indexInfo.getIndex(), seaTunnelRowType);
        this.seaTunnelRowType = seaTunnelRowType;
        this.indexInfo = indexInfo;
        this.hasKeys = CollectionUtils.isNotEmpty(indexInfo.getIds());
    }

    @Override
    public String serializeRow(SeaTunnelRow row) {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Map<String, Object> doc = new HashMap<>(fieldNames.length);
        Object[] fields = row.getFields();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
            Object value = fields[i];
            if (value instanceof Temporal){
                //jackson not support jdk8 new time api
                doc.put(fieldNames[i], value.toString());
            } else {
                doc.put(fieldNames[i], value);
            }
            if (hasKeys && indexInfo.getIds().stream().filter(f -> f.equalsIgnoreCase(name)).count() > 0) {
                keys.add(convertKey(seaTunnelRowType.getFieldType(i), value));
            }
        }

        StringBuilder sb = new StringBuilder();

        Map<String, String> indexInner = new HashMap<>();
        String index = indexSerializer.serialize(row);
        indexInner.put("_index", index);
        if (hasKeys && keys.size() > 0) {
            indexInner.put("_id", keys.stream().collect(Collectors.joining("+")));
        }
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

    private String convertKey(SeaTunnelDataType<?> dataType, Object value) {
        if (dataType instanceof BasicType
                || dataType instanceof DecimalType
                || dataType instanceof PrimitiveByteArrayType
                || dataType instanceof LocalTimeType) {
            return String.valueOf(value);
        } else if (dataType instanceof MapType) {
            Map<Object, Object> result = (Map<Object, Object>) value;
            return result.values().stream().map(String::valueOf).collect(Collectors.joining("+"));
        } else if (dataType instanceof ArrayType) {
            return Stream.of((Object[]) value).map(String::valueOf).collect(Collectors.joining("+"));
        } else if (dataType instanceof SeaTunnelRowType) {
            List<String> list = new ArrayList<>();
            SeaTunnelRowType seaTunnelRowType = (SeaTunnelRowType) dataType;
            Object[] fields = ((SeaTunnelRow) value).getFields();
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                list.add(convertKey(seaTunnelRowType.getFieldType(i), fields[i]));
            }
            return list.stream().collect(Collectors.joining("+"));
        } else {
            throw new IllegalArgumentException("Not supported data type: " + dataType);
        }
    }
}
