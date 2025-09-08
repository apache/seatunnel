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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.IndexInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class ElasticsearchRowSerializerTest {
    @Test
    public void testSerializeUpsert() {
        String index = "st_index";
        String primaryKey = "id";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(ElasticsearchSinkOptions.INDEX.key(), index);
        confMap.put(ElasticsearchSinkOptions.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder().clusterVersion("8.0.0").build();
        IndexInfo indexInfo = new IndexInfo(index, pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {primaryKey, "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});

        final ElasticsearchRowSerializer serializer =
                new ElasticsearchRowSerializer(clusterInfo, indexInfo, schema);

        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);

        String expected =
                "{ \"update\" :{\"_index\":\""
                        + index
                        + "\",\"_id\":\""
                        + id
                        + "\"} }\n"
                        + "{ \"doc\" :{\"name\":\""
                        + name
                        + "\",\"id\":\""
                        + id
                        + "\"}, \"doc_as_upsert\" : true }";

        String upsertStr = serializer.serializeRow(row);
        Assertions.assertEquals(expected, upsertStr);
    }

    @Test
    public void testSerializeUpsertWithoutKey() {
        String index = "st_index";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(ElasticsearchSinkOptions.INDEX.key(), index);

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder().clusterVersion("8.0.0").build();
        IndexInfo indexInfo = new IndexInfo(index, pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});

        final ElasticsearchRowSerializer serializer =
                new ElasticsearchRowSerializer(clusterInfo, indexInfo, schema);

        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);

        String expected =
                "{ \"index\" :{\"_index\":\""
                        + index
                        + "\"} }\n"
                        + "{\"name\":\""
                        + name
                        + "\",\"id\":\""
                        + id
                        + "\"}";

        String upsertStr = serializer.serializeRow(row);
        Assertions.assertEquals(expected, upsertStr);
    }

    @Test
    public void testSerializeUpsertDocumentError() {
        String index = "st_index";
        String primaryKey = "id";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(ElasticsearchSinkOptions.INDEX.key(), index);
        confMap.put(ElasticsearchSinkOptions.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder().clusterVersion("8.0.0").build();
        IndexInfo indexInfo = new IndexInfo(index, pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {primaryKey, "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});

        final ElasticsearchRowSerializer serializer =
                new ElasticsearchRowSerializer(clusterInfo, indexInfo, schema);

        String id = "0001";
        Object mockObj = new Object();
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, mockObj});
        row.setRowKind(RowKind.UPDATE_AFTER);

        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put(primaryKey, id);
        expectedMap.put("name", mockObj);

        SeaTunnelRuntimeException expected =
                CommonError.jsonOperationError(
                        "Elasticsearch", "document:" + expectedMap.toString());
        SeaTunnelRuntimeException actual =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> serializer.serializeRow(row));
        Assertions.assertEquals(expected.getMessage(), actual.getMessage());
    }

    @Test
    public void testSerializeDelete() {
        String index = "st_index";
        String primaryKey = "id";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(ElasticsearchSinkOptions.INDEX.key(), index);
        confMap.put(ElasticsearchSinkOptions.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        ElasticsearchClusterInfo clusterInfo =
                ElasticsearchClusterInfo.builder().clusterVersion("8.0.0").build();
        IndexInfo indexInfo = new IndexInfo(index, pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {primaryKey, "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});

        final ElasticsearchRowSerializer serializer =
                new ElasticsearchRowSerializer(clusterInfo, indexInfo, schema);

        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.DELETE);

        String expected = "{ \"delete\" :{\"_index\":\"" + index + "\",\"_id\":\"" + id + "\"} }";

        String upsertStr = serializer.serializeRow(row);
        Assertions.assertEquals(expected, upsertStr);
    }
}
