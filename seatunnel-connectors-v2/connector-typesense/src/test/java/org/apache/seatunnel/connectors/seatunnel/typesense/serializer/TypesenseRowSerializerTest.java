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

package org.apache.seatunnel.connectors.seatunnel.typesense.serializer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.typesense.dto.CollectionInfo;
import org.apache.seatunnel.connectors.seatunnel.typesense.serialize.sink.TypesenseRowSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class TypesenseRowSerializerTest {
    @Test
    public void testSerializeUpsert() {
        String collection = "test";
        String primaryKey = "id";
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(TypesenseBaseOptions.COLLECTION.key(), collection);
        confMap.put(TypesenseSinkOptions.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

        ReadonlyConfig pluginConf = ReadonlyConfig.fromMap(confMap);
        CollectionInfo collectionInfo = new CollectionInfo(collection, pluginConf);
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {primaryKey, "name"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});
        TypesenseRowSerializer typesenseRowSerializer =
                new TypesenseRowSerializer(collectionInfo, schema);
        String id = "0001";
        String name = "jack";
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(RowKind.UPDATE_AFTER);
        Assertions.assertEquals(typesenseRowSerializer.serializeRowForDelete(row), id);
        row.setRowKind(RowKind.INSERT);
        String data = "{\"name\":\"jack\",\"id\":\"0001\"}";
        Assertions.assertEquals(typesenseRowSerializer.serializeRow(row), data);
    }

    @Test
    public void testCompositeKeyEncodingPreservesLegacyIdsByDefault() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"first", "second"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});
        SeaTunnelRow left = new SeaTunnelRow(new Object[] {"1_1", "2"});
        SeaTunnelRow right = new SeaTunnelRow(new Object[] {"1", "1_2"});
        TypesenseRowSerializer legacy = serializer(schema, null);
        TypesenseRowSerializer safe = serializer(schema, "LENGTH_PREFIXED");

        Assertions.assertEquals("1_1_2", legacy.serializeRowForDelete(left));
        Assertions.assertEquals(
                legacy.serializeRowForDelete(left), legacy.serializeRowForDelete(right));
        Assertions.assertEquals("3:1_11:2", safe.serializeRowForDelete(left));
        Assertions.assertEquals("1:13:1_2", safe.serializeRowForDelete(right));
        Assertions.assertTrue(safe.serializeRow(left).contains("\"id\":\"3:1_11:2\""));
        Assertions.assertTrue(safe.serializeRow(right).contains("\"id\":\"1:13:1_2\""));
    }

    @Test
    public void testSingleKeyEncodingKeepsTheExistingId() {
        SeaTunnelRowType schema =
                new SeaTunnelRowType(
                        new String[] {"first", "second"},
                        new SeaTunnelDataType[] {STRING_TYPE, STRING_TYPE});
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(TypesenseBaseOptions.COLLECTION.key(), "id_probe");
        confMap.put(TypesenseSinkOptions.PRIMARY_KEYS.key(), Arrays.asList("first"));
        confMap.put(TypesenseSinkOptions.KEY_ENCODING.key(), "LENGTH_PREFIXED");
        TypesenseRowSerializer serializer =
                new TypesenseRowSerializer(
                        new CollectionInfo("id_probe", ReadonlyConfig.fromMap(confMap)), schema);

        Assertions.assertEquals(
                "1_1",
                serializer.serializeRowForDelete(new SeaTunnelRow(new Object[] {"1_1", "2"})));
    }

    private static TypesenseRowSerializer serializer(SeaTunnelRowType schema, String encoding) {
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(TypesenseBaseOptions.COLLECTION.key(), "id_probe");
        confMap.put(TypesenseSinkOptions.PRIMARY_KEYS.key(), Arrays.asList("first", "second"));
        if (encoding != null) {
            confMap.put(TypesenseSinkOptions.KEY_ENCODING.key(), encoding);
        }
        return new TypesenseRowSerializer(
                new CollectionInfo("id_probe", ReadonlyConfig.fromMap(confMap)), schema);
    }
}
