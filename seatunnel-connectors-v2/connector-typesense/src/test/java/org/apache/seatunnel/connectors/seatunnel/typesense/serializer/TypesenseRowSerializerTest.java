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
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig;
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
        confMap.put(SinkConfig.COLLECTION.key(), collection);
        confMap.put(SinkConfig.PRIMARY_KEYS.key(), Arrays.asList(primaryKey));

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
}
