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

package amazonsqs;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.serialize.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.serialize.DefaultSeaTunnelRowSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class AmazonSqsSerDeTest {

    @Test
    void deserializeMessageBody() {

        Map<String, Object> configMap = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();
        Map<String, String> fieldsMap = new HashMap<>();
        fieldsMap.put("foo", "String");
        fieldsMap.put("id", "Int");
        schemaMap.put("fields", fieldsMap);
        configMap.put("schema", schemaMap);
        Config config = ConfigFactory.parseMap(configMap);
        SeaTunnelRowType typeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();

        DefaultSeaTunnelRowDeserializer defaultSeaTunnelRowDeserializer =
                new DefaultSeaTunnelRowDeserializer(typeInfo);

        String messageBody = "foo: bar\n" + "id: 1\n";
        SeaTunnelRow deserializedObject = defaultSeaTunnelRowDeserializer.deserialize(messageBody);
        Assertions.assertEquals("bar", deserializedObject.getField(0));
        Assertions.assertEquals(
                "String", deserializedObject.getField(0).getClass().getSimpleName());
        Assertions.assertEquals(1, deserializedObject.getField(1));
        Assertions.assertEquals(
                "Integer", deserializedObject.getField(1).getClass().getSimpleName());
    }

    @Test
    void serializeSeaTunnelRow() {
        Map<String, Object> configMap = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();
        Map<String, String> fieldsMap = new HashMap<>();
        fieldsMap.put("foo", "String");
        fieldsMap.put("id", "Int");
        schemaMap.put("fields", fieldsMap);
        configMap.put("schema", schemaMap);
        Config config = ConfigFactory.parseMap(configMap);
        SeaTunnelRowType typeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(2);
        seaTunnelRow.setField(0, "bar");
        seaTunnelRow.setField(1, 1);

        DefaultSeaTunnelRowSerializer defaultSeaTunnelRowSerializer =
                new DefaultSeaTunnelRowSerializer(typeInfo, null);
        String messageBody = defaultSeaTunnelRowSerializer.serializeRowToString(seaTunnelRow);
        Assertions.assertEquals("foo: bar\n" + "id: 1\n", messageBody);
    }
}
