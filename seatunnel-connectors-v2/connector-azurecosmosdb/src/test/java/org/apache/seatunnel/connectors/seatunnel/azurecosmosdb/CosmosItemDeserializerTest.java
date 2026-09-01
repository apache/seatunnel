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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.serialize.CosmosItemDeserializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CosmosItemDeserializerTest {

    @Test
    public void testDeserialize() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "active", "score", "tags", "labels"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.BOOLEAN_TYPE,
                            BasicType.DOUBLE_TYPE,
                            ArrayType.of(BasicType.STRING_TYPE),
                            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)
                        });

        CosmosItemDeserializer deserializer = new CosmosItemDeserializer(rowType);
        Map<String, Object> labels = new HashMap<>();
        labels.put("region", "westus");
        labels.put("team", "data");

        Map<String, Object> doc = new HashMap<>();
        doc.put("id", 7);
        doc.put("name", "cosmos-user");
        doc.put("active", true);
        doc.put("score", 98.5);
        doc.put("tags", new String[] {"alpha", "beta"});
        doc.put("labels", labels);

        SeaTunnelRow row = deserializer.deserialize(doc);

        Assertions.assertEquals(7, row.getField(0));
        Assertions.assertEquals("cosmos-user", row.getField(1));
        Assertions.assertEquals(true, row.getField(2));
        Assertions.assertEquals(98.5, row.getField(3));
        Assertions.assertArrayEquals(new String[] {"alpha", "beta"}, (Object[]) row.getField(4));
        Assertions.assertEquals(labels, row.getField(5));
    }
}
