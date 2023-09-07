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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class StarRocksJsonSerializerTest {

    @Test
    public void serialize() {
        String[] filedNames = {"id", "name", "array", "map"};
        SeaTunnelDataType<?>[] filedTypes = {
            BasicType.LONG_TYPE,
            BasicType.STRING_TYPE,
            ArrayType.STRING_ARRAY_TYPE,
            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)
        };

        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(filedNames, filedTypes);
        StarRocksJsonSerializer starRocksJsonSerializer =
                new StarRocksJsonSerializer(seaTunnelRowType, false);
        Object[] fields = {
            1, "Tom", new String[] {"tag1", "tag2"}, Collections.singletonMap("key1", "value1")
        };
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        String jsonString = starRocksJsonSerializer.serialize(seaTunnelRow);
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Tom\",\"array\":[\"tag1\",\"tag2\"],\"map\":{\"key1\":\"value1\"}}",
                jsonString);
    }
}
