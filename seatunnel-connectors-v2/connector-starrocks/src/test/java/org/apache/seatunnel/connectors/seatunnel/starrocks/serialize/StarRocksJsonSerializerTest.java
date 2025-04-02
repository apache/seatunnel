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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;

public class StarRocksJsonSerializerTest {

    private DateTimeFormatter dateTimeFormatter =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    @Test
    public void serialize() {
        String[] fieldNames = {"id", "name", "array", "map", "timestamp"};
        SeaTunnelDataType<?>[] fieldTypes = {
            BasicType.LONG_TYPE,
            BasicType.STRING_TYPE,
            ArrayType.STRING_ARRAY_TYPE,
            new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE),
            LocalTimeType.LOCAL_DATE_TIME_TYPE
        };

        SeaTunnelRowType seaTunnelRowType = new SeaTunnelRowType(fieldNames, fieldTypes);
        StarRocksJsonSerializer starRocksJsonSerializer =
                new StarRocksJsonSerializer(seaTunnelRowType, false);
        Object[] fields = {
            1,
            "Tom",
            new String[] {"tag1", "tag2"},
            Collections.singletonMap("key1", "value1"),
            LocalDateTime.parse("2024-01-25 07:55:45.123", dateTimeFormatter)
        };
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        String jsonString = starRocksJsonSerializer.serialize(seaTunnelRow);
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Tom\",\"array\":[\"tag1\",\"tag2\"],\"map\":{\"key1\":\"value1\"},\"timestamp\":\"2024-01-25 07:55:45.123\"}",
                jsonString);
    }
}
