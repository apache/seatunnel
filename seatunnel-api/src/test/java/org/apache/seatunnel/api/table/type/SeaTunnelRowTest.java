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

package org.apache.seatunnel.api.table.type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SeaTunnelRowTest {

    @Test
    void testForRowSize() {
        Map<String, Object> map = new HashMap<>();
        map.put(
                "key1",
                new SeaTunnelRow(
                        new Object[] {
                            1, "test", 1L, new BigDecimal("3333.333"),
                        }));
        map.put(
                "key2",
                new SeaTunnelRow(
                        new Object[] {
                            1, "test", 1L, new BigDecimal("3333.333"),
                        }));
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "test",
                            1L,
                            map,
                            new BigDecimal("3333.333"),
                            new String[] {"test2", "test", "3333.333"}
                        });

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"f0", "f1", "f2", "f3", "f4", "f5"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.LONG_TYPE,
                            new MapType<>(
                                    BasicType.STRING_TYPE,
                                    new SeaTunnelRowType(
                                            new String[] {"f0", "f1", "f2", "f3"},
                                            new SeaTunnelDataType<?>[] {
                                                BasicType.INT_TYPE,
                                                BasicType.STRING_TYPE,
                                                BasicType.LONG_TYPE,
                                                new DecimalType(10, 3)
                                            })),
                            new DecimalType(10, 3),
                            ArrayType.STRING_ARRAY_TYPE
                        });

        Assertions.assertEquals(181, row.getBytesSize(rowType));

        SeaTunnelRow row2 =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "test",
                            1L,
                            map,
                            new BigDecimal("3333.333"),
                            new String[] {"test2", "test", "3333.333"}
                        });
        Assertions.assertEquals(181, row2.getBytesSize());
    }

    @Test
    void testWithLinkHashMap() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("key", "value");
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {map});
        Assertions.assertEquals(8, row.getBytesSize());
    }

    @Test
    void testWithMapInterface() {
        Map<String, String> map = Collections.singletonMap("key", "value");
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {map});
        Assertions.assertEquals(8, row.getBytesSize());
    }

    @Test
    void testArrayEmptyElement() {
        String[] stringElements = {"key", null, "value"};
        SeaTunnelRow stringRow = new SeaTunnelRow(new Object[] {stringElements});
        Assertions.assertEquals(8, stringRow.getBytesSize());

        Double[] doubleElements = {null, 10.0, 12.0};
        SeaTunnelRow doubleRow = new SeaTunnelRow(new Object[] {doubleElements});
        Assertions.assertEquals(16, doubleRow.getBytesSize());

        String str = "seatunnel";
        SeaTunnelRow strRow = new SeaTunnelRow(new Object[] {str});
        Assertions.assertEquals(9, strRow.getBytesSize());

        String nonStr = null;
        SeaTunnelRow nonStrRow = new SeaTunnelRow(new Object[] {nonStr});
        Assertions.assertEquals(0, nonStrRow.getBytesSize());
    }
}
