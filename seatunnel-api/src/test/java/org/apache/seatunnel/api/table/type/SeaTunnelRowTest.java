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

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

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

        Map<String, Object> objectMap = Maps.newHashMap();
        objectMap.put("name", "cosmos");
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "test",
                            1L,
                            map,
                            new BigDecimal("3333.333"),
                            new String[] {"test2", "test", "3333.333"},
                            new Integer[] {1, 2, 3},
                            new Long[] {1L, 2L, 3L},
                            new Double[] {1D, 2D},
                            new Float[] {1F, 2F},
                            new Boolean[] {Boolean.TRUE, Boolean.FALSE},
                            new Byte[] {1, 2, 3, 4},
                            new Short[] {Short.parseShort("1")},
                            new Map[] {objectMap}
                        });

        SeaTunnelRow row2 =
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "test",
                            1L,
                            map,
                            new BigDecimal("3333.333"),
                            new String[] {"test2", "test", "3333.333", null},
                            new Integer[] {1, 2, 3, null},
                            new Long[] {1L, 2L, 3L, null},
                            new Double[] {1D, 2D, null},
                            new Float[] {1F, 2F, null},
                            new Boolean[] {Boolean.TRUE, Boolean.FALSE, null},
                            new Byte[] {1, 2, 3, 4, null},
                            new Short[] {Short.parseShort("1"), null},
                            new Map[] {objectMap}
                        });

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
                            "f11", "f12", "f13"
                        },
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
                            ArrayType.STRING_ARRAY_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            ArrayType.LONG_ARRAY_TYPE,
                            ArrayType.DOUBLE_ARRAY_TYPE,
                            ArrayType.FLOAT_ARRAY_TYPE,
                            ArrayType.BOOLEAN_ARRAY_TYPE,
                            ArrayType.BYTE_ARRAY_TYPE,
                            ArrayType.SHORT_ARRAY_TYPE,
                            new ArrayType<>(
                                    Map[].class,
                                    new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE))
                        });

        Assertions.assertEquals(259, row.getBytesSize(rowType));
        Assertions.assertEquals(259, row.getBytesSize());

        Assertions.assertEquals(259, row2.getBytesSize(rowType));
        Assertions.assertEquals(259, row2.getBytesSize());
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
}
