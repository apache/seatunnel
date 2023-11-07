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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class SeaTunnelSinkWithBufferWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final List<Object[]> valueBuffer;

    public SeaTunnelSinkWithBufferWriter() {
        this.valueBuffer = new ArrayList<>();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        valueBuffer.add(element.getFields());
        if (valueBuffer.size() == 3) {
            List<Object[]> expected =
                    Arrays.asList(
                            new Object[] {
                                42,
                                "string1",
                                true,
                                1.1f,
                                33.33,
                                (byte) 1,
                                (short) 2,
                                Long.MAX_VALUE,
                                new BigDecimal("55.55"),
                                LocalDate.parse("2021-01-01"),
                                LocalDateTime.parse("2021-01-01T00:00:00"),
                                null,
                                new Object[] {"string1", "string2", "string3"},
                                new Object[] {true, false, true},
                                new Object[] {(byte) 1, (byte) 2, (byte) 3},
                                new Object[] {(short) 1, (short) 2, (short) 3},
                                new Object[] {1, 2, 3},
                                new Object[] {1L, 2L, 3L},
                                new Object[] {1.1f, 2.2f, 3.3f},
                                new Object[] {11.11, 22.22, 33.33},
                                new HashMap<String, String>() {
                                    {
                                        put("key1", "value1");
                                        put("key2", "value2");
                                        put("key3", "value3");
                                    }
                                },
                                new SeaTunnelRow(
                                        new Object[] {
                                            42,
                                            "string1",
                                            true,
                                            1.1f,
                                            33.33,
                                            (byte) 1,
                                            (short) 2,
                                            Long.MAX_VALUE,
                                            new BigDecimal("55.55"),
                                            LocalDate.parse("2021-01-01"),
                                            LocalDateTime.parse("2021-01-01T00:00:00"),
                                            null,
                                            new Object[] {"string1", "string2", "string3"},
                                            new Object[] {true, false, true},
                                            new Object[] {(byte) 1, (byte) 2, (byte) 3},
                                            new Object[] {(short) 1, (short) 2, (short) 3},
                                            new Object[] {1, 2, 3},
                                            new Object[] {1L, 2L, 3L},
                                            new Object[] {1.1f, 2.2f, 3.3f},
                                            new Object[] {11.11, 22.22, 33.33},
                                            new HashMap<String, String>() {
                                                {
                                                    put("key1", "value1");
                                                    put("key2", "value2");
                                                    put("key3", "value3");
                                                }
                                            }
                                        })
                            },
                            new Object[] {
                                12,
                                "string2",
                                false,
                                2.2f,
                                43.33,
                                (byte) 5,
                                (short) 42,
                                Long.MAX_VALUE - 1,
                                new BigDecimal("25.55"),
                                LocalDate.parse("2011-01-01"),
                                LocalDateTime.parse("2020-01-01T00:00:00"),
                                null,
                                new Object[] {"string3", "string2", "string1"},
                                new Object[] {true, false, false},
                                new Object[] {(byte) 3, (byte) 4, (byte) 5},
                                new Object[] {(short) 2, (short) 6, (short) 8},
                                new Object[] {2, 4, 6},
                                new Object[] {643634L, 421412L, 543543L},
                                new Object[] {1.24f, 21.2f, 32.3f},
                                new Object[] {421.11, 5322.22, 323.33},
                                new HashMap<String, String>() {
                                    {
                                        put("key2", "value534");
                                        put("key3", "value3");
                                        put("key4", "value43");
                                    }
                                },
                                new SeaTunnelRow(
                                        new Object[] {
                                            12,
                                            "string2",
                                            false,
                                            2.2f,
                                            43.33,
                                            (byte) 5,
                                            (short) 42,
                                            Long.MAX_VALUE - 1,
                                            new BigDecimal("25.55"),
                                            LocalDate.parse("2011-01-01"),
                                            LocalDateTime.parse("2020-01-01T00:00:00"),
                                            null,
                                            new Object[] {"string3", "string2", "string1"},
                                            new Object[] {true, false, false},
                                            new Object[] {(byte) 3, (byte) 4, (byte) 5},
                                            new Object[] {(short) 2, (short) 6, (short) 8},
                                            new Object[] {2, 4, 6},
                                            new Object[] {643634L, 421412L, 543543L},
                                            new Object[] {1.24f, 21.2f, 32.3f},
                                            new Object[] {421.11, 5322.22, 323.33},
                                            new HashMap<String, String>() {
                                                {
                                                    put("key2", "value534");
                                                    put("key3", "value3");
                                                    put("key4", "value43");
                                                }
                                            }
                                        })
                            },
                            new Object[] {
                                233,
                                "string3",
                                true,
                                231.1f,
                                3533.33,
                                (byte) 7,
                                (short) 2,
                                Long.MAX_VALUE - 2,
                                new BigDecimal("65.55"),
                                LocalDate.parse("2001-01-01"),
                                LocalDateTime.parse("2031-01-01T00:00:00"),
                                null,
                                new Object[] {"string1fsa", "stringdsa2", "strfdsaing3"},
                                new Object[] {false, true, true},
                                new Object[] {(byte) 6, (byte) 2, (byte) 1},
                                new Object[] {(short) 7, (short) 8, (short) 9},
                                new Object[] {3, 77, 22},
                                new Object[] {143L, 642L, 533L},
                                new Object[] {24.1f, 54.2f, 1.3f},
                                new Object[] {431.11, 2422.22, 3243.33},
                                new HashMap<String, String>() {
                                    {
                                        put("keyfs1", "valfdsue1");
                                        put("kedfasy2", "vafdslue2");
                                        put("kefdsay3", "vfdasalue3");
                                    }
                                },
                                new SeaTunnelRow(
                                        new Object[] {
                                            233,
                                            "string3",
                                            true,
                                            231.1f,
                                            3533.33,
                                            (byte) 7,
                                            (short) 2,
                                            Long.MAX_VALUE - 2,
                                            new BigDecimal("65.55"),
                                            LocalDate.parse("2001-01-01"),
                                            LocalDateTime.parse("2031-01-01T00:00:00"),
                                            null,
                                            new Object[] {
                                                "string1fsa", "stringdsa2", "strfdsaing3"
                                            },
                                            new Object[] {false, true, true},
                                            new Object[] {(byte) 6, (byte) 2, (byte) 1},
                                            new Object[] {(short) 7, (short) 8, (short) 9},
                                            new Object[] {3, 77, 22},
                                            new Object[] {143L, 642L, 533L},
                                            new Object[] {24.1f, 54.2f, 1.3f},
                                            new Object[] {431.11, 2422.22, 3243.33},
                                            new HashMap<String, String>() {
                                                {
                                                    put("keyfs1", "valfdsue1");
                                                    put("kedfasy2", "vafdslue2");
                                                    put("kefdsay3", "vfdasalue3");
                                                }
                                            }
                                        })
                            });
            for (int i = 0; i < expected.size(); i++) {
                Object[] values = expected.get(i);
                Object[] actual = valueBuffer.get(i);
                for (int v = 0; v < values.length; v++) {
                    if (values[v] instanceof Object[]) {
                        Assertions.assertArrayEquals((Object[]) values[v], (Object[]) actual[v]);
                    } else {
                        Assertions.assertEquals(values[v], actual[v]);
                    }
                }
            }
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {}
}
