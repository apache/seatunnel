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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Mockito.when;

public class RedisSinkWriterTest {

    private RedisClient mockRedisClient;

    private RedisParameters mockRedisParameters;

    private SeaTunnelRowType rowType;
    private RedisSinkWriter redisSinkWriter;

    @BeforeEach
    void setUp() {
        rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age", "email"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE
                        });

        mockRedisParameters = Mockito.mock(RedisParameters.class);
        mockRedisClient = Mockito.mock(RedisClient.class);

        when(mockRedisParameters.buildRedisClient()).thenReturn(mockRedisClient);
        when(mockRedisParameters.getBatchSize()).thenReturn(3);
        when(mockRedisParameters.getFormat()).thenReturn(RedisBaseOptions.Format.JSON);
        when(mockRedisParameters.getFieldDelimiter()).thenReturn(",");
    }

    @Test
    void testGetCustomKey() {
        // Set custom key mode
        when(mockRedisParameters.getKeyField()).thenReturn("user:${id}:profile");
        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:1:profile", customKey);
    }

    @Test
    public void testLegacyCustomKey() {
        when(mockRedisParameters.getKeyField()).thenReturn("user:{id}:profile");

        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:1:profile", customKey);
    }
}
