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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IcebergSinkConfigTest {

    @Test
    public void testPartitionKeysParsingWithTransformArgs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "tbl");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), new HashMap<String, String>());
        configs.put(
                IcebergSinkOptions.TABLE_DEFAULT_PARTITION_KEYS.key(),
                "bucket(id, 16),truncate(col, 8),dt");

        IcebergSinkConfig config = new IcebergSinkConfig(ReadonlyConfig.fromMap(configs));
        Assertions.assertEquals(
                Arrays.asList("bucket(id, 16)", "truncate(col, 8)", "dt"),
                config.getPartitionKeys());
    }

    @Test
    public void testFlushRowSizeIsUnsetByDefault() {
        Assertions.assertNull(new IcebergSinkConfig(flushRowSizeConfig(null)).getFlushRowSize());
        Assertions.assertEquals(
                1000L,
                new IcebergSinkConfig(flushRowSizeConfig(1000L)).getFlushRowSize().longValue());
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L})
    public void testFlushRowSizeMustBePositive(long flushRowSize) {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> new IcebergSinkConfig(flushRowSizeConfig(flushRowSize)));
        Assertions.assertTrue(
                exception.getMessage().contains("'flush_row_size' must be greater than 0"),
                exception.getMessage());
    }

    private static ReadonlyConfig flushRowSizeConfig(Long flushRowSize) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "tbl");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), new HashMap<String, String>());
        if (flushRowSize != null) {
            configs.put(IcebergSinkOptions.FLUSH_ROW_SIZE.key(), flushRowSize);
        }
        return ReadonlyConfig.fromMap(configs);
    }
}
