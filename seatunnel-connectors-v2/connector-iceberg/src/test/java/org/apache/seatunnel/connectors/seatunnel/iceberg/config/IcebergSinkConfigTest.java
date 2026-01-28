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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
