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

package org.apache.seatunnel.connectors.seatunnel.hbase.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class HbaseParametersTest {

    @Test
    public void testBuildWithSourceConfigWithoutNamespace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "tbl");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        HbaseParameters parameters = HbaseParameters.buildWithSourceConfig(readonlyConfig);
        Assertions.assertEquals(HbaseParameters.DEFAULT_NAMESPACE, parameters.getNamespace());
        Assertions.assertEquals("tbl", parameters.getTable());
    }

    @Test
    public void testBuildWithSourceConfigWithNamespace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "test:tbl");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        HbaseParameters parameters = HbaseParameters.buildWithSourceConfig(readonlyConfig);
        Assertions.assertEquals("test", parameters.getNamespace());
        Assertions.assertEquals("tbl", parameters.getTable());
    }
}
