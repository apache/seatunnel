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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HbaseParametersTest {

    @Test
    void testBuildWithSourceConfigWithoutNamespace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "tbl");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        HbaseParameters parameters = HbaseParameters.buildWithSourceConfig(readonlyConfig);
        assertEquals(HbaseParameters.DEFAULT_NAMESPACE, parameters.getNamespace());
        assertEquals("tbl", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigWithNamespace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "test:tbl");
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);

        HbaseParameters parameters = HbaseParameters.buildWithSourceConfig(readonlyConfig);
        assertEquals("test", parameters.getNamespace());
        assertEquals("tbl", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigReadsTimeRange() {
        Map<String, Object> config = new HashMap<>();
        config.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        config.put(HbaseBaseOptions.TABLE.key(), "test_table");
        config.put(HbaseSourceOptions.START_TIMESTAMP.key(), 1000L);
        config.put(HbaseSourceOptions.END_TIMESTAMP.key(), 2000L);

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(config));

        assertEquals(1000L, parameters.getStartTimestamp());
        assertEquals(2000L, parameters.getEndTimestamp());
    }

    @Test
    void testGetNamespaceReturnsDefaultWhenNull() {
        HbaseParameters parameters =
                HbaseParameters.builder()
                        .namespace(null)
                        .table("tbl")
                        .zookeeperQuorum("127.0.0.1:2181")
                        .build();
        assertEquals(HbaseParameters.DEFAULT_NAMESPACE, parameters.getNamespace());
    }

    @Test
    void testBuildWithSourceConfigWithLeadingColonUsesDefaultNamespace() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), ":tbl");

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(configMap));
        assertEquals(HbaseParameters.DEFAULT_NAMESPACE, parameters.getNamespace());
        assertEquals("tbl", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigWithMultipleColons() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "ns:tbl:extra");

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(configMap));
        assertEquals("ns", parameters.getNamespace());
        assertEquals("tbl:extra", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigWithSpaces() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), " ns : tbl ");

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(configMap));
        assertEquals(" ns ", parameters.getNamespace());
        assertEquals(" tbl ", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigWithEmptyTableName() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), "test:");

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(configMap));
        assertEquals("test", parameters.getNamespace());
        assertEquals("", parameters.getTable());
    }

    @Test
    void testBuildWithSourceConfigWithoutNamespaceKeepsSpacesInTableName() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HbaseBaseOptions.ZOOKEEPER_QUORUM.key(), "127.0.0.1:2181");
        configMap.put(HbaseBaseOptions.TABLE.key(), " tbl ");

        HbaseParameters parameters =
                HbaseParameters.buildWithSourceConfig(ReadonlyConfig.fromMap(configMap));
        assertEquals(HbaseParameters.DEFAULT_NAMESPACE, parameters.getNamespace());
        assertEquals(" tbl ", parameters.getTable());
    }
}
