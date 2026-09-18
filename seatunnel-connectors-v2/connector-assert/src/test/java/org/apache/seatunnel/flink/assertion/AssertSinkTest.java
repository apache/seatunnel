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
package org.apache.seatunnel.flink.assertion;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSink;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AssertSinkTest {

    @Test
    public void testEmptyRulesFailsAtRuntime() {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AssertSinkOptions.RULES.key(), Collections.emptyMap());

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
        CatalogTable catalogTable = createCatalogTable();

        ConfigException.BadValue badValue =
                Assertions.assertThrows(
                        ConfigException.BadValue.class,
                        () -> new AssertSink(readonlyConfig, catalogTable));

        Assertions.assertTrue(badValue.getMessage().contains("Assert rule config is empty"));
    }

    @Test
    public void testEffectivelyEmptyRulesFailsAtRuntime() {
        Map<String, Object> rules = new HashMap<>();
        rules.put(ConnectorCommonOptions.TABLE_NAMES.key(), Collections.emptyList());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AssertSinkOptions.RULES.key(), rules);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogTable catalogTable = createCatalogTable();

        ConfigException.BadValue exception =
                Assertions.assertThrows(
                        ConfigException.BadValue.class, () -> new AssertSink(config, catalogTable));

        Assertions.assertTrue(exception.getMessage().contains("Assert rule config is empty"));
    }

    private CatalogTable createCatalogTable() {
        SeaTunnelRowType seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return CatalogTableUtil.getCatalogTable("test", seaTunnelRowType);
    }
}
