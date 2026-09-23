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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests the OceanBase CDC wrapper contract that differentiates it from the reused MySQL CDC
 * implementation.
 */
public class OceanBaseIncrementalSourceFactoryTest {

    /** Verify the OceanBase wrapper uses a dedicated factory identifier for plugin discovery. */
    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals(
                "OceanBase-CDC", new OceanBaseIncrementalSourceFactory().factoryIdentifier());
    }

    /** Verify the wrapper keeps the MySQL factory inheritance so restore logic stays aligned. */
    @Test
    public void testFactoryInheritance() {
        Assertions.assertEquals(
                MySqlIncrementalSourceFactory.class,
                OceanBaseIncrementalSourceFactory.class.getSuperclass());
    }

    /** Verify the factory returns the OceanBase wrapper source instead of the raw MySQL source. */
    @Test
    public void testSourceClass() {
        Assertions.assertEquals(
                OceanBaseIncrementalSource.class,
                new OceanBaseIncrementalSourceFactory().getSourceClass());
    }

    /** Verify restore configuration defaults the OceanBase catalog selector to MySQL mode. */
    @Test
    public void testRestoreConfigurationDefaultsCatalogCompatibleModeToMysql() {
        OceanBaseIncrementalSourceFactory factory = new OceanBaseIncrementalSourceFactory();
        ReadonlyConfig config = ReadonlyConfig.fromMap(requiredOptions());

        ConfigValidator.of(config).validate(factory.optionRule());
        ConfigValidator.validateUnknownKeys(config, factory.optionRule(), "OceanBase-CDC");

        Assertions.assertEquals(
                "mysql",
                factory.mysqlCompatibleConfig(config).get(JdbcCommonOptions.COMPATIBLE_MODE));
    }

    /** Verify static validation accepts the explicit MySQL selector used by E2E configurations. */
    @Test
    public void testOptionRuleAcceptsMysqlCompatibleMode() {
        OceanBaseIncrementalSourceFactory factory = new OceanBaseIncrementalSourceFactory();
        Map<String, Object> options = requiredOptions();
        options.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), "mysql");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        ConfigValidator.of(config).validate(factory.optionRule());
        ConfigValidator.validateUnknownKeys(config, factory.optionRule(), "OceanBase-CDC");
    }

    /** Verify this MySQL-binlog connector rejects the unsupported OceanBase Oracle mode. */
    @Test
    public void testOptionRuleRejectsOracleCompatibleMode() {
        Map<String, Object> options = requiredOptions();
        options.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), "oracle");

        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                                .validate(new OceanBaseIncrementalSourceFactory().optionRule()));
    }

    private static Map<String, Object> requiredOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("username", "root");
        options.put("password", "password");
        options.put("url", "jdbc:mysql://localhost:2881/inventory");
        options.put("table-names", Collections.singletonList("inventory.orders"));
        return options;
    }
}
