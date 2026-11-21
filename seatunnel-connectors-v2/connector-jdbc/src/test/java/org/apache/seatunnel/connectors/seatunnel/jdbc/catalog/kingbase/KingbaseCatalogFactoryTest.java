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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that {@link KingbaseCatalogFactory} honors an explicitly configured {@code
 * compatible_mode} without probing the database. The URL below points at an unroutable address, so
 * a live probe would fail (and, before the fix, silently fall back to the native catalog).
 */
public class KingbaseCatalogFactoryTest {

    private static final KingbaseCatalogFactory FACTORY = new KingbaseCatalogFactory();

    /** compatible_mode = mysql selects the MySQL catalog without opening a connection. */
    @Test
    public void testExplicitMysqlCompatibleModeSelectsMySqlCatalog() {
        Catalog catalog = FACTORY.createCatalog("kingbase", config("mysql"));

        Assertions.assertTrue(catalog instanceof MySqlCatalog, catalog.getClass().getName());
    }

    /** Any other explicit mode keeps the native Kingbase catalog, also without probing. */
    @Test
    public void testExplicitPgCompatibleModeSelectsKingbaseCatalog() {
        Catalog catalog = FACTORY.createCatalog("kingbase", config("pg"));

        Assertions.assertTrue(catalog instanceof KingbaseCatalog, catalog.getClass().getName());
    }

    /** The option is declared on the factory rule so validation does not reject it. */
    @Test
    public void testOptionRuleDeclaresCompatibleMode() {
        Assertions.assertTrue(
                FACTORY.optionRule()
                        .getOptionalOptions()
                        .contains(JdbcCommonOptions.COMPATIBLE_MODE));
    }

    private static ReadonlyConfig config(String compatibleMode) {
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcCommonOptions.URL.key(), "jdbc:kingbase8://192.0.2.1:54321/test");
        options.put(JdbcCommonOptions.USERNAME.key(), "kingbase");
        options.put(JdbcCommonOptions.PASSWORD.key(), "kingbase");
        options.put(JdbcCommonOptions.DRIVER.key(), "com.kingbase8.Driver");
        options.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), compatibleMode);
        return ReadonlyConfig.fromMap(options);
    }
}
