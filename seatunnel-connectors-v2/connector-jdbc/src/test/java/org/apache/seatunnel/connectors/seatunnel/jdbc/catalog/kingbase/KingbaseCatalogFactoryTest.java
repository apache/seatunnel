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
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that {@link KingbaseCatalogFactory} selects the catalog implementation purely from the
 * explicit {@code compatible_mode} option.
 *
 * <p>The catalog implementations themselves are replaced by recording stubs, so this test never
 * opens a connection. That is intentional: a real {@code MySqlCatalog} resolves the server version
 * in its constructor, so an unroutable URL would still trigger a network attempt and could stall
 * the suite. Asserting on the recorded selection keeps the test deterministic. It pins which
 * catalog the factory selects from configuration; it does not by itself prove that no other code
 * path opens a connection. That boundary is covered by source inspection and by the licensed
 * Kingbase end-to-end test, not by this unit test.
 */
public class KingbaseCatalogFactoryTest {

    /** Records which catalog implementation the factory decided to build. */
    private static final class RecordingFactory extends KingbaseCatalogFactory {
        private String selected;
        private String compatibleModeSeen;

        @Override
        MySqlCatalog createMySqlCatalog(
                String catalogName,
                String username,
                String pwd,
                JdbcUrlUtil.UrlInfo urlInfo,
                String driverClass) {
            this.selected = "mysql";
            return null;
        }

        @Override
        KingbaseCatalog createKingbaseCatalog(
                String catalogName,
                String username,
                String pwd,
                JdbcUrlUtil.UrlInfo urlInfo,
                String defaultSchema,
                String driverClass) {
            this.selected = "native";
            return null;
        }

        @Override
        public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
            this.compatibleModeSeen = options.get(JdbcCommonOptions.COMPATIBLE_MODE);
            return super.createCatalog(catalogName, options);
        }
    }

    @Test
    public void testExplicitMysqlCompatibleModeSelectsMySqlCatalog() {
        RecordingFactory factory = new RecordingFactory();
        factory.createCatalog("kingbase", config("mysql"));

        Assertions.assertEquals("mysql", factory.selected);
    }

    @Test
    public void testExplicitPgCompatibleModeSelectsKingbaseCatalog() {
        RecordingFactory factory = new RecordingFactory();
        factory.createCatalog("kingbase", config("pg"));

        Assertions.assertEquals("native", factory.selected);
    }

    @Test
    public void testExplicitOracleCompatibleModeSelectsKingbaseCatalog() {
        RecordingFactory factory = new RecordingFactory();
        factory.createCatalog("kingbase", config("oracle"));

        Assertions.assertEquals("native", factory.selected);
    }

    @Test
    public void testAbsentCompatibleModeSelectsKingbaseCatalog() {
        RecordingFactory factory = new RecordingFactory();
        factory.createCatalog("kingbase", config(null));

        Assertions.assertEquals("native", factory.selected);
        Assertions.assertNull(factory.compatibleModeSeen);
    }

    @Test
    public void testCompatibleModeIsCaseInsensitiveAndTrimmed() {
        for (String mode : new String[] {"MYSQL", "MySql", "  mysql  "}) {
            RecordingFactory factory = new RecordingFactory();
            factory.createCatalog("kingbase", config(mode));

            Assertions.assertEquals("mysql", factory.selected, "mode=" + mode);
        }
    }

    @Test
    public void testSelectionHelperIsPure() {
        Assertions.assertTrue(KingbaseCatalogFactory.resolvesToMySqlCatalog("mysql"));
        Assertions.assertTrue(KingbaseCatalogFactory.resolvesToMySqlCatalog(" MySQL "));
        Assertions.assertFalse(KingbaseCatalogFactory.resolvesToMySqlCatalog("oracle"));
        Assertions.assertFalse(KingbaseCatalogFactory.resolvesToMySqlCatalog("pg"));
        Assertions.assertFalse(KingbaseCatalogFactory.resolvesToMySqlCatalog(""));
        Assertions.assertFalse(KingbaseCatalogFactory.resolvesToMySqlCatalog(null));
    }

    @Test
    public void testOptionRuleDeclaresCompatibleMode() {
        Assertions.assertTrue(
                new KingbaseCatalogFactory()
                        .optionRule()
                        .getOptionalOptions()
                        .contains(JdbcCommonOptions.COMPATIBLE_MODE));
    }

    /**
     * Deliberately uses an unroutable address: because the stubs above never construct a real
     * catalog, nothing resolves the URL, so the suite stays fast. A live probe reintroduced inside
     * {@link KingbaseCatalogFactory#resolvesToMySqlCatalog(String)} would still make this test
     * pass; that helper is covered by {@link #testSelectionHelperIsPure()}, while the absence of
     * any other connection during selection is established by source inspection and the licensed
     * end-to-end test.
     */
    private static ReadonlyConfig config(String compatibleMode) {
        Map<String, Object> options = new HashMap<>();
        options.put(JdbcCommonOptions.URL.key(), "jdbc:kingbase8://192.0.2.1:54321/test");
        options.put(JdbcCommonOptions.USERNAME.key(), "kingbase");
        options.put(JdbcCommonOptions.PASSWORD.key(), "kingbase");
        options.put(JdbcCommonOptions.DRIVER.key(), "com.kingbase8.Driver");
        if (compatibleMode != null) {
            options.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), compatibleMode);
        }
        return ReadonlyConfig.fromMap(options);
    }
}
