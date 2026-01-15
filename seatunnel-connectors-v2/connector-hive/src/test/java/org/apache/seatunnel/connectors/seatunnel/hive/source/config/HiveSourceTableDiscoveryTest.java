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

package org.apache.seatunnel.connectors.seatunnel.hive.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HiveSourceTableDiscoveryTest {

    @Test
    void testDiscoverByUseRegexWithTableName() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "tmp_1");
        catalog.addTable("ods", "tmp_2");
        catalog.addTable("ods", "t1");
        catalog.addTable("dw", "tmp_1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "ods.tmp_\\d+");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<TablePath> result = HiveSourceTableDiscovery.discoverTablePaths(config, catalog);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(TablePath.of("ods.tmp_1")));
        Assertions.assertTrue(result.contains(TablePath.of("ods.tmp_2")));
    }

    @Test
    void testDiscoverWholeDatabaseByDatabasePattern() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "t1");
        catalog.addTable("ods", "t2");
        catalog.addTable("dw", "t1");
        catalog.addTable("ods_backup", "t3");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "ods.\\.*");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<TablePath> result = HiveSourceTableDiscovery.discoverTablePaths(config, catalog);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(TablePath.of("ods.t1")));
        Assertions.assertTrue(result.contains(TablePath.of("ods.t2")));
    }

    @Test
    void testDiscoverWholeDatabaseByExactDatabaseNameDoesNotMatchPrefixDatabases() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("a", "t1");
        catalog.addTable("a", "t2");
        catalog.addTable("abc", "t3");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "a.\\.*");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<TablePath> result = HiveSourceTableDiscovery.discoverTablePaths(config, catalog);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(TablePath.of("a.t1")));
        Assertions.assertTrue(result.contains(TablePath.of("a.t2")));
    }

    @Test
    void testDiscoverAllDatabasesAllTables() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("a", "t1");
        catalog.addTable("a", "t2");
        catalog.addTable("b", "t3");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "\\.*.\\.*");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<TablePath> result = HiveSourceTableDiscovery.discoverTablePaths(config, catalog);
        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.contains(TablePath.of("a.t1")));
        Assertions.assertTrue(result.contains(TablePath.of("a.t2")));
        Assertions.assertTrue(result.contains(TablePath.of("b.t3")));
    }

    @Test
    void testUseRegexRequiresEscapingDotsInsideTablePattern() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "tmp_1");
        catalog.addTable("ods", "tmp_2");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "ods.tmp_.*");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> HiveSourceTableDiscovery.discoverTablePaths(config, catalog));
    }

    @Test
    void testUseRegexAllowsEscapedDotsInsideTablePattern() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "tmp_1");
        catalog.addTable("ods", "tmp_2");
        catalog.addTable("ods", "t1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "ods.tmp_\\.*");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<TablePath> result = HiveSourceTableDiscovery.discoverTablePaths(config, catalog);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(TablePath.of("ods.tmp_1")));
        Assertions.assertTrue(result.contains(TablePath.of("ods.tmp_2")));
    }

    @Test
    void testUseRegexRequiresTableName() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "t1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> HiveSourceTableDiscovery.discoverTablePaths(config, catalog));
    }

    @Test
    void testUseRegexRequiresDatabaseAndTableSeparator() {
        FakeCatalog catalog = new FakeCatalog();
        catalog.addTable("ods", "tmp_1");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.USE_REGEX.key(), true);
        configMap.put(HiveOptions.TABLE_NAME.key(), "tmp_\\d+");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> HiveSourceTableDiscovery.discoverTablePaths(config, catalog));
    }

    private static class FakeCatalog implements Catalog {

        private final Map<String, List<String>> databaseTables = new HashMap<>();

        void addTable(String database, String table) {
            databaseTables.computeIfAbsent(database, ignored -> new ArrayList<>()).add(table);
        }

        @Override
        public void open() throws CatalogException {}

        @Override
        public void close() throws CatalogException {}

        @Override
        public String name() {
            return "fake_hive_catalog";
        }

        @Override
        public String getDefaultDatabase() throws CatalogException {
            return "default";
        }

        @Override
        public boolean databaseExists(String databaseName) throws CatalogException {
            return databaseTables.containsKey(databaseName);
        }

        @Override
        public List<String> listDatabases() throws CatalogException {
            return new ArrayList<>(databaseTables.keySet());
        }

        @Override
        public List<String> listTables(String databaseName)
                throws CatalogException, DatabaseNotExistException {
            return databaseTables.getOrDefault(databaseName, Collections.emptyList());
        }

        @Override
        public boolean tableExists(TablePath tablePath) throws CatalogException {
            if (tablePath == null || tablePath.getDatabaseName() == null) {
                return false;
            }
            return databaseTables
                    .getOrDefault(tablePath.getDatabaseName(), Collections.emptyList())
                    .contains(tablePath.getTableName());
        }

        @Override
        public CatalogTable getTable(TablePath tablePath)
                throws CatalogException, TableNotExistException {
            throw new UnsupportedOperationException("not needed for discovery test");
        }

        @Override
        public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
                throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException("not needed for discovery test");
        }

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {
            throw new UnsupportedOperationException("not needed for discovery test");
        }

        @Override
        public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
                throws DatabaseAlreadyExistException, CatalogException {
            throw new UnsupportedOperationException("not needed for discovery test");
        }

        @Override
        public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
                throws DatabaseNotExistException, CatalogException {
            throw new UnsupportedOperationException("not needed for discovery test");
        }
    }
}
