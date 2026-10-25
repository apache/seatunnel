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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Covers DB2 factory contracts and consistent unquoted table names without losing split overrides.
 */
class Db2IncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new Db2IncrementalSourceFactory()).optionRule());
    }

    /** Verifies Db2 checkpoint tables use the empty-catalog identifier returned by discovery. */
    @Test
    public void testToTableIdDropsCatalog() {
        Db2SourceConfigFactory configFactory = Mockito.mock(Db2SourceConfigFactory.class);
        Mockito.when(configFactory.create(0)).thenReturn(Mockito.mock(Db2SourceConfig.class));
        Db2Dialect dialect = new Db2Dialect(configFactory, Collections.emptyList());

        Assertions.assertEquals(
                new TableId("", "DB2INST1", "CUSTOMERS"),
                dialect.toTableId(TablePath.of("SAMPLE", "DB2INST1", "CUSTOMERS")));
    }

    @Test
    void shouldNormalizeSchemaAndTableWithoutChangingDatabaseOrSplitOverrides() {
        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("table", "e2e.db2inst1.customers");
        tableConfig.put("primaryKeys", Collections.singletonList("ID"));
        tableConfig.put("snapshotSplitColumn", "ID");
        Map<String, Object> raw = new HashMap<>();
        raw.put("table-names", Collections.singletonList("e2e.db2inst1.customers"));
        raw.put("table-names-config", Collections.singletonList(tableConfig));
        raw.put("database-names", Collections.singletonList("e2e"));
        raw.put("base-url", "jdbc:db2://localhost:50000/e2e");
        raw.put("exactly_once", true);
        raw.put("enable_concurrent_read", false);
        ReadonlyConfig original = ReadonlyConfig.fromMap(raw);

        ReadonlyConfig normalized = Db2IncrementalSourceFactory.normalizeDb2Options(original);
        JdbcSourceTableConfig table =
                normalized.get(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG).get(0);

        Assertions.assertEquals(
                Collections.singletonList("e2e.DB2INST1.CUSTOMERS"),
                normalized.get(ConnectorCommonOptions.TABLE_NAMES));
        Assertions.assertEquals("e2e.DB2INST1.CUSTOMERS", table.getTable());
        TablePath tablePath = TablePath.of(table.getTable(), true);
        Assertions.assertEquals("e2e", tablePath.getDatabaseName());
        Assertions.assertEquals("DB2INST1", tablePath.getSchemaName());
        Assertions.assertEquals("CUSTOMERS", tablePath.getTableName());
        Assertions.assertEquals(
                Collections.singletonList("e2e"),
                normalized.get(Db2IncrementalSourceOptions.DATABASE_NAMES));
        Assertions.assertEquals(Collections.singletonList("ID"), table.getPrimaryKeys());
        Assertions.assertEquals("ID", table.getSnapshotSplitColumn());
        Assertions.assertEquals(
                "jdbc:db2://localhost:50000/e2e", normalized.get(Db2IncrementalSourceOptions.URL));
        Assertions.assertTrue(normalized.get(Db2IncrementalSourceOptions.EXACTLY_ONCE));
        Assertions.assertEquals(
                Collections.singletonList("e2e.db2inst1.customers"),
                original.get(ConnectorCommonOptions.TABLE_NAMES));
        Assertions.assertEquals("e2e.db2inst1.customers", tableConfig.get("table"));
    }

    @Test
    void shouldKeepTablePatternAndQuotedSpellingUnchanged() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("table-pattern", "e2e.DB2INST1.Customer.*");
        ReadonlyConfig normalized =
                Db2IncrementalSourceFactory.normalizeDb2Options(ReadonlyConfig.fromMap(raw));
        Assertions.assertEquals(
                "e2e.DB2INST1.Customer.*", normalized.get(ConnectorCommonOptions.TABLE_PATTERN));
        Assertions.assertFalse(
                normalized.getOptional(ConnectorCommonOptions.TABLE_NAMES).isPresent());

        raw.remove("table-pattern");
        raw.put("table-names", Collections.singletonList("E2E.DB2INST1.\"Customers\""));
        normalized = Db2IncrementalSourceFactory.normalizeDb2Options(ReadonlyConfig.fromMap(raw));
        Assertions.assertEquals(
                Collections.singletonList("E2E.DB2INST1.\"Customers\""),
                normalized.get(ConnectorCommonOptions.TABLE_NAMES));
    }

    @Test
    void shouldUseNormalizedNamesForDebeziumCaptureFilter() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("table-names", Collections.singletonList("e2e.db2inst1.customers"));
        raw.put("username", "user");
        raw.put("password", "password");
        raw.put("exactly_once", true);
        raw.put("enable_concurrent_read", false);
        Db2SourceConfigFactory factory = new Db2SourceConfigFactory();
        factory.fromReadonlyConfig(
                Db2IncrementalSourceFactory.normalizeDb2Options(ReadonlyConfig.fromMap(raw)));
        factory.hostname("localhost").port(50000).databaseList("e2e");
        Db2SourceConfig config = factory.create(0);

        Assertions.assertTrue(
                config.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("", "DB2INST1", "CUSTOMERS")));
        Assertions.assertFalse(
                config.getTableFilters()
                        .dataCollectionFilter()
                        .isIncluded(new TableId("", "DB2INST1", "ORDERS")));
        Assertions.assertTrue(config.isExactlyOnce());
        Assertions.assertFalse(config.isEnableConcurrentRead());
    }
}
