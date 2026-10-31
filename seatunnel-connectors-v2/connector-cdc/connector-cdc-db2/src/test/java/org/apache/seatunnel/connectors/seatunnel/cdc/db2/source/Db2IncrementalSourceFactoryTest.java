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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.db2.config.Db2SourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Covers DB2 factory contracts and consistent unquoted table names without losing split overrides.
 */
class Db2IncrementalSourceFactoryTest {
    private static final String VALID_URL = "jdbc:db2://localhost:50000/seatunnel";

    private final OptionRule optionRule = new Db2IncrementalSourceFactory().optionRule();

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new Db2IncrementalSourceFactory()).optionRule());
    }

    @ParameterizedTest
    @MethodSource("nonblankUrls")
    void testNonblankUrlAcceptedWithoutChangingValue(String key, String value) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        config.put(key, value);

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        // Validation does not trim a supplied URL or check whether it can connect.
        Assertions.assertEquals(value, readonlyConfig.get(Db2IncrementalSourceOptions.URL));
    }

    private static Stream<Arguments> nonblankUrls() {
        return Stream.of("url", "base-url")
                .flatMap(
                        key ->
                                Stream.of(VALID_URL, " \t" + VALID_URL + " \r\n")
                                        .map(value -> Arguments.of(key, value)));
    }

    @ParameterizedTest
    @MethodSource("blankUrls")
    void testBlankUrlRejected(String key, String value) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        config.put(key, value);

        assertInvalidUrl(config);
    }

    private static Stream<Arguments> blankUrls() {
        return Stream.of("url", "base-url")
                .flatMap(
                        key ->
                                Stream.of("", " ", "\t", "\n", " \t\r\n ")
                                        .map(value -> Arguments.of(key, value)));
    }

    @Test
    void testMissingUrlRejected() {
        Map<String, Object> config = validConfig();
        config.remove("url");

        assertInvalidUrl(config);
    }

    @Test
    void testCanonicalUrlTakesPrecedenceOverFallback() {
        Map<String, Object> config = validConfig();
        config.put("base-url", " ");

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(VALID_URL, readonlyConfig.get(Db2IncrementalSourceOptions.URL));
    }

    @Test
    void testBlankCanonicalUrlDoesNotUseValidFallback() {
        Map<String, Object> config = validConfig();
        config.put("url", " \t ");
        config.put("base-url", VALID_URL);

        assertInvalidUrl(config);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", VALID_URL);
        config.put("username", "test-user");
        config.put("password", "test-password");
        config.put("table-names", Collections.singletonList("seatunnel.DB2INST1.CUSTOMERS"));
        return config;
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "DB2-CDC");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }

    private void assertInvalidUrl(Map<String, Object> config) {
        OptionValidationException error =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(error.getMessage().contains("url"), error.getMessage());
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

        ReadonlyConfig normalized =
                Db2IncrementalSourceFactory.normalizeDb2Options(original, tablePath -> true);
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
                Db2IncrementalSourceFactory.normalizeDb2Options(
                        ReadonlyConfig.fromMap(raw), tablePath -> true);
        Assertions.assertEquals(
                "e2e.DB2INST1.Customer.*", normalized.get(ConnectorCommonOptions.TABLE_PATTERN));
        Assertions.assertFalse(
                normalized.getOptional(ConnectorCommonOptions.TABLE_NAMES).isPresent());

        raw.remove("table-pattern");
        raw.put("table-names", Collections.singletonList("E2E.DB2INST1.\"Customers\""));
        normalized =
                Db2IncrementalSourceFactory.normalizeDb2Options(
                        ReadonlyConfig.fromMap(raw), tablePath -> true);
        Assertions.assertEquals(
                Collections.singletonList("E2E.DB2INST1.\"Customers\""),
                normalized.get(ConnectorCommonOptions.TABLE_NAMES));
    }

    /**
     * Objects created with quoted lowercase identifiers were matched by unquoted lowercase
     * configuration before folding existed. When only the configured spelling exists, both {@code
     * table-names} and {@code table-names-config.table} must keep it; otherwise the upgraded job
     * fails at startup with a table that is no longer found or captured.
     */
    @Test
    void shouldKeepConfiguredSpellingWhenOnlyLowercaseCreatedObjectExists() {
        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("table", "e2e.db2inst1.customers");
        tableConfig.put("primaryKeys", Collections.singletonList("id"));
        Map<String, Object> raw = new HashMap<>();
        raw.put("table-names", Arrays.asList("e2e.db2inst1.customers", "e2e.db2inst1.orders"));
        raw.put("table-names-config", Collections.singletonList(tableConfig));
        TablePath lowercaseCustomers = TablePath.of("e2e", "db2inst1", "customers");
        TablePath uppercaseOrders = TablePath.of("e2e", "DB2INST1", "ORDERS");

        ReadonlyConfig normalized =
                Db2IncrementalSourceFactory.normalizeDb2Options(
                        ReadonlyConfig.fromMap(raw),
                        tablePath ->
                                tablePath.equals(lowercaseCustomers)
                                        || tablePath.equals(uppercaseOrders));

        Assertions.assertEquals(
                Arrays.asList("e2e.db2inst1.customers", "e2e.DB2INST1.ORDERS"),
                normalized.get(ConnectorCommonOptions.TABLE_NAMES));
        JdbcSourceTableConfig table =
                normalized.get(Db2IncrementalSourceOptions.TABLE_NAMES_CONFIG).get(0);
        Assertions.assertEquals("e2e.db2inst1.customers", table.getTable());
        Assertions.assertEquals(Collections.singletonList("id"), table.getPrimaryKeys());
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
                Db2IncrementalSourceFactory.normalizeDb2Options(
                        ReadonlyConfig.fromMap(raw), tablePath -> true));
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
