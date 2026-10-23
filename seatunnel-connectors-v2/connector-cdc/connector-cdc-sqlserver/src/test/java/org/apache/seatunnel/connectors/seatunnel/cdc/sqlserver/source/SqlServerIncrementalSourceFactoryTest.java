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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

class SqlServerIncrementalSourceFactoryTest {
    private static final String VALID_URL =
            "jdbc:sqlserver://localhost:1433;databaseName=seatunnel";

    private final OptionRule optionRule = new SqlServerIncrementalSourceFactory().optionRule();

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new SqlServerIncrementalSourceFactory()).optionRule());
    }

    @ParameterizedTest
    @ValueSource(strings = {"url", "base-url"})
    void testNonblankUrlAccepted(String key) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        config.put(key, VALID_URL);

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(
                VALID_URL, readonlyConfig.get(SqlServerIncrementalSourceOptions.URL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"url", "base-url"})
    void testPaddedNonblankUrlAcceptedWithoutTrimming(String key) {
        Map<String, Object> config = validConfig();
        config.remove("url");
        String value = " \t" + VALID_URL + " \r\n";
        config.put(key, value);

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(value, readonlyConfig.get(SqlServerIncrementalSourceOptions.URL));
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
                                Stream.of("", " ", "\t", "\n", "\r", " \t\r\n ")
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
        config.put("base-url", "jdbc:sqlserver://other-host:1433;databaseName=other");

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals(
                VALID_URL, readonlyConfig.get(SqlServerIncrementalSourceOptions.URL));
    }

    @Test
    void testValidCanonicalUrlIgnoresBlankFallback() {
        Map<String, Object> config = validConfig();
        config.put("base-url", " \t ");

        Assertions.assertDoesNotThrow(() -> validate(config));
    }

    @Test
    void testBlankCanonicalUrlDoesNotUseValidFallback() {
        Map<String, Object> config = validConfig();
        config.put("url", " \t ");
        config.put("base-url", VALID_URL);

        assertInvalidUrl(config);
    }

    @Test
    void testEmptyPasswordRemainsAccepted() {
        Map<String, Object> config = validConfig();
        config.put("password", "");

        ReadonlyConfig readonlyConfig = Assertions.assertDoesNotThrow(() -> validate(config));

        Assertions.assertEquals("", readonlyConfig.get(SqlServerIncrementalSourceOptions.PASSWORD));
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", VALID_URL);
        config.put("username", "sa");
        config.put("password", "Password!");
        config.put("table-names", Collections.singletonList("seatunnel.dbo.orders"));
        return config;
    }

    private ReadonlyConfig validate(Map<String, Object> config) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        ConfigValidator.validateUnknownKeys(readonlyConfig, optionRule, "SqlServer-CDC");
        ConfigValidator.of(readonlyConfig).validate(optionRule);
        return readonlyConfig;
    }

    private void assertInvalidUrl(Map<String, Object> config) {
        OptionValidationException error =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(config));

        Assertions.assertTrue(error.getMessage().contains("url"), error.getMessage());
    }

    /**
     * SQLServer CDC source creation must accept the driver-specific databaseName URL syntax during
     * the submission-time catalog validation step.
     */
    @Test
    public void testCreateOptionalCatalogWithSqlServerStyleUrl() {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:sqlserver://localhost:1433;databaseName=seatunnel");
        config.put("username", "sa");
        config.put("password", "Password!");
        config.put("database-names", Arrays.asList("seatunnel"));
        config.put("table-names", Arrays.asList("seatunnel.dbo.orders"));

        Optional<Catalog> catalog =
                FactoryUtil.createOptionalCatalog(
                        "SqlServer",
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader(),
                        "SqlServer");

        Assertions.assertTrue(catalog.isPresent());
        catalog.ifPresent(Catalog::close);
    }

    @Test
    public void testForwardJdbcUrlPropertiesToDebeziumConnection() {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.encrypt", "false");
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname("localhost")
                                .port(1433)
                                .username("sa")
                                .password("Password!")
                                .databaseList("seatunnel");
        configFactory.jdbcUrlProperties(
                SqlServerIncrementalSource.toDebeziumJdbcProperties(
                        "databaseName=seatunnel;encrypt=true;trustServerCertificate=true"));
        configFactory.debeziumProperties(debeziumProperties);
        SqlServerSourceConfig sourceConfig = configFactory.create(0);

        Assertions.assertEquals(
                "false", sourceConfig.getDbzConfiguration().getString("database.encrypt"));
        Assertions.assertEquals(
                "true",
                sourceConfig.getDbzConfiguration().getString("database.trustServerCertificate"));
        Assertions.assertNull(
                sourceConfig.getDbzConfiguration().getString("database.databaseName"));
    }

    /**
     * URL-embedded {@code user} / {@code password} segments must never override the
     * operator-configured {@code username} / {@code password} options — doing so would silently
     * change the credentials the job connects with.
     */
    @Test
    public void testJdbcUrlCredentialsDoNotOverrideConfiguredCredentials() {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname("localhost")
                                .port(1433)
                                .username("configured-user")
                                .password("configured-password")
                                .databaseList("seatunnel");
        configFactory.jdbcUrlProperties(
                SqlServerIncrementalSource.toDebeziumJdbcProperties(
                        "user=url-user;password=url-password;encrypt=true"));
        SqlServerSourceConfig sourceConfig = configFactory.create(0);

        // The configured username/password must win — URL-embedded user/password must be
        // filtered out before they are forwarded into the Debezium Properties bag.
        Assertions.assertEquals(
                "configured-user", sourceConfig.getDbzConfiguration().getString("database.user"));
        Assertions.assertEquals(
                "configured-password",
                sourceConfig.getDbzConfiguration().getString("database.password"));
        Assertions.assertNull(sourceConfig.getDbzConfiguration().getString("database.url-user"));
        Assertions.assertNull(
                sourceConfig.getDbzConfiguration().getString("database.url-password"));
        // Forwarding of unrelated URL properties is still in effect.
        Assertions.assertEquals(
                "true", sourceConfig.getDbzConfiguration().getString("database.encrypt"));
    }
}
