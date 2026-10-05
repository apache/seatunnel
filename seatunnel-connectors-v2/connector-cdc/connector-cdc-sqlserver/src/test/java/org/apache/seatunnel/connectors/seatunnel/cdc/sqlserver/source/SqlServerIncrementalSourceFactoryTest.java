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
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfigFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

class SqlServerIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new SqlServerIncrementalSourceFactory()).optionRule());
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
