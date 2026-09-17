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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MySqlIncrementalSourceFactoryTest {

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testImplementsSupportSourceDryRunValidation() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Assertions.assertInstanceOf(SupportSourceDryRunValidation.class, factory);
    }

    @Test
    public void testValidateConnectionForDryRunFailsWithInvalidUrl() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://invalid-host-that-does-not-exist:3306/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader());

        // validateConnectionForDryRun should throw because the host is unreachable
        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> factory.validateConnectionForDryRun(context, Collections.emptyList()));
    }

    @Test
    public void testValidateConnectionForDryRunFailsWithNullUrl() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("username", "testuser");
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader());

        // Should throw because URL is null
        Assertions.assertThrows(
                Exception.class,
                () -> factory.validateConnectionForDryRun(context, Collections.emptyList()));
    }

    @Test
    public void testInferSchemaForDryRunFailsWithInvalidUrl() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://invalid-host-that-does-not-exist:3306/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader());

        // inferSchemaForDryRun should throw because the host is unreachable
        Assertions.assertThrows(Exception.class, () -> factory.inferSchemaForDryRun(context));
    }

    @Test
    public void testRestoreSourceFailsWithInvalidUrl() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://invalid-host-that-does-not-exist:3306/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader());

        // restoreSource should throw because permission validation fails (unreachable host)
        Assertions.assertThrows(
                SeaTunnelException.class,
                () -> factory.restoreSource(context, Collections.emptyList()));
    }

    @Test
    public void testValidateConnectionErrorMessageContainsUsername() {
        MySqlIncrementalSourceFactory factory = new MySqlIncrementalSourceFactory();
        String testUsername = "my_test_user";
        Map<String, Object> config = new HashMap<>();
        config.put("url", "jdbc:mysql://invalid-host-that-does-not-exist:3306/testdb");
        config.put("username", testUsername);
        config.put("password", "testpass");
        config.put("table-names", Collections.singletonList("testdb.test_table"));

        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(config),
                        Thread.currentThread().getContextClassLoader());

        SeaTunnelException exception =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                factory.validateConnectionForDryRun(
                                        context, Collections.emptyList()));
        // Error message should contain the username for debugging
        Assertions.assertTrue(
                exception.getMessage().contains(testUsername),
                "Error message should contain username, actual: " + exception.getMessage());
    }
}
