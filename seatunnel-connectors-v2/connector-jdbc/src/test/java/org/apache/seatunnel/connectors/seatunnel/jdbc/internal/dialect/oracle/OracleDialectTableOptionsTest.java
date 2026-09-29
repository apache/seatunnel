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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OracleDialectTableOptionsTest {

    @Test
    public void testValidateTableOptions() {
        OracleDialect dialect = new OracleDialect();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "USERS");
        tableOptions.put("pctfree", "10");

        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    public void testValidateTableOptionsPctfreeBoundary() {
        OracleDialect dialect = new OracleDialect();
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("pctfree", "0")));
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("pctfree", "99")));
    }

    @Test
    public void testValidateTableOptionsWithUnknownKey() {
        OracleDialect dialect = new OracleDialect();

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("engine", "InnoDB")));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Oracle"));
    }

    @Test
    public void testValidateTableOptionsRejectBlankValues() {
        OracleDialect dialect = new OracleDialect();

        JdbcConnectorException blankTablespace =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", " ")));
        Assertions.assertTrue(blankTablespace.getMessage().contains("must not be blank"));

        JdbcConnectorException blankPctfree =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("pctfree", " ")));
        Assertions.assertTrue(blankPctfree.getMessage().contains("must not be blank"));
    }

    @Test
    public void testValidateTableOptionsRejectInvalidPctfree() {
        OracleDialect dialect = new OracleDialect();

        JdbcConnectorException nonNumeric =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("pctfree", "abc")));
        Assertions.assertTrue(nonNumeric.getMessage().contains("must be an integer between"));

        JdbcConnectorException outOfRange =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("pctfree", "100")));
        Assertions.assertTrue(outOfRange.getMessage().contains("must be an integer between"));
    }

    @Test
    public void testValidateTableOptionsRejectIllegalTablespace() {
        OracleDialect dialect = new OracleDialect();

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", "USER\"S")));
        Assertions.assertTrue(exception.getMessage().contains("illegal characters"));
    }
}
