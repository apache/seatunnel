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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class KingbaseDialectTableOptionsTest {

    @Test
    void testValidateTableOptionsAcceptsSupportedKeys() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "pg_default");
        tableOptions.put("fillfactor", "70");
        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    void testValidateTableOptionsFillfactorBoundary() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "10")));
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "100")));
    }

    @Test
    void testValidateTableOptionsRejectsUnsupportedKeys() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dialect.validateTableOptions(tableOptions));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("KingBase"));
    }

    @Test
    void testValidateTableOptionsRejectBlankValues() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException blankTablespace =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", " ")));
        Assertions.assertTrue(blankTablespace.getMessage().contains("must not be blank"));

        JdbcConnectorException blankFillfactor =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", " ")));
        Assertions.assertTrue(blankFillfactor.getMessage().contains("must not be blank"));
    }

    @Test
    void testValidateTableOptionsRejectInvalidFillfactor() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException nonNumeric =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "abc")));
        Assertions.assertTrue(nonNumeric.getMessage().contains("must be an integer between"));

        JdbcConnectorException tooLow =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "9")));
        Assertions.assertTrue(tooLow.getMessage().contains("must be an integer between"));

        JdbcConnectorException tooHigh =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "101")));
        Assertions.assertTrue(tooHigh.getMessage().contains("must be an integer between"));
    }

    @Test
    void testValidateTableOptionsRejectIllegalTablespace() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", "pg_\"default\"")));
        Assertions.assertTrue(exception.getMessage().contains("illegal characters"));
    }
}
