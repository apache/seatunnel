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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DmdbDialectTest {
    @Test
    public void testIdentifierCaseSensitive() {
        DmdbDialectFactory factory = new DmdbDialectFactory();

        JdbcDialect dialect = factory.create();
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));
    }

    @Test
    void testValidateTableOptionsAcceptsSupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "MAIN");
        tableOptions.put("fillfactor", "80");
        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    void testValidateTableOptionsFillfactorBoundary() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "0")));
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "100")));
    }

    @Test
    void testValidateTableOptionsRejectsUnsupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("pctfree", "10");
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dialect.validateTableOptions(tableOptions));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Dameng"));
    }

    @Test
    void testValidateTableOptionsRejectBlankValues() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

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
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException nonNumeric =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "abc")));
        Assertions.assertTrue(nonNumeric.getMessage().contains("must be an integer between"));

        JdbcConnectorException outOfRange =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "101")));
        Assertions.assertTrue(outOfRange.getMessage().contains("must be an integer between"));
    }

    @Test
    void testValidateTableOptionsRejectIllegalTablespace() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", "MAIN\"TS")));
        Assertions.assertTrue(exception.getMessage().contains("illegal characters"));
    }

    @Test
    void testNormalizeFillfactorAcceptsPlusSignAndLeadingZeros() {
        Assertions.assertEquals("80", DmdbDialect.normalizeFillfactorForDdl("+80"));
        Assertions.assertEquals("80", DmdbDialect.normalizeFillfactorForDdl("080"));
    }

    @Test
    void testNormalizeFillfactorRejectsOutOfRange() {
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> DmdbDialect.normalizeFillfactorForDdl("101"));
        Assertions.assertTrue(exception.getMessage().contains("must be an integer between"));
    }

    @Test
    void testNormalizeTablespaceRejectsControlCharacters() {
        JdbcConnectorException tabCharacter =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> DmdbDialect.normalizeTablespaceForDdl("MAIN\tTS"));
        Assertions.assertTrue(tabCharacter.getMessage().contains("illegal characters"));
    }

    @Test
    void testAllKeyTableOmitsEmptyUpdateSet() {
        JdbcDialect dialect = new DmdbDialectFactory().create();
        String[] allFields = {"id", "name", "age"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, allFields);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertFalse(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "all-key table must NOT emit an empty 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
        Assertions.assertTrue(
                sql.contains("WHEN NOT MATCHED"), "all-key table must still insert unmatched rows");
        Assertions.assertTrue(
                sql.contains("INSERT"), "all-key table statement must contain an INSERT branch");
    }

    @Test
    void testPartialKeyTableStillUpdates() {
        JdbcDialect dialect = new DmdbDialectFactory().create();
        String[] allFields = {"id", "name", "age"};
        String[] uniqueKeys = {"id"};
        Optional<String> upsert =
                dialect.getUpsertStatement("test_db", "test_table", allFields, uniqueKeys);
        Assertions.assertTrue(upsert.isPresent(), "upsert statement should be present");
        String sql = upsert.get().toUpperCase();
        Assertions.assertTrue(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "partial-key table must still emit 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
    }
}
