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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbDialectFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.vertica.VerticaDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu.XuguDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Regression test for <a href="https://github.com/apache/seatunnel/issues/11729">issue #11729</a>:
 * when every column of a table is part of the primary/unique key (an "all-key" table), the JDBC
 * MERGE-based upsert statement must omit the {@code WHEN MATCHED THEN UPDATE SET} clause entirely,
 * instead of emitting it with an empty body (which is invalid SQL and fails on DM / Oracle / SAP
 * HANA / SQL Server / Vertica / Xugu).
 */
public class JdbcAllKeyTableUpsertTest {

    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";
    private static final String[] ALL_FIELDS = {"id", "name", "age"};

    private List<JdbcDialect> dialects() {
        return Arrays.asList(
                new DmdbDialectFactory().create(),
                new OracleDialect(),
                new SapHanaDialect(),
                new SqlServerDialect(),
                new VerticaDialect(),
                new XuguDialect());
    }

    @Test
    void testAllKeyTableOmitsEmptyUpdateSet() {
        for (JdbcDialect dialect : dialects()) {
            // Every column is a key -> there is nothing to update when a row matches.
            Optional<String> upsert =
                    dialect.getUpsertStatement(DATABASE, TABLE, ALL_FIELDS, ALL_FIELDS);
            Assertions.assertTrue(
                    upsert.isPresent(),
                    () -> dialect.dialectName() + ": upsert statement should be present");
            String sql = upsert.get().toUpperCase();
            Assertions.assertFalse(
                    sql.contains("WHEN MATCHED THEN UPDATE SET"),
                    () ->
                            dialect.dialectName()
                                    + ": all-key table must NOT emit an empty 'WHEN MATCHED THEN UPDATE SET' (got: "
                                    + sql
                                    + ")");
            Assertions.assertTrue(
                    sql.contains("WHEN NOT MATCHED"),
                    () ->
                            dialect.dialectName()
                                    + ": all-key table must still insert unmatched rows (got: "
                                    + sql
                                    + ")");
            Assertions.assertTrue(
                    sql.contains("INSERT"),
                    () ->
                            dialect.dialectName()
                                    + ": all-key table statement must contain an INSERT branch (got: "
                                    + sql
                                    + ")");
        }
    }

    @Test
    void testPartialKeyTableStillUpdates() {
        // Only "id" is the key; "name" and "age" must still be updated on match.
        String[] uniqueKeys = {"id"};
        for (JdbcDialect dialect : dialects()) {
            Optional<String> upsert =
                    dialect.getUpsertStatement(DATABASE, TABLE, ALL_FIELDS, uniqueKeys);
            Assertions.assertTrue(
                    upsert.isPresent(),
                    () -> dialect.dialectName() + ": upsert statement should be present");
            String sql = upsert.get().toUpperCase();
            Assertions.assertTrue(
                    sql.contains("WHEN MATCHED THEN UPDATE SET"),
                    () ->
                            dialect.dialectName()
                                    + ": partial-key table must still emit 'WHEN MATCHED THEN UPDATE SET' (got: "
                                    + sql
                                    + ")");
        }
    }
}
