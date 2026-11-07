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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class SapHanaDialectTest {

    @Test
    void testAllKeyTableOmitsEmptyUpdateSet() {
        JdbcDialect dialect = new SapHanaDialect();
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
        JdbcDialect dialect = new SapHanaDialect();
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

    @Test
    void testSingleKeyOnlyUpsertOmitsMatchedUpdate() {
        String sql =
                new SapHanaDialect()
                        .getUpsertStatement("S", "T", new String[] {"ID"}, new String[] {"ID"})
                        .get();

        Assertions.assertEquals(
                " MERGE INTO \"S\".\"T\" USING (SELECT :ID AS \"ID\" FROM DUMMY) AS SOURCE"
                        + " ON (\"S\".\"T\".\"ID\"=SOURCE.\"ID\") "
                        + " WHEN NOT MATCHED THEN INSERT (\"ID\") VALUES (SOURCE.\"ID\")",
                sql);
    }

    @Test
    void testCompositeKeyOnlyUpsertOmitsMatchedUpdate() {
        String sql =
                new SapHanaDialect()
                        .getUpsertStatement(
                                "S", "T", new String[] {"ID", "PART"}, new String[] {"ID", "PART"})
                        .get();

        Assertions.assertEquals(
                " MERGE INTO \"S\".\"T\" USING (SELECT :ID AS \"ID\", :PART AS \"PART\" FROM DUMMY) AS SOURCE"
                        + " ON (\"S\".\"T\".\"ID\"=SOURCE.\"ID\" AND \"S\".\"T\".\"PART\"=SOURCE.\"PART\") "
                        + " WHEN NOT MATCHED THEN INSERT (\"ID\", \"PART\") VALUES (SOURCE.\"ID\", SOURCE.\"PART\")",
                sql);
    }

    @Test
    void testUpsertWithPayloadPreservesMatchedUpdate() {
        String sql =
                new SapHanaDialect()
                        .getUpsertStatement(
                                "S", "T", new String[] {"ID", "PAYLOAD"}, new String[] {"ID"})
                        .get();

        Assertions.assertEquals(
                " MERGE INTO \"S\".\"T\" USING (SELECT :ID AS \"ID\", :PAYLOAD AS \"PAYLOAD\" FROM DUMMY) AS SOURCE"
                        + " ON (\"S\".\"T\".\"ID\"=SOURCE.\"ID\") "
                        + " WHEN MATCHED THEN UPDATE SET \"PAYLOAD\"=SOURCE.\"PAYLOAD\" "
                        + " WHEN NOT MATCHED THEN INSERT (\"ID\", \"PAYLOAD\") VALUES (SOURCE.\"ID\", SOURCE.\"PAYLOAD\")",
                sql);
    }
}
