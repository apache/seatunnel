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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.opengauss;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenGaussDialectTest {

    @Test
    void returnsUpsertStatementWhenUpdateClauseIsNotEmpty() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeyFields = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);
        assertTrue(upsertStatement.isPresent());
        assertEquals(
                "INSERT INTO \"test_db\".\"test_table\" (\"id\", \"name\", \"age\") VALUES (:id, :name, :age) ON DUPLICATE KEY UPDATE \"name\"=EXCLUDED.\"name\", \"age\"=EXCLUDED.\"age\"",
                upsertStatement.get());
    }

    @Test
    void returnsEmptyWhenUpdateClauseIsEmpty() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id"};
        String[] uniqueKeyFields = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);
        assertFalse(upsertStatement.isPresent());
    }

    @Test
    void handlesEmptyFieldNames() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {};
        String[] uniqueKeyFields = {"id"};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);
        assertFalse(upsertStatement.isPresent());
    }

    @Test
    void handlesEmptyUniqueKeyFields() {
        OpenGaussDialect dialect = new OpenGaussDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeyFields = {};
        Optional<String> upsertStatement =
                dialect.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);
        assertTrue(upsertStatement.isPresent());
        assertEquals(
                "INSERT INTO \"test_db\".\"test_table\" (\"id\", \"name\", \"age\") VALUES (:id, :name, :age) ON DUPLICATE KEY UPDATE \"id\"=EXCLUDED.\"id\", \"name\"=EXCLUDED.\"name\", \"age\"=EXCLUDED.\"age\"",
                upsertStatement.get());
    }
}
