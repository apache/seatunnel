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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OracleDialectSqlTest {

    @Test
    void testUpsertStatementAllKey() {
        OracleDialect dialect = new OracleDialect();
        String[] fieldNames = {"id", "name", "age"};
        String[] allKeyFields = {"id", "name", "age"};

        String sql =
                dialect.getUpsertStatement("test_schema", "test_table", fieldNames, allKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected upsert SQL to be present"));

        Assertions.assertFalse(
                sql.contains("WHEN MATCHED"),
                "All-key upsert SQL should not contain WHEN MATCHED clause");
        Assertions.assertTrue(
                sql.contains("WHEN NOT MATCHED"),
                "All-key upsert SQL should contain WHEN NOT MATCHED clause");
    }
}