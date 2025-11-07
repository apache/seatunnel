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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DB2DialectTest {

    @Test
    void testStatement() {
        DB2Dialect dialect = new DB2Dialect();
        final String database = "seatunnel";
        final String tableName = "test_schema.role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time", "id_2"
        };
        final String[] doUpdateKeyFields = {"id", "id_2"};

        // test upsert sql
        String upsertSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, doUpdateKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected doUpdateSql String to be present"));
        Assertions.assertEquals(
                "MERGE INTO \"seatunnel\".\"test_schema\".\"role\" AS target USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\", \"id_2\") ON target.\"id\" = source.\"id\" AND target.\"id_2\" = source.\"id_2\" WHEN MATCHED AND (target.\"id\" <> source.\"id\" OR target.\"type\" <> source.\"type\" OR target.\"role_name\" <> source.\"role_name\" OR target.\"description\" <> source.\"description\" OR target.\"create_time\" <> source.\"create_time\" OR target.\"update_time\" <> source.\"update_time\" OR target.\"id_2\" <> source.\"id_2\") THEN UPDATE SET target.\"id\" = source.\"id\", target.\"type\" = source.\"type\", target.\"role_name\" = source.\"role_name\", target.\"description\" = source.\"description\", target.\"create_time\" = source.\"create_time\", target.\"update_time\" = source.\"update_time\", target.\"id_2\" = source.\"id_2\" WHEN NOT MATCHED THEN INSERT (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\", \"id_2\") VALUES (source.\"id\", source.\"type\", source.\"role_name\", source.\"description\", source.\"create_time\", source.\"update_time\", source.\"id_2\")",
                upsertSql);

        // test insert sql
        String insertSql = dialect.getInsertIntoStatement(database, tableName, fieldNames);
        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"test_schema\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\", \"id_2\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time, :id_2)",
                insertSql);
    }
}
