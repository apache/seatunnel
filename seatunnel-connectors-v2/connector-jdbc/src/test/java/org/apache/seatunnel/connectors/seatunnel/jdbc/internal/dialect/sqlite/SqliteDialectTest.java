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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlite;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqliteDialectTest {
    @Test
    void testUpsertStatement() {
        SqliteDialect dialect = new SqliteDialect();
        final String database = "seatunnel";
        final String tableName = "role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };
        final String[] uniqueKeyFields = {"id"};

        String upsertSql =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Expected upsertSql String to be present"));
        // SqliteDialect uses backticks (`) for quoting identifiers (table only, not database)
        // Note: SqliteDialect always generates DO UPDATE SET, not DO NOTHING
        Assertions.assertEquals(
                "INSERT INTO seatunnel.`role` (`id`, `type`, `role_name`, `description`, `create_time`, `update_time`) VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT(`id`) DO UPDATE SET `id`=EXCLUDED.`id`, `type`=EXCLUDED.`type`, `role_name`=EXCLUDED.`role_name`, `description`=EXCLUDED.`description`, `create_time`=EXCLUDED.`create_time`, `update_time`=EXCLUDED.`update_time`",
                upsertSql);
    }
}
