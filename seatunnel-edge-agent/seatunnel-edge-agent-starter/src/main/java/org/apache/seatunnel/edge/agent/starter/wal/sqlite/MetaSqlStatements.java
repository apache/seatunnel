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

package org.apache.seatunnel.edge.agent.starter.wal.sqlite;

public class MetaSqlStatements {

    private static final String META_TABLE = "edge_agent_meta";

    static final String KEY_NEXT_BATCH_ID = "next_batch_id";

    static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS "
                    + META_TABLE
                    + " (key TEXT PRIMARY KEY, value INTEGER NOT NULL)";

    static final String SELECT_VALUE = "SELECT value FROM " + META_TABLE + " WHERE key = ?";

    static final String UPDATE_VALUE = "UPDATE " + META_TABLE + " SET value = ? WHERE key = ?";

    static final String SEED_NEXT_BATCH_ID_FROM_WAL =
            "INSERT INTO "
                    + META_TABLE
                    + " (key, value) SELECT '"
                    + KEY_NEXT_BATCH_ID
                    + "', COALESCE((SELECT MAX(batch_id) FROM "
                    + WalSqlStatements.TABLE
                    + "), 0) + 1 WHERE NOT EXISTS (SELECT 1 FROM "
                    + META_TABLE
                    + " WHERE key = '"
                    + KEY_NEXT_BATCH_ID
                    + "')";
}
