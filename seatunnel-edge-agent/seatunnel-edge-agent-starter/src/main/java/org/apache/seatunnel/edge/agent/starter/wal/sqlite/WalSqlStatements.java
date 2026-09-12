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

public class WalSqlStatements {

    static final String TABLE = "edge_agent_wal";

    static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS "
                    + TABLE
                    + " ("
                    + "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    + "batch_id INTEGER NOT NULL,"
                    + "source_id TEXT,"
                    + "payload BLOB NOT NULL,"
                    + "event_time INTEGER NOT NULL,"
                    + "metadata BLOB,"
                    + "status TEXT NOT NULL,"
                    + "attempt_count INTEGER NOT NULL DEFAULT 0,"
                    + "created_at INTEGER NOT NULL,"
                    + "updated_at INTEGER NOT NULL"
                    + ")";

    static final String CREATE_INDEX_STATUS_ID =
            "CREATE INDEX IF NOT EXISTS idx_edge_agent_wal_status_id ON " + TABLE + " (status, id)";

    static final String CREATE_INDEX_UPDATED_AT =
            "CREATE INDEX IF NOT EXISTS idx_edge_agent_wal_updated_at ON "
                    + TABLE
                    + " (updated_at)";

    static final String INSERT =
            "INSERT INTO "
                    + TABLE
                    + " (source_id, payload, event_time, metadata, status, attempt_count,"
                    + " created_at, updated_at, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    static final String MIGRATE_ADD_BATCH_ID =
            "ALTER TABLE " + TABLE + " ADD COLUMN batch_id INTEGER";

    static final String MIGRATE_BACKFILL_BATCH_ID =
            "UPDATE " + TABLE + " SET batch_id = id WHERE batch_id IS NULL";

    static final String UPDATE_STATUS_BY_ID =
            "UPDATE " + TABLE + " SET status = ?, updated_at = ? WHERE id = ?";

    static final String MARK_SENDING =
            "UPDATE "
                    + TABLE
                    + " SET status = ?, attempt_count = attempt_count + 1, updated_at = ?"
                    + " WHERE id = ? AND status = ?";

    private static final String SELECT_COLUMNS =
            "id, batch_id, source_id, payload, event_time, metadata, status, attempt_count,"
                    + " created_at, updated_at";

    static final String SELECT_BY_STATUS_ORDER_BY_ID_ASC =
            "SELECT "
                    + SELECT_COLUMNS
                    + " FROM "
                    + TABLE
                    + " WHERE status = ? ORDER BY id ASC LIMIT ?";

    static final String SELECT_PENDING_ORDER_BY_ID_ASC =
            "SELECT "
                    + SELECT_COLUMNS
                    + " FROM "
                    + TABLE
                    + " WHERE status = ? AND attempt_count < ? ORDER BY id ASC LIMIT ?";

    static final String SELECT_BY_STATUS_ORDER_BY_UPDATED_AT_ASC =
            "SELECT "
                    + SELECT_COLUMNS
                    + " FROM "
                    + TABLE
                    + " WHERE status = ? ORDER BY updated_at ASC LIMIT ?";

    static final String SELECT_STALE_SENDING_ORDER_BY_UPDATED_AT_ASC =
            "SELECT "
                    + SELECT_COLUMNS
                    + " FROM "
                    + TABLE
                    + " WHERE status = ? AND updated_at <= ? ORDER BY updated_at ASC LIMIT ?";

    static final String SELECT_EXCEEDED_PENDING_IDS =
            "SELECT id FROM "
                    + TABLE
                    + " WHERE status = ? AND attempt_count >= ? ORDER BY id ASC LIMIT ?";

    static final String MARK_EXCEEDED_AS_DEAD =
            "UPDATE " + TABLE + " SET status = ?, updated_at = ? WHERE id = ? AND status = ?";

    static final String DELETE_BY_ID = "DELETE FROM " + TABLE + " WHERE id = ?";

    static final String SELECT_ACKED_IDS_FOR_CLEANUP =
            "SELECT id FROM "
                    + TABLE
                    + " WHERE status = ? AND updated_at <= ? ORDER BY updated_at ASC LIMIT ?";
}
