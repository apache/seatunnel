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

final class SourcePositionSqlStatements {

    private static final String TABLE = "edge_agent_source_position";

    static final String CREATE_TABLE =
            "CREATE TABLE IF NOT EXISTS "
                    + TABLE
                    + " ("
                    + "source_id TEXT NOT NULL,"
                    + "partition_key TEXT NOT NULL,"
                    + "offset_value INTEGER NOT NULL,"
                    + "metadata BLOB,"
                    + "updated_at INTEGER NOT NULL,"
                    + "PRIMARY KEY (source_id, partition_key)"
                    + ")";

    static final String SELECT_BY_SOURCE_AND_PARTITION =
            "SELECT source_id, partition_key, offset_value, metadata, updated_at FROM "
                    + TABLE
                    + " WHERE source_id = ? AND partition_key = ?";

    static final String SELECT_BY_SOURCE =
            "SELECT source_id, partition_key, offset_value, metadata, updated_at FROM "
                    + TABLE
                    + " WHERE source_id = ? ORDER BY partition_key ASC";

    static final String UPSERT =
            "INSERT INTO "
                    + TABLE
                    + " (source_id, partition_key, offset_value, metadata, updated_at)"
                    + " VALUES (?, ?, ?, ?, ?)"
                    + " ON CONFLICT(source_id, partition_key) DO UPDATE SET"
                    + " offset_value = excluded.offset_value,"
                    + " metadata = excluded.metadata,"
                    + " updated_at = excluded.updated_at";
}
