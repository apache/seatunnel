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

package org.apache.seatunnel.edge.agent.starter.command.db;

public class EdgeAgentDbSql {

    static final String WAL_TABLE = "edge_agent_wal";
    static final String POSITION_TABLE = "edge_agent_source_position";

    static final String WAL_COUNT_BY_STATUS =
            "SELECT status, COUNT(*) AS cnt FROM " + WAL_TABLE + " GROUP BY status ORDER BY status";

    static final String WAL_OLDEST_BY_STATUS =
            "SELECT status, MIN(updated_at) AS oldest_updated_at FROM "
                    + WAL_TABLE
                    + " GROUP BY status ORDER BY status";

    static final String WAL_SHOW =
            "SELECT id, batch_id, status, attempt_count, created_at, updated_at, source_id,"
                    + " event_time, payload FROM "
                    + WAL_TABLE
                    + " WHERE id = ?";

    static final String WAL_LIST =
            "SELECT id, batch_id, status, attempt_count, updated_at, source_id,"
                    + " length(payload) AS payload_bytes FROM "
                    + WAL_TABLE
                    + " WHERE (? IS NULL OR status = ?) ORDER BY id ASC LIMIT ?";

    static final String WAL_COUNT_BY_STATUS_FILTER =
            "SELECT COUNT(*) FROM " + WAL_TABLE + " WHERE status = ?";

    static final String WAL_PURGE_DEAD = "DELETE FROM " + WAL_TABLE + " WHERE status = ?";

    static final String WAL_RETRY_DEAD =
            "UPDATE "
                    + WAL_TABLE
                    + " SET status = ?, attempt_count = 0, updated_at = ? WHERE status = ?";

    static final String WAL_UNSTICK_SENDING =
            "UPDATE " + WAL_TABLE + " SET status = ?, updated_at = ? WHERE status = ?";

    static final String WAL_COUNT_ACKED_BEFORE =
            "SELECT COUNT(*) FROM " + WAL_TABLE + " WHERE status = ? AND updated_at < ?";

    static final String WAL_PURGE_ACKED_BEFORE =
            "DELETE FROM " + WAL_TABLE + " WHERE status = ? AND updated_at < ?";

    static final String POSITION_LIST =
            "SELECT source_id, partition_key, offset_value, updated_at FROM "
                    + POSITION_TABLE
                    + " WHERE (? IS NULL OR source_id = ?) ORDER BY source_id, partition_key";
}
