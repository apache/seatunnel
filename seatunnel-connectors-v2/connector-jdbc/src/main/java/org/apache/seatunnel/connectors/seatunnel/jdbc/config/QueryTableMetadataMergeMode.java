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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Locale;

/**
 * How much metadata of the resolved underlying physical table is merged into the schema derived
 * from a query-only source table (a table defined by {@code query} without {@code table_path}). The
 * merge only applies when JDBC metadata verifies that every result column originates from the same
 * physical table.
 */
public enum QueryTableMetadataMergeMode {

    /**
     * Do not resolve the underlying physical table; use the query-derived schema as-is (the
     * behavior before the merge feature was introduced).
     */
    NONE,

    /**
     * Merge only the metadata that cannot change runtime behavior: column comments, the table
     * comment and the table options. The primary key, constraint keys and partition keys are not
     * merged, so sinks with {@code generate_sink_sql} keep plain inserts and split planning is
     * unchanged. This is the default.
     */
    COMMENT,

    /**
     * Additionally merge the primary key, constraint keys and partition keys — the same result as
     * configuring {@code table_path} together with {@code query}. This may switch sinks with {@code
     * generate_sink_sql} from insert to upsert and enable primary-key based split planning.
     */
    ALL;

    /** Accepts case-insensitive values from {@code table_list} entries parsed by Jackson. */
    @JsonCreator
    public static QueryTableMetadataMergeMode fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
