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
package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;

import java.util.Locale;

/**
 * Validates the insert-only SQL Server bulk-copy contract before writer creation.
 *
 * <p>Other JDBC configurations retain their existing validation and execution paths.
 */
public final class JdbcSinkConfigChecker {
    private JdbcSinkConfigChecker() {}

    /**
     * Rejects incompatible options before the writer allocates a connection or buffers rows.
     *
     * <p>Bulk copy cannot execute custom SQL, updates, deletes, or XA transactions.
     */
    public static void check(ReadonlyConfig config) {
        if (!config.get(JdbcSinkOptions.USE_SQLSERVER_BULK_COPY)) {
            return;
        }
        if (!config.get(JdbcSinkOptions.URL)
                .toLowerCase(Locale.ROOT)
                .startsWith("jdbc:sqlserver:")) {
            throw new OptionValidationException(
                    "use_sqlserver_bulk_copy requires a SQL Server JDBC URL.");
        }
        if (config.get(JdbcSinkOptions.IS_EXACTLY_ONCE)) {
            throw new OptionValidationException(
                    "use_sqlserver_bulk_copy does not support is_exactly_once=true.");
        }
        if (config.get(JdbcSinkOptions.ENABLE_UPSERT)) {
            throw new OptionValidationException(
                    "use_sqlserver_bulk_copy requires enable_upsert=false.");
        }
        if (config.get(JdbcSinkOptions.USE_COPY_STATEMENT)) {
            throw new OptionValidationException(
                    "use_sqlserver_bulk_copy cannot be combined with use_copy_statement.");
        }
        if (!config.get(JdbcSinkOptions.GENERATE_SINK_SQL)
                || config.getOptional(JdbcSinkOptions.QUERY)
                        .filter(query -> !query.trim().isEmpty())
                        .isPresent()) {
            throw new OptionValidationException(
                    "use_sqlserver_bulk_copy requires generated insert mode without a custom query.");
        }
    }
}
