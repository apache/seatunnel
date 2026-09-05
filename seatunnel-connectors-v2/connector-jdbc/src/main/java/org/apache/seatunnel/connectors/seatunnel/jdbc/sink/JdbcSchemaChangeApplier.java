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

import org.apache.seatunnel.api.sink.SchemaChangeApplier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import java.io.IOException;
import java.sql.Connection;

/** Applies JDBC schema changes without mutating any sink writer-local state. */
public class JdbcSchemaChangeApplier implements SchemaChangeApplier {

    private final JdbcDialect dialect;
    private final JdbcSinkConfig jdbcSinkConfig;
    private final TablePath sinkTablePath;

    public JdbcSchemaChangeApplier(
            JdbcDialect dialect, JdbcSinkConfig jdbcSinkConfig, TablePath sinkTablePath) {
        this.dialect = dialect;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.sinkTablePath = sinkTablePath;
    }

    @Override
    public void apply(SchemaChangeEvent event) throws IOException {
        JdbcConnectionProvider connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        try (Connection connection = connectionProvider.getOrEstablishConnection()) {
            dialect.applySchemaChange(connection, sinkTablePath, event);
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.REFRESH_PHYSICAL_TABLESCHEMA_BY_SCHEMA_CHANGE_EVENT, e);
        }
    }
}
