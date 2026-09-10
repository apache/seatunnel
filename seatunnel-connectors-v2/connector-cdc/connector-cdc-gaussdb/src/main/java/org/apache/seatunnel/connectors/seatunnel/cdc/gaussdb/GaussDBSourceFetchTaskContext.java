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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.history.TableChanges;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

/** PostgreSQL snapshot context extended with the GaussDB mppdb replication stream. */
final class GaussDBSourceFetchTaskContext extends PostgresSourceFetchTaskContext {

    /** Checkpoint-aware mppdb stream sharing the task's regular data connection. */
    private final MppdbReplicationStream mppdbStream;

    /** Creates a task context for one GaussDB source split. */
    GaussDBSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            PostgresConnection dataConnection,
            Collection<TableChanges.TableChange> engineHistory,
            List<CatalogTable> relationSchemaBaseline,
            GaussDBMppdbConfig mppdbConfig) {
        super(
                sourceConfig,
                dataSourceDialect,
                dataConnection,
                engineHistory,
                relationSchemaBaseline);
        this.mppdbStream =
                new MppdbReplicationStream(
                        dataConnection.connection(),
                        mppdbConfig,
                        sourceConfig.getUsername(),
                        sourceConfig.getPassword(),
                        sourceConfig.getFetchSize());
    }

    /**
     * Debezium does not recognize mppdb_decoding, so its snapshotter receives no plugin-specific
     * slot state. The GaussDB stream verifies the actual slot independently.
     */
    @Override
    protected SlotState getReplicationSlotState(PostgresConnectorConfig connectorConfig) {
        return null;
    }

    /** Creates the mppdb slot before snapshot rows are read, preventing a snapshot/WAL gap. */
    @Override
    protected void prepareReplicationConnection(
            PostgresConnectorConfig connectorConfig, SlotState slotInfo) {
        if (!getSnapshotter().shouldStream()) {
            return;
        }
        try {
            mppdbStream.ensureSlot();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to prepare GaussDB mppdb_decoding replication slot", e);
        }
    }

    /** Returns the mppdb stream consumed by the incremental fetch task. */
    MppdbReplicationStream getMppdbStream() {
        return mppdbStream;
    }

    /** Returns the connection-scoped type registry used to convert mppdb text values. */
    TypeRegistry getTypeRegistry() {
        return getDataConnection().getTypeRegistry();
    }

    /** Closes the dedicated replication stream before the shared PostgreSQL context. */
    @Override
    public void close() {
        mppdbStream.close();
        super.close();
    }
}
