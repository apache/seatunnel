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

package org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/**
 * Base JDBC source config factory for PostgreSQL-compatible CDC connectors.
 *
 * <p>This factory centralizes the common Debezium property assembly shared by PG-base connectors
 * while leaving connector-specific identifiers and extra properties to subclasses.
 */
public abstract class PgBaseSourceConfigFactory<C extends JdbcSourceConfig>
        extends JdbcSourceConfigFactory {

    // Pinned because this class sits in the serialization hierarchy of the concrete factories,
    // which are shipped inside the job DAG. A computed UID would drift on every edit here and
    // break rolling upgrades (jobs submitted on the prior version fail to deserialize).
    private static final long serialVersionUID = 1L;

    @Override
    public C create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", connectorClassName());
        props.setProperty("database.server.name", databaseServerName());
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        // Validate before indexing: databaseList.get(0) would otherwise throw a bare NPE or
        // IndexOutOfBoundsException that gives no hint the 'database-names' option is missing.
        checkNotNull(databaseList, "The 'database-names' option is required.");
        checkArgument(!databaseList.isEmpty(), "The 'database-names' option must not be empty.");
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

        // Keep the current in-memory history wiring unchanged to avoid restore drift in phase 1.
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        props.setProperty("include.schema.changes", String.valueOf(false));

        configureConnectorProperties(props, subtask);

        if (tableList != null) {
            props.setProperty(
                    "table.include.list",
                    tableList.stream()
                            .map(this::formatTableIdentifier)
                            .collect(Collectors.joining(",")));
        }

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        if (startupConfig != null && startupConfig.getStartupMode() == StartupMode.SNAPSHOT_ONLY) {
            props.setProperty("snapshot.mode", "initial_only");
        } else if (startupConfig != null
                && startupConfig.getStartupMode() == StartupMode.COMMITTED_OFFSET) {
            props.setProperty("snapshot.mode", "never");
        }

        C config = createSourceConfig(props, driverClassName());
        // Keep the concurrent-read flag wired after moving concrete connectors onto PG-base.
        config.setEnableConcurrentRead(this.enableConcurrentRead);
        return config;
    }

    /** Returns the Debezium connector class name used by the concrete PG-base connector. */
    protected abstract String connectorClassName();

    /** Returns the logical server name used as Debezium topic namespace. */
    protected abstract String databaseServerName();

    /** Returns the JDBC driver class used by the concrete PG-base connector. */
    protected abstract String driverClassName();

    /**
     * Allows subclasses to inject connector-specific Debezium properties such as slot or plugin
     * settings before user-supplied Debezium overrides are merged.
     */
    protected void configureConnectorProperties(Properties props, int subtask) {}

    /**
     * Normalizes table identifiers to the schema.table form expected by PostgreSQL-compatible
     * Debezium connectors.
     *
     * <p>The rejection message keeps the pre-refactor wording verbatim: both Postgres-CDC and
     * Opengauss-CDC already surfaced it to users, and the internal module name would mean nothing
     * to someone reading the failure.
     */
    protected String formatTableIdentifier(String tableIdentifier) {
        String[] splits = tableIdentifier.split("\\.");
        if (splits.length == 2) {
            return tableIdentifier;
        }
        if (splits.length == 3) {
            return String.join(".", splits[1], splits[2]);
        }
        throw new IllegalArgumentException(
                "Invalid table name: "
                        + tableIdentifier
                        + " ,Postgres identifier is of the form schemaName.tableName");
    }

    /** Creates the concrete JDBC source config after the common Debezium properties are built. */
    protected abstract C createSourceConfig(Properties props, String driverClassName);
}
