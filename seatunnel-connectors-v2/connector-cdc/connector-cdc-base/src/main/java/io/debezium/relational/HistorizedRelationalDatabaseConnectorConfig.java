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

package io.debezium.relational;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.DatabaseHistoryMetrics;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.KafkaDatabaseHistory;

/**
 * Copied from Debezium project. Configuration options shared across the relational CDC connectors
 * which use a persistent database schema history.
 *
 * <p>Added JMX_METRICS_ENABLED option.
 *
 * <p>Line 147: set classloader to load the EmbeddedDatabaseHistory in seatunnel
 */
public abstract class HistorizedRelationalDatabaseConnectorConfig
        extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    private boolean useCatalogBeforeSchema;
    private final String logicalName;
    private final Class<? extends SourceConnector> connectorClass;
    private final boolean multiPartitionMode;

    /**
     * The database history class is hidden in the {@link #configDef()} since that is designed to
     * work with a user interface, and in these situations using Kafka is the only way to go.
     */
    public static final Field DATABASE_HISTORY =
            Field.create("database.history")
                    .withDisplayName("Database history class")
                    .withType(Type.CLASS)
                    .withWidth(Width.LONG)
                    .withImportance(Importance.LOW)
                    .withInvisibleRecommender()
                    .withDescription(
                            "The name of the DatabaseHistory class that should be used to store and recover database schema changes. "
                                    + "The configuration properties for the history are prefixed with the '"
                                    + DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING
                                    + "' string.")
                    .withDefault(KafkaDatabaseHistory.class.getName());

    public static final Field JMX_METRICS_ENABLED =
            Field.create(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING + "metrics.enabled")
                    .withDisplayName("Skip DDL statements that cannot be parsed")
                    .withType(Type.BOOLEAN)
                    .withImportance(Importance.LOW)
                    .withDescription("Whether to enable JMX history metrics")
                    .withDefault(false);

    protected static final ConfigDefinition CONFIG_DEFINITION =
            RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
                    .edit()
                    .history(
                            DATABASE_HISTORY,
                            DatabaseHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS,
                            DatabaseHistory.STORE_ONLY_MONITORED_TABLES_DDL,
                            DatabaseHistory.STORE_ONLY_CAPTURED_TABLES_DDL,
                            KafkaDatabaseHistory.BOOTSTRAP_SERVERS,
                            KafkaDatabaseHistory.TOPIC,
                            KafkaDatabaseHistory.RECOVERY_POLL_ATTEMPTS,
                            KafkaDatabaseHistory.RECOVERY_POLL_INTERVAL_MS,
                            KafkaDatabaseHistory.KAFKA_QUERY_TIMEOUT_MS)
                    .create();

    protected HistorizedRelationalDatabaseConnectorConfig(
            Class<? extends SourceConnector> connectorClass,
            Configuration config,
            String logicalName,
            TableFilter systemTablesFilter,
            boolean useCatalogBeforeSchema,
            int defaultSnapshotFetchSize,
            ColumnFilterMode columnFilterMode,
            boolean multiPartitionMode) {
        super(
                config,
                logicalName,
                systemTablesFilter,
                TableId::toString,
                defaultSnapshotFetchSize,
                columnFilterMode);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    protected HistorizedRelationalDatabaseConnectorConfig(
            Class<? extends SourceConnector> connectorClass,
            Configuration config,
            String logicalName,
            TableFilter systemTablesFilter,
            TableIdToStringMapper tableIdMapper,
            boolean useCatalogBeforeSchema,
            ColumnFilterMode columnFilterMode,
            boolean multiPartitionMode) {
        super(
                config,
                logicalName,
                systemTablesFilter,
                tableIdMapper,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                columnFilterMode);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    /** Returns a configured (but not yet started) instance of the database history. */
    public DatabaseHistory getDatabaseHistory() {
        Configuration config = getConfig();
        DatabaseHistory databaseHistory =
                config.getInstance(
                        HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY,
                        DatabaseHistory.class,
                        () -> HistorizedRelationalDatabaseConnectorConfig.class.getClassLoader());
        if (databaseHistory == null) {
            throw new ConnectException(
                    "Unable to instantiate the database history class "
                            + config.getString(
                                    HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig =
                config.subset(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                        .edit()
                        .withDefault(DatabaseHistory.NAME, getLogicalName() + "-dbhistory")
                        .withDefault(
                                KafkaDatabaseHistory.INTERNAL_CONNECTOR_CLASS,
                                connectorClass.getName())
                        .withDefault(KafkaDatabaseHistory.INTERNAL_CONNECTOR_ID, logicalName)
                        .build();

        DatabaseHistoryListener listener =
                config.getBoolean(JMX_METRICS_ENABLED)
                        ? new DatabaseHistoryMetrics(this, multiPartitionMode)
                        : DatabaseHistoryListener.NOOP;

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        databaseHistory.configure(
                dbHistoryConfig, historyComparator, listener, useCatalogBeforeSchema); // validates

        return databaseHistory;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure
     * no history entries newer than the offset we resume from are recovered (which could happen
     * when restarting a connector after history records have been persisted but no new offset has
     * been committed yet).
     */
    protected abstract HistoryRecordComparator getHistoryRecordComparator();
}
