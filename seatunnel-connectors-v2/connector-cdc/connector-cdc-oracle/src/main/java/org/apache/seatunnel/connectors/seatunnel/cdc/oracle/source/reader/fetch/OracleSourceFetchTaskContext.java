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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleConnectorConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.OracleChangeEventSourceMetricsFactory;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils.createOracleConnection;

/** The context for fetch task that fetching data of snapshot split from Oracle data source. */
@Slf4j
public class OracleSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
    private final OracleConnection connection;
    private final OracleEventMetadataProvider metadataProvider;

    private OracleDatabaseSchema databaseSchema;
    private OracleTaskContext taskContext;
    private OracleOffsetContext offsetContext;
    private SnapshotChangeEventSourceMetrics<OraclePartition> snapshotChangeEventSourceMetrics;
    private OracleStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;

    private Collection<TableChanges.TableChange> engineHistory;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<OraclePartition> dispatcher;
    private OraclePartition oraclePartition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;

    public OracleSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);
        this.connection =
                createOracleConnection(sourceConfig.getDbzConnectorConfig().getJdbcConfig());
        this.metadataProvider = new OracleEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // Initializes the table schema
        registerDatabaseHistory(sourceSplitBase);

        // initial stateful objects
        final OracleConnectorConfig connectorConfig = getDbzConnectorConfig();
        this.topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);

        this.databaseSchema = OracleUtils.createOracleDatabaseSchema(connectorConfig, connection);
        // todo logMiner or xStream
        this.offsetContext =
                loadStartingOffsetState(
                        new LogMinerOracleOffsetContextLoader(connectorConfig), sourceSplitBase);
        this.oraclePartition = new OraclePartition(connectorConfig.getLogicalName());

        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new OracleTaskContext(connectorConfig, databaseSchema);

        // If in the snapshot read phase and enable exactly-once, the queue needs to be set to a
        // maximum size of `Integer.MAX_VALUE` (buffered a current snapshot all data). otherwise,
        // use the configuration queue size.
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        taskContext.configureLoggingContext(
                                                "oracle-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        final OracleChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new OracleChangeEventSourceMetricsFactory(
                        new OracleStreamingChangeEventSourceMetrics(
                                taskContext, queue, metadataProvider, connectorConfig));

        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);
        this.streamingChangeEventSourceMetrics =
                (OracleStreamingChangeEventSourceMetrics)
                        changeEventSourceMetricsFactory.getStreamingMetrics(
                                taskContext, queue, metadataProvider);
        this.errorHandler = new ErrorHandler(OracleConnector.class, connectorConfig, queue);
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            log.warn("Failed to close connection", e);
        }
    }

    @Override
    public OracleSourceConfig getSourceConfig() {
        return (OracleSourceConfig) sourceConfig;
    }

    public OracleConnection getConnection() {
        return connection;
    }

    @Override
    public OracleConnectorConfig getDbzConnectorConfig() {
        return (OracleConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public OracleOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public OraclePartition getPartition() {
        return oraclePartition;
    }

    public SnapshotChangeEventSourceMetrics<OraclePartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    public OracleStreamingChangeEventSourceMetrics getStreamingChangeEventSourceMetrics() {
        return streamingChangeEventSourceMetrics;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public OracleDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return OracleUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher<OraclePartition> getDispatcher() {
        return dispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return OracleUtils.getRedoLogPosition(sourceRecord);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private OracleOffsetContext loadStartingOffsetState(
            OffsetContext.Loader loader, SourceSplitBase oracleSplit) {
        Offset offset =
                oracleSplit.isSnapshotSplit()
                        ? RedoLogOffset.INITIAL_OFFSET
                        : oracleSplit.asIncrementalSplit().getStartupOffset();

        OracleOffsetContext oracleOffsetContext =
                (OracleOffsetContext) loader.load(offset.getOffset());

        return oracleOffsetContext;
    }

    private void registerDatabaseHistory(SourceSplitBase sourceSplitBase) {
        List<TableChanges.TableChange> engineHistory = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            engineHistory.add(
                    dataSourceDialect.queryTableSchema(connection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            for (TableId tableId : incrementalSplit.getTableIds()) {
                engineHistory.add(dataSourceDialect.queryTableSchema(connection, tableId));
            }
        }

        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
    }

    private void validateAndLoadDatabaseHistory(
            OracleOffsetContext offset, OracleDatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(Offsets.of(oraclePartition, offset));
    }

    /** Copied from debezium for accessing here. */
    public static class OracleEventMetadataProvider implements EventMetadataProvider {
        @Override
        public Instant getEventTimestamp(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
            return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
        }

        @Override
        public Map<String, String> getEventSourcePosition(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            final String scn = sourceInfo.getString(SourceInfo.SCN_KEY);
            return Collect.hashMapOf(SourceInfo.SCN_KEY, scn == null ? "null" : scn);
        }

        @Override
        public String getTransactionId(
                DataCollectionId source, OffsetContext offset, Object key, Struct value) {
            if (value == null) {
                return null;
            }
            final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
            if (source == null) {
                return null;
            }
            return sourceInfo.getString(SourceInfo.TXID_KEY);
        }
    }
}
