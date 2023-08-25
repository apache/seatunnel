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

package org.apache.searunnel.connectors.cdc.db2.source.reader.fetch;

import io.debezium.connector.db2.Db2Connection;
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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfig;
import org.apache.searunnel.connectors.cdc.db2.source.offset.LsnOffset;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2ConnectionUtils;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2Utils;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
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
import java.util.List;
import java.util.Map;

import static org.apache.searunnel.connectors.cdc.db2.utils.Db2ConnectionUtils.createDb2Connection;

/** The context for fetch task that fetching data of snapshot split from MySQL data source. */
@Slf4j
public class Db2SourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private final Db2Connection dataConnection;

    private final Db2Connection metadataConnection;

    private final Db2EventMetadataProvider metadataProvider;
    private Db2DatabaseSchema databaseSchema;
    private Db2OffsetContext offsetContext;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher dispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;
    private Db2TaskContext taskContext;

    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;

    public Db2SourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        super(sourceConfig, dataSourceDialect);

        this.dataConnection =
                Db2ConnectionUtils.createDb2Connection(sourceConfig.getDbzConfiguration());
        this.metadataConnection = createDb2Connection(sourceConfig.getDbzConfiguration());
        this.metadataProvider = new Db2EventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        registerDatabaseHistory(sourceSplitBase);

        // initial stateful objects
        final Db2ConnectorConfig connectorConfig = getDbzConnectorConfig();

        final Db2ValueConverters valueConverters =
                new Db2ValueConverters(
                        connectorConfig.getDecimalMode(),
                        connectorConfig.getTemporalPrecisionMode());

        this.topicSelector = Db2TopicSelector.defaultSelector(connectorConfig);

        this.databaseSchema =
                new Db2DatabaseSchema(
                        connectorConfig,
                        schemaNameAdjuster,
                        topicSelector,
                        Db2ConnectionUtils.createDb2Connection((Configuration) connectorConfig));

        this.offsetContext =
                loadStartingOffsetState(
                        new Db2OffsetContext.Loader(connectorConfig), sourceSplitBase);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new Db2TaskContext(connectorConfig, databaseSchema);

        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () -> taskContext.configureLoggingContext("db2-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
        this.dispatcher =
                new JdbcSourceEventDispatcher(
                        connectorConfig,
                        topicSelector,
                        databaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster);

        final DefaultChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new DefaultChangeEventSourceMetricsFactory();

        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);

        this.errorHandler =
                new ErrorHandler(Db2Connector.class, connectorConfig.getLogicalName(), queue);
    }

    @Override
    public void close() {
        try {
            this.dataConnection.close();
            this.metadataConnection.close();
        } catch (SQLException e) {
            log.warn("Failed to close connection", e);
        }
    }

    @Override
    public Db2SourceConfig getSourceConfig() {
        return (Db2SourceConfig) sourceConfig;
    }

    public Db2Connection getDataConnection() {
        return dataConnection;
    }

    public Db2Connection getMetadataConnection() {
        return metadataConnection;
    }

    public SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    @Override
    public Db2ConnectorConfig getDbzConnectorConfig() {
        return (Db2ConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public Db2OffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public Db2DatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return Db2Utils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher getDispatcher() {
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
        return Db2Utils.getLsn(sourceRecord);
    }

    private void validateAndLoadDatabaseHistory(Db2OffsetContext offset, Db2DatabaseSchema schema) {
        schema.initializeStorage();
        schema.recover(offset);
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private Db2OffsetContext loadStartingOffsetState(
            Db2OffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();

        Db2OffsetContext db2OffsetContext = loader.load(offset.getOffset());

        return db2OffsetContext;
    }

    private void registerDatabaseHistory(SourceSplitBase sourceSplitBase) {
        List<TableChanges.TableChange> engineHistory = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            engineHistory.add(
                    dataSourceDialect.queryTableSchema(dataConnection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            for (TableId tableId : incrementalSplit.getTableIds()) {
                engineHistory.add(dataSourceDialect.queryTableSchema(dataConnection, tableId));
            }
        }

        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
    }

    public static class Db2EventMetadataProvider implements EventMetadataProvider {

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
            return Collect.hashMapOf(
                    SourceInfo.COMMIT_LSN_KEY, sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY),
                    SourceInfo.CHANGE_LSN_KEY, sourceInfo.getString(SourceInfo.CHANGE_LSN_KEY));
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
            return sourceInfo.getString(SourceInfo.COMMIT_LSN_KEY);
        }
    }
}
