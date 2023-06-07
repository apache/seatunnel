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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresUtils;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventMetadataProvider;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.PostgresValueConverter;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Collection;

@Slf4j
public class PostgresSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final String CONTEXT_NAME = "postgres-cdc-connector-task";

    private final PostgresConnection dataConnection;

    @Getter private ReplicationConnection replicationConnection;

    private final PostgresEventMetadataProvider metadataProvider;

    @Getter private Snapshotter snapshotter;
    private PostgresSchema databaseSchema;
    private PostgresOffsetContext offsetContext;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher dispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresErrorHandler errorHandler;

    @Getter private PostgresTaskContext taskContext;

    private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;

    //    private PostgresStreamingChangeEventSourceMetrics streamingChangeEventSourceMetrics;

    private Collection<TableChanges.TableChange> engineHistory;

    public PostgresSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            PostgresConnection dataConnection,
            Collection<TableChanges.TableChange> engineHistory) {
        super(sourceConfig, dataSourceDialect);
        this.dataConnection = dataConnection;
        this.metadataProvider = new PostgresEventMetadataProvider();
        this.engineHistory = engineHistory;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        // initial stateful objects
        final PostgresConnectorConfig connectorConfig = getDbzConnectorConfig();

        final Clock clock = Clock.system();
        this.topicSelector = PostgresTopicSelector.create(connectorConfig);

        final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder =
                typeRegistry ->
                        PostgresValueConverter.of(
                                connectorConfig, dataConnection.getDatabaseCharset(), typeRegistry);
        final TypeRegistry typeRegistry = dataConnection.getTypeRegistry();

        this.databaseSchema =
                new PostgresSchema(
                        connectorConfig,
                        typeRegistry,
                        topicSelector,
                        valueConverterBuilder.build(typeRegistry));
        try {
            this.databaseSchema.refresh(dataConnection, true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.taskContext = new PostgresTaskContext(connectorConfig, databaseSchema, topicSelector);

        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(connectorConfig), sourceSplitBase);

        final int queueSize =
                sourceSplitBase.isSnapshotSplit()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();

        LoggingContext.PreviousContext previousContext =
                taskContext.configureLoggingContext(CONTEXT_NAME);

        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () -> taskContext.configureLoggingContext(CONTEXT_NAME))
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

        this.snapshotChangeEventSourceMetrics =
                new DefaultChangeEventSourceMetricsFactory()
                        .getSnapshotMetrics(taskContext, queue, metadataProvider);

        this.errorHandler = new PostgresErrorHandler(connectorConfig.getLogicalName(), queue);
    }

    @Override
    public PostgresSourceConfig getSourceConfig() {
        return (PostgresSourceConfig) sourceConfig;
    }

    public PostgresConnection getDataConnection() {
        return dataConnection;
    }

    public SnapshotChangeEventSourceMetrics getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return (PostgresConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public PostgresSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return PostgresUtils.getSplitType(table);
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
        return PostgresUtils.getLsnPosition(sourceRecord);
    }

    @Override
    public void close() {}

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        return loader.load(offset.getOffset());
    }
}
