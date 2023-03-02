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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;
import org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffset;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengUtils;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.connector.dameng.DamengErrorHandler;
import io.debezium.connector.dameng.DamengEventMetadataProvider;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.connector.dameng.DamengTaskContext;
import io.debezium.connector.dameng.DamengTopicSelector;
import io.debezium.connector.dameng.DamengValueConverters;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import lombok.Getter;

import java.util.Collection;

public class DamengSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
    @Getter private final DamengConnection connection;
    private final Collection<TableChanges.TableChange> engineHistory;
    private final DamengEventMetadataProvider metadataProvider;
    private TopicSelector<TableId> topicSelector;
    private DamengDatabaseSchema databaseSchema;
    private DamengOffsetContext offsetContext;
    private DamengTaskContext taskContext;
    private ChangeEventQueue<DataChangeEvent> queue;
    private JdbcSourceEventDispatcher dispatcher;
    @Getter private SnapshotChangeEventSourceMetrics snapshotChangeEventSourceMetrics;
    private DamengErrorHandler errorHandler;

    public DamengSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            DamengConnection connection,
            Collection<TableChanges.TableChange> engineHistory) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.engineHistory = engineHistory;
        this.metadataProvider = new DamengEventMetadataProvider();
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
        DamengConnectorConfig connectorConfig = getDbzConnectorConfig();

        this.topicSelector = DamengTopicSelector.defaultSelector(connectorConfig);
        this.databaseSchema =
                new DamengDatabaseSchema(
                        connectorConfig,
                        new DamengValueConverters(),
                        topicSelector,
                        schemaNameAdjuster,
                        false);
        this.offsetContext =
                loadStartingOffsetState(
                        new DamengOffsetContext.Loader(connectorConfig), sourceSplitBase);
        validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

        this.taskContext = new DamengTaskContext(connectorConfig, databaseSchema);

        int queueSize =
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
                                () ->
                                        taskContext.configureLoggingContext(
                                                "dameng-cdc-connector-task"))
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

        DefaultChangeEventSourceMetricsFactory changeEventSourceMetricsFactory =
                new DefaultChangeEventSourceMetricsFactory();
        this.snapshotChangeEventSourceMetrics =
                changeEventSourceMetricsFactory.getSnapshotMetrics(
                        taskContext, queue, metadataProvider);

        this.errorHandler = new DamengErrorHandler(connectorConfig.getLogicalName(), queue);
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
    public LogMinerOffset getStreamOffset(SourceRecord record) {
        return DamengUtils.createLogMinerOffset(record.sourceOffset());
    }

    @Override
    public DamengDatabaseSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return DamengUtils.getSplitType(table);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public JdbcSourceEventDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public DamengOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public DamengSourceConfig getSourceConfig() {
        return (DamengSourceConfig) sourceConfig;
    }

    @Override
    public DamengConnectorConfig getDbzConnectorConfig() {
        return (DamengConnectorConfig) super.getDbzConnectorConfig();
    }

    private DamengOffsetContext loadStartingOffsetState(
            DamengOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LogMinerOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        return loader.load(offset.getOffset());
    }

    private void validateAndLoadDatabaseHistory(
            DamengOffsetContext offsetContext, DamengDatabaseSchema databaseSchema) {
        databaseSchema.initializeStorage();
        databaseSchema.recover(offsetContext);
    }
}
