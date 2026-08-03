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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessTableSchemaState;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.vitess.Filters;
import io.debezium.connector.vitess.VitessChangeEventSourceFactory;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.VitessErrorHandler;
import io.debezium.connector.vitess.VitessEventMetadataProvider;
import io.debezium.connector.vitess.VitessOffsetContext;
import io.debezium.connector.vitess.VitessPartition;
import io.debezium.connector.vitess.VitessTaskContext;
import io.debezium.connector.vitess.VitessTopicSelector;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.SchemaNameAdjuster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Source reader backed by Debezium Vitess streaming.
 *
 * <p>The reader owns one Debezium streaming runtime and persists the latest source offset directly
 * in the SeaTunnel split so checkpoint / restore can resume from the last emitted Vitess position.
 */
public class VitessSourceReader implements SourceReader<SeaTunnelRow, VitessSourceSplit> {

    /** Reader context is also used to request the single split. */
    private final Context context;

    /** Immutable connector configuration shared by runtime and checkpoint code. */
    private final VitessSourceConfig sourceConfig;

    /**
     * Debezium deserializer is owned by the source config so startup semantics stay centralized.
     */
    private final DebeziumDeserializationSchema<SeaTunnelRow> deserializer;

    /** Reader state is tiny but still guarded because snapshotState can race with pollNext. */
    private final Object stateLock = new Object();

    private final List<VitessSourceSplit> sourceSplits = new ArrayList<>(1);

    private VitessStreamingRuntime runtime;

    public VitessSourceReader(Context context, VitessSourceConfig sourceConfig) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.deserializer = sourceConfig.createDeserializer();
    }

    @Override
    public void open() {
        context.sendSplitRequest();
    }

    /**
     * Polls Debezium records and updates the split offset inside the same critical section that
     * emits SeaTunnel rows.
     *
     * <p>This keeps checkpoint state aligned with the last fully emitted source record.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        VitessStreamingRuntime currentRuntime = ensureRuntime();
        if (currentRuntime == null) {
            return;
        }

        List<SourceRecord> records = currentRuntime.poll();
        if (records.isEmpty()) {
            return;
        }

        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                VitessSourceSplit split = sourceSplits.get(0);
                for (SourceRecord record : records) {
                    deserializer.deserialize(record, output);
                    split.setOffset(record.sourceOffset());
                }
            }
        }
    }

    @Override
    public List<VitessSourceSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            if (runtime != null && !sourceSplits.isEmpty()) {
                Map<String, byte[]> runtimeTableSchemas = runtime.snapshotTableSchemas();
                if (runtimeTableSchemas != null && !runtimeTableSchemas.isEmpty()) {
                    sourceSplits.get(0).setTableSchemas(runtimeTableSchemas);
                }
            }
            return sourceSplits.stream().map(VitessSourceSplit::copy).collect(Collectors.toList());
        }
    }

    @Override
    public void addSplits(List<VitessSourceSplit> splits) {
        synchronized (stateLock) {
            if (!sourceSplits.isEmpty() && splits != null && !splits.isEmpty()) {
                throw new IllegalStateException(
                        "Vitess CDC reader only supports one active streaming split.");
            }
            if (splits != null) {
                for (VitessSourceSplit split : splits) {
                    sourceSplits.add(split.copy());
                }
            }
            if (runtime == null && !sourceSplits.isEmpty()) {
                // Start Debezium as soon as the split arrives so latest startup does not miss
                // changes produced before the framework issues the first pollNext call.
                runtime = new VitessStreamingRuntime(sourceConfig, sourceSplits.get(0));
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void close() throws IOException {
        VitessStreamingRuntime currentRuntime;
        synchronized (stateLock) {
            currentRuntime = runtime;
            runtime = null;
        }
        if (currentRuntime != null) {
            currentRuntime.close();
        }
    }

    private VitessStreamingRuntime ensureRuntime() {
        synchronized (stateLock) {
            if (sourceSplits.isEmpty()) {
                return null;
            }
            if (runtime == null) {
                runtime = new VitessStreamingRuntime(sourceConfig, sourceSplits.get(0));
            }
            return runtime;
        }
    }

    /** Small Debezium runtime wrapper so the SeaTunnel reader can stay checkpoint-focused. */
    static final class VitessStreamingRuntime implements AutoCloseable {

        private final ChangeEventQueue<DataChangeEvent> queue;
        private final VitessDatabaseSchema schema;
        private final EventDispatcher<VitessPartition, TableId> dispatcher;
        private final VitessReplicationConnection replicationConnection;
        private final ChangeEventSourceCoordinator<VitessPartition, VitessOffsetContext>
                coordinator;

        /**
         * Creates and starts the Debezium runtime for one SeaTunnel split using the same
         * coordinator lifecycle as Debezium's official Vitess connector task.
         */
        VitessStreamingRuntime(VitessSourceConfig sourceConfig, VitessSourceSplit split) {
            Properties properties = sourceConfig.toDebeziumProperties();
            Configuration configuration = Configuration.from(properties);
            VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
            TopicSelector<TableId> topicSelector =
                    VitessTopicSelector.defaultSelector(connectorConfig);
            SchemaNameAdjuster schemaNameAdjuster =
                    connectorConfig.schemaNameAdjustmentMode().createAdjuster();
            this.schema =
                    new VitessDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector);
            restoreTableSchemas(split.getTableSchemas());
            this.queue =
                    new ChangeEventQueue.Builder<DataChangeEvent>()
                            .pollInterval(connectorConfig.getPollInterval())
                            .maxBatchSize(connectorConfig.getMaxBatchSize())
                            .maxQueueSize(connectorConfig.getMaxQueueSize())
                            .loggingContextSupplier(
                                    () ->
                                            LoggingContext.forConnector(
                                                    connectorConfig.getConnectorName(),
                                                    connectorConfig.getLogicalName(),
                                                    "seatunnel-vitess-cdc-reader"))
                            .build();

            VitessErrorHandler errorHandler = new VitessErrorHandler(connectorConfig, queue);
            this.dispatcher =
                    new EventDispatcher<>(
                            connectorConfig,
                            topicSelector,
                            schema,
                            queue,
                            createTableFilter(connectorConfig),
                            DataChangeEvent::new,
                            new VitessEventMetadataProvider(),
                            schemaNameAdjuster);
            this.replicationConnection = new VitessReplicationConnection(connectorConfig, schema);
            VitessTaskContext taskContext = new VitessTaskContext(connectorConfig, schema);
            VitessPartition partition = new VitessPartition(connectorConfig.getLogicalName());
            VitessOffsetContext previousOffset =
                    split.getOffset() == null
                            ? null
                            : new VitessOffsetContext.Loader(connectorConfig)
                                    .load(split.getOffset());
            this.coordinator =
                    new ChangeEventSourceCoordinator<>(
                            Offsets.of(partition, previousOffset),
                            errorHandler,
                            io.debezium.connector.vitess.VitessConnector.class,
                            connectorConfig,
                            new VitessChangeEventSourceFactory(
                                    connectorConfig,
                                    errorHandler,
                                    dispatcher,
                                    Clock.system(),
                                    schema,
                                    replicationConnection),
                            new DefaultChangeEventSourceMetricsFactory<>(),
                            dispatcher,
                            schema);
            this.coordinator.start(taskContext, queue, new VitessEventMetadataProvider());
        }

        /**
         * Debezium already batches SourceRecords for us, so the reader only converts queue items.
         */
        List<SourceRecord> poll() throws InterruptedException {
            return queue.poll().stream()
                    .map(DataChangeEvent::getRecord)
                    .collect(Collectors.toList());
        }

        /**
         * Captures the current Debezium table definitions so restore can resume from backlog rows
         * without waiting for VTGate to resend FIELD metadata.
         */
        Map<String, byte[]> snapshotTableSchemas() {
            Map<String, byte[]> tableSchemas = new HashMap<>();
            for (TableId tableId : schema.tableIds()) {
                Table table = schema.tableFor(tableId);
                if (table != null) {
                    tableSchemas.put(
                            tableId.toDoubleQuotedString(),
                            VitessTableSchemaState.serialize(table));
                }
            }
            return tableSchemas.isEmpty() ? null : tableSchemas;
        }

        @Override
        public void close() throws IOException {
            IOException closeException = null;

            try {
                coordinator.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                IOException interruptedException =
                        new IOException(
                                "Interrupted while stopping Vitess change-event coordinator", e);
                if (closeException == null) {
                    closeException = interruptedException;
                } else {
                    closeException.addSuppressed(interruptedException);
                }
            }

            try {
                replicationConnection.close();
            } catch (Exception e) {
                IOException connectionException =
                        new IOException("Failed to close Vitess replication connection", e);
                if (closeException == null) {
                    closeException = connectionException;
                } else {
                    closeException.addSuppressed(connectionException);
                }
            }

            try {
                schema.close();
            } catch (Exception e) {
                IOException schemaException =
                        new IOException("Failed to close Vitess database schema", e);
                if (closeException == null) {
                    closeException = schemaException;
                } else {
                    closeException.addSuppressed(schemaException);
                }
            }

            if (closeException != null) {
                throw closeException;
            }
        }

        /**
         * Debezium 1.9's Vitess connector keeps the table filter in its dedicated Filters helper
         * instead of wiring RelationalDatabaseConnectorConfig#getTableFilters().
         */
        private static Tables.TableFilter createTableFilter(VitessConnectorConfig connectorConfig) {
            return new AccessibleVitessFilters(connectorConfig).exposeTableFilter();
        }

        private void restoreTableSchemas(Map<String, byte[]> tableSchemas) {
            if (tableSchemas == null || tableSchemas.isEmpty()) {
                return;
            }
            tableSchemas.values().forEach(this::restoreTableSchema);
        }

        private void restoreTableSchema(byte[] tableSchemaBytes) {
            Table restoredTable = VitessTableSchemaState.deserialize(tableSchemaBytes);
            if (restoredTable != null) {
                schema.applySchemaChangesForTable(restoredTable);
            }
        }
    }

    /** Small bridge that exposes Debezium's protected Vitess table filter. */
    static final class AccessibleVitessFilters extends Filters {

        AccessibleVitessFilters(VitessConnectorConfig connectorConfig) {
            super(connectorConfig);
        }

        /**
         * Returns the Debezium-owned table filter so SeaTunnel stays aligned with upstream logic.
         */
        private Tables.TableFilter exposeTableFilter() {
            return super.tableFilter();
        }
    }
}
