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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.HybridSplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.SplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.format.compatible.debezium.json.CompatibleDebeziumJsonDeserializationSchema;

import com.google.common.collect.Sets;
import io.debezium.relational.TableId;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor
public abstract class IncrementalSource<T, C extends SourceConfig>
        implements SeaTunnelSource<T, SourceSplitBase, PendingSplitsState>, SupportCoordinate {

    protected ReadonlyConfig readonlyConfig;
    protected SourceConfig.Factory<C> configFactory;
    protected OffsetFactory offsetFactory;

    protected DataSourceDialect<C> dataSourceDialect;
    protected StartupConfig startupConfig;

    protected int incrementalParallelism;
    protected StopConfig stopConfig;
    protected List<CatalogTable> catalogTables;

    protected StopMode stopMode;
    protected DebeziumDeserializationSchema<T> deserializationSchema;

    protected SeaTunnelDataType<SeaTunnelRow> dataType;

    protected IncrementalSource(
            ReadonlyConfig options,
            SeaTunnelDataType<SeaTunnelRow> dataType,
            List<CatalogTable> catalogTables) {
        this.dataType = dataType;
        this.catalogTables = catalogTables;
        this.readonlyConfig = options;
        this.startupConfig = getStartupConfig(readonlyConfig);
        this.stopConfig = getStopConfig(readonlyConfig);
        this.stopMode = stopConfig.getStopMode();
        this.incrementalParallelism = readonlyConfig.get(SourceOptions.INCREMENTAL_PARALLELISM);
        this.configFactory = createSourceConfigFactory(readonlyConfig);
        this.dataSourceDialect = createDataSourceDialect(readonlyConfig);
        this.deserializationSchema = createDebeziumDeserializationSchema(readonlyConfig);
        this.offsetFactory = createOffsetFactory(readonlyConfig);
    }

    protected StartupConfig getStartupConfig(ReadonlyConfig config) {
        return new StartupConfig(
                config.get(getStartupModeOption()),
                config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE),
                config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS),
                config.get(SourceOptions.STARTUP_TIMESTAMP));
    }

    private StopConfig getStopConfig(ReadonlyConfig config) {
        return new StopConfig(
                config.get(getStopModeOption()),
                config.get(SourceOptions.STOP_SPECIFIC_OFFSET_FILE),
                config.get(SourceOptions.STOP_SPECIFIC_OFFSET_POS),
                config.get(SourceOptions.STOP_TIMESTAMP));
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                readonlyConfig.get(JdbcSourceOptions.FORMAT))) {
            return Collections.singletonList(
                    CatalogTableUtil.getCatalogTable(
                            "default.default",
                            CompatibleDebeziumJsonDeserializationSchema.DEBEZIUM_DATA_ROW_TYPE));
        }
        return catalogTables;
    }

    public abstract Option<StartupMode> getStartupModeOption();

    public abstract Option<StopMode> getStopModeOption();

    public abstract SourceConfig.Factory<C> createSourceConfigFactory(ReadonlyConfig config);

    public abstract DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config);

    public abstract DataSourceDialect<C> createDataSourceDialect(ReadonlyConfig config);

    public abstract OffsetFactory createOffsetFactory(ReadonlyConfig config);

    @Override
    public Boundedness getBoundedness() {
        return stopMode == StopMode.NEVER ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @SuppressWarnings("MagicNumber")
    @Override
    public SourceReader<T, SourceSplitBase> createReader(SourceReader.Context readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        C sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
                new LinkedBlockingQueue<>(2);

        SchemaChangeResolver schemaChangeResolver = deserializationSchema.getSchemaChangeResolver();
        Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier =
                () ->
                        new IncrementalSourceSplitReader<>(
                                readerContext.getIndexOfSubtask(),
                                dataSourceDialect,
                                sourceConfig,
                                schemaChangeResolver);
        return new IncrementalSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                createRecordEmitter(sourceConfig, readerContext.getMetricsContext()),
                new SourceReaderOptions(readonlyConfig),
                readerContext,
                sourceConfig,
                deserializationSchema);
    }

    protected RecordEmitter<SourceRecords, T, SourceSplitStateBase> createRecordEmitter(
            SourceConfig sourceConfig, MetricsContext metricsContext) {
        return new IncrementalSourceRecordEmitter<>(
                deserializationSchema, offsetFactory, metricsContext);
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) throws Exception {
        C sourceConfig = configFactory.create(0);
        final List<TableId> remainingTables =
                dataSourceDialect.discoverDataCollections(sourceConfig);
        final SplitAssigner splitAssigner;
        SplitAssigner.Context<C> assignerContext =
                new SplitAssigner.Context<>(
                        sourceConfig,
                        new HashSet<>(remainingTables),
                        new HashMap<>(),
                        new HashMap<>());
        if (sourceConfig.getStartupConfig().getStartupMode() == StartupMode.INITIAL) {
            try {

                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner<>(
                                assignerContext,
                                enumeratorContext.currentParallelism(),
                                incrementalParallelism,
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner =
                    new IncrementalSplitAssigner<>(
                            assignerContext, incrementalParallelism, offsetFactory);
        }

        return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext,
            PendingSplitsState checkpointState)
            throws Exception {
        C sourceConfig = configFactory.create(0);
        Set<TableId> capturedTables =
                new HashSet<>(dataSourceDialect.discoverDataCollections(sourceConfig));

        final SplitAssigner splitAssigner;
        if (checkpointState instanceof HybridPendingSplitsState) {
            checkpointState = restore(capturedTables, (HybridPendingSplitsState) checkpointState);
            SnapshotPhaseState checkpointSnapshotState =
                    ((HybridPendingSplitsState) checkpointState).getSnapshotPhaseState();
            SplitAssigner.Context<C> assignerContext =
                    new SplitAssigner.Context<>(
                            sourceConfig,
                            capturedTables,
                            checkpointSnapshotState.getAssignedSplits(),
                            checkpointSnapshotState.getSplitCompletedOffsets());
            splitAssigner =
                    new HybridSplitAssigner<>(
                            assignerContext,
                            enumeratorContext.currentParallelism(),
                            incrementalParallelism,
                            (HybridPendingSplitsState) checkpointState,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpointState instanceof IncrementalPhaseState) {
            SplitAssigner.Context<C> assignerContext =
                    new SplitAssigner.Context<>(
                            sourceConfig, capturedTables, new HashMap<>(), new HashMap<>());
            splitAssigner =
                    new IncrementalSplitAssigner<>(
                            assignerContext, incrementalParallelism, offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpointState);
        }
        return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
    }

    private HybridPendingSplitsState restore(
            Set<TableId> capturedTables, HybridPendingSplitsState checkpointState) {
        SnapshotPhaseState checkpointSnapshotState = checkpointState.getSnapshotPhaseState();
        Set<TableId> checkpointCapturedTables =
                Stream.concat(
                                checkpointSnapshotState.getAlreadyProcessedTables().stream(),
                                checkpointSnapshotState.getRemainingTables().stream())
                        .collect(Collectors.toSet());
        Set<TableId> newTables = Sets.difference(capturedTables, checkpointCapturedTables);
        Set<TableId> deletedTables = Sets.difference(checkpointCapturedTables, capturedTables);

        checkpointSnapshotState.getRemainingTables().addAll(newTables);
        checkpointSnapshotState.getRemainingTables().removeAll(deletedTables);
        checkpointSnapshotState.getAlreadyProcessedTables().removeAll(deletedTables);
        Set<String> deletedSplitIds = new HashSet<>();
        Iterator<SnapshotSplit> splitIterator =
                checkpointSnapshotState.getRemainingSplits().iterator();
        while (splitIterator.hasNext()) {
            SnapshotSplit split = splitIterator.next();
            if (deletedTables.contains(split.getTableId())) {
                splitIterator.remove();
                deletedSplitIds.add(split.splitId());
            }
        }
        for (Map.Entry<String, SnapshotSplit> entry :
                checkpointSnapshotState.getAssignedSplits().entrySet()) {
            SnapshotSplit split = entry.getValue();
            if (deletedTables.contains(split.getTableId())) {
                deletedSplitIds.add(entry.getKey());
            }
        }
        deletedSplitIds.forEach(
                splitId -> {
                    checkpointSnapshotState.getAssignedSplits().remove(splitId);
                    checkpointSnapshotState.getSplitCompletedOffsets().remove(splitId);
                });
        return checkpointState;
    }
}
