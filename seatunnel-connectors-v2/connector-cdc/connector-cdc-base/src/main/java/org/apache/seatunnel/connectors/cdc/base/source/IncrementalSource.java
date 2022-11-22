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

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.HybridSplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.SplitAssigner;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

public abstract class IncrementalSource<T, C extends SourceConfig> implements SeaTunnelSource<T, SourceSplitBase, PendingSplitsState> {

    protected ReadonlyConfig readonlyConfig;
    protected SourceConfig.Factory<C> configFactory;
    protected OffsetFactory offsetFactory;

    protected DataSourceDialect<C> dataSourceDialect;
    protected StartupConfig startupConfig;

    protected int incrementalParallelism;
    protected StopConfig stopConfig;

    protected StopMode stopMode;
    protected DebeziumDeserializationSchema<T> deserializationSchema;

    @Override
    public final void prepare(Config pluginConfig) throws PrepareFailException {
        this.readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);

        this.startupConfig = getStartupConfig(readonlyConfig);
        this.stopConfig = getStopConfig(readonlyConfig);
        this.stopMode = stopConfig.getStopMode();
        this.incrementalParallelism = readonlyConfig.get(SourceOptions.INCREMENTAL_PARALLELISM);
        this.configFactory = createSourceConfigFactory(readonlyConfig);
        this.deserializationSchema = createDebeziumDeserializationSchema(readonlyConfig);
        this.dataSourceDialect = createDataSourceDialect(readonlyConfig);
        this.offsetFactory = createOffsetFactory(readonlyConfig);
    }

    protected StartupConfig getStartupConfig(ReadonlyConfig config) {
        return new StartupConfig(
            config.get(SourceOptions.STARTUP_MODE),
            config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE),
            config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS),
            config.get(SourceOptions.STARTUP_TIMESTAMP));
    }

    private StopConfig getStopConfig(ReadonlyConfig config) {
        return new StopConfig(
            config.get(SourceOptions.STOP_MODE),
            config.get(SourceOptions.STOP_SPECIFIC_OFFSET_FILE),
            config.get(SourceOptions.STOP_SPECIFIC_OFFSET_POS),
            config.get(SourceOptions.STOP_TIMESTAMP));
    }

    public abstract SourceConfig.Factory<C> createSourceConfigFactory(ReadonlyConfig config);

    public abstract DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config);

    public abstract DataSourceDialect<C> createDataSourceDialect(ReadonlyConfig config);

    public abstract OffsetFactory createOffsetFactory(ReadonlyConfig config);

    @Override
    public Boundedness getBoundedness() {
        return stopMode == StopMode.NEVER ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @SuppressWarnings("MagicNumber")
    @Override
    public SourceReader<T, SourceSplitBase> createReader(SourceReader.Context readerContext) throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        C sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue = new LinkedBlockingQueue<>(1024);

        Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier = () ->
            new IncrementalSourceSplitReader<>(
                readerContext.getIndexOfSubtask(), dataSourceDialect, sourceConfig);
        return new IncrementalSourceReader<>(
            elementsQueue,
            splitReaderSupplier,
            createRecordEmitter(sourceConfig),
            new SourceReaderOptions(readonlyConfig),
            readerContext,
            sourceConfig);
    }

    protected RecordEmitter<SourceRecords, T, SourceSplitStateBase> createRecordEmitter(
        SourceConfig sourceConfig) {
        return new IncrementalSourceRecordEmitter<>(
            deserializationSchema,
            offsetFactory);
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext) throws Exception {
        C sourceConfig = configFactory.create(0);
        final List<TableId> remainingTables = dataSourceDialect.discoverDataCollections(sourceConfig);
        final SplitAssigner splitAssigner;
        SplitAssigner.Context<C> assignerContext = new SplitAssigner.Context<>(sourceConfig, new HashSet<>(remainingTables), new HashMap<>(), new HashMap<>());
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
            splitAssigner = new IncrementalSplitAssigner<>(assignerContext, incrementalParallelism, offsetFactory);
        }

        return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext,
                                                                                        PendingSplitsState checkpointState) throws Exception {
        C sourceConfig = configFactory.create(0);
        final List<TableId> remainingTables = dataSourceDialect.discoverDataCollections(sourceConfig);
        SplitAssigner.Context<C> assignerContext = new SplitAssigner.Context<>(sourceConfig, new HashSet<>(remainingTables), new HashMap<>(), new HashMap<>());
        final SplitAssigner splitAssigner;
        if (checkpointState instanceof HybridPendingSplitsState) {
            splitAssigner = new HybridSplitAssigner<>(
                assignerContext,
                enumeratorContext.currentParallelism(),
                incrementalParallelism,
                (HybridPendingSplitsState) checkpointState,
                dataSourceDialect,
                offsetFactory);
        } else if (checkpointState instanceof IncrementalPhaseState) {
            splitAssigner = new IncrementalSplitAssigner<>(
                assignerContext,
                incrementalParallelism,
                offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported restored PendingSplitsState: " + checkpointState);
        }
        return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
    }
}
