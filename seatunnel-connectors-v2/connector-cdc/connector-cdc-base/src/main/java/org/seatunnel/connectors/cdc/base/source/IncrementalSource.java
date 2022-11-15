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

package org.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import io.debezium.relational.TableId;
import org.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.seatunnel.connectors.cdc.base.config.StopConfig;
import org.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.seatunnel.connectors.cdc.base.option.StartupMode;
import org.seatunnel.connectors.cdc.base.option.StopMode;
import org.seatunnel.connectors.cdc.base.source.enumerator.HybridSplitAssigner;
import org.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSourceEnumerator;
import org.seatunnel.connectors.cdc.base.source.enumerator.IncrementalSplitAssigner;
import org.seatunnel.connectors.cdc.base.source.enumerator.SplitAssigner;
import org.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public abstract class IncrementalSource<T, C extends SourceConfig> implements SeaTunnelSource<T, SourceSplitBase, PendingSplitsState> {

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
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);

        this.startupConfig = getStartupConfig(config);
        this.stopConfig = getStopConfig(config);
        this.stopMode = stopConfig.getStopMode();
        this.incrementalParallelism = config.get(SourceOptions.INCREMENTAL_PARALLELISM);
        this.configFactory = createSourceConfigFactory(config);
        this.offsetFactory = createOffsetFactory(config);
        this.deserializationSchema = createDebeziumDeserializationSchema(config);
        this.dataSourceDialect = createDataSourceDialect(config);
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

    public abstract OffsetFactory createOffsetFactory(ReadonlyConfig config);

    public abstract DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config);

    public abstract DataSourceDialect<C> createDataSourceDialect(ReadonlyConfig config);

    @Override
    public Boundedness getBoundedness() {
        return stopMode == StopMode.NEVER ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SourceReader<T, SourceSplitBase> createReader(SourceReader.Context readerContext) throws Exception {
        // TODO: https://github.com/apache/incubator-seatunnel/issues/3255
        // https://github.com/apache/incubator-seatunnel/issues/3256
        return null;
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
