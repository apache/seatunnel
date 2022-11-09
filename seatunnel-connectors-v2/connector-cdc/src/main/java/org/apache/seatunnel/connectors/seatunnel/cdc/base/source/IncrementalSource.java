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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.options.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.HybridSplitAssigner;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.SplitAssigner;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.StreamSplitAssigner;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.PendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.assigner.state.StreamPendingSplitsState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceRecords;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split.SourceSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader.IncrementalSourceReader;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.reader.IncrementalSourceSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import io.debezium.relational.TableId;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * The basic source of Incremental Snapshot framework for datasource, it is based on FLIP-27 and
 * Watermark Signal Algorithm which supports parallel reading snapshot of table and then continue to
 * capture data change by streaming reading.
 */

public abstract class IncrementalSource<T, C extends SourceConfig>
        implements SeaTunnelSource<T, SourceSplitBase, PendingSplitsState> {

    private static final long serialVersionUID = 1L;

    protected final SourceConfig.Factory<C> configFactory;
    protected final DataSourceDialect<C> dataSourceDialect;
    protected final OffsetFactory offsetFactory;
    protected final DebeziumDeserializationSchema<T> deserializationSchema;
    protected Config pluginConfig;

    public IncrementalSource(
            SourceConfig.Factory<C> configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            DataSourceDialect<C> dataSourceDialect) {
        this.configFactory = configFactory;
        this.deserializationSchema = deserializationSchema;
        this.offsetFactory = offsetFactory;
        this.dataSourceDialect = dataSourceDialect;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public IncrementalSourceReader<T, C> createReader(SourceReader.Context readerContext)
            throws Exception {
        // create source config for the given subtask (e.g. unique server id)
        C sourceConfig = configFactory.create(readerContext.getIndexOfSubtask());
        BlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue  = new LinkedBlockingQueue<>();

        Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier =
            () ->
                new IncrementalSourceSplitReader<>(
                    readerContext.getIndexOfSubtask(), dataSourceDialect, sourceConfig);

        return new IncrementalSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                createRecordEmitter(sourceConfig),
                new SourceReaderOptions(pluginConfig),
                readerContext,
                sourceConfig,
                dataSourceDialect);
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SourceSplitEnumerator.Context<SourceSplitBase> enumContext) {
        C sourceConfig = configFactory.create(0);
        final SplitAssigner splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new SeaTunnelException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new StreamSplitAssigner(sourceConfig, dataSourceDialect, offsetFactory);
        }

        // TODO return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
        return null;
    }

    @Override
    public SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
        SourceSplitEnumerator.Context<SourceSplitBase> enumContext, PendingSplitsState checkpoint) {
        C sourceConfig = configFactory.create(0);

        final SplitAssigner splitAssigner;
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner(
                            sourceConfig,
                            (StreamPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }

        // TODO return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
        return null;
    }

    @Override
    public SeaTunnelDataType<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
            SourceConfig sourceConfig) {
        return new IncrementalSourceRecordEmitter<>(
                deserializationSchema,
                sourceConfig.isIncludeSchemaChanges(),
                offsetFactory);
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
    }
}
