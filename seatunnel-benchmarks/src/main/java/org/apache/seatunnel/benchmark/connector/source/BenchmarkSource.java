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

package org.apache.seatunnel.benchmark.connector.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.benchmark.connector.BenchmarkConnectorOptions;

import java.util.Collections;
import java.util.List;

/** Bounded, deterministic source for full-pipeline SeaTunnel benchmarks. */
public final class BenchmarkSource
        implements SeaTunnelSource<SeaTunnelRow, BenchmarkSourceSplit, BenchmarkSourceState>,
                SupportParallelism {

    public static final String PLUGIN_NAME = "BenchmarkSource";

    private final long totalRows;
    private final long ratePerSecond;
    private final int payloadSize;
    private final long startDelayMillis;
    private final int emitBatchSize;

    public BenchmarkSource(ReadonlyConfig config) {
        totalRows = config.get(BenchmarkConnectorOptions.TOTAL_ROWS);
        ratePerSecond = config.get(BenchmarkConnectorOptions.RATE_PER_SECOND);
        payloadSize = config.get(BenchmarkConnectorOptions.PAYLOAD_SIZE);
        startDelayMillis = config.get(BenchmarkConnectorOptions.START_DELAY_MILLIS);
        emitBatchSize = config.get(BenchmarkConnectorOptions.EMIT_BATCH_SIZE);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(BenchmarkConnectorOptions.catalogTable());
    }

    @Override
    public SourceReader<SeaTunnelRow, BenchmarkSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new BenchmarkSourceReader(readerContext);
    }

    @Override
    public SourceSplitEnumerator<BenchmarkSourceSplit, BenchmarkSourceState> createEnumerator(
            SourceSplitEnumerator.Context<BenchmarkSourceSplit> enumeratorContext) {
        return new BenchmarkSourceEnumerator(
                enumeratorContext,
                totalRows,
                ratePerSecond,
                payloadSize,
                emitBatchSize,
                System.currentTimeMillis() + startDelayMillis,
                Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<BenchmarkSourceSplit, BenchmarkSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<BenchmarkSourceSplit> enumeratorContext,
            BenchmarkSourceState checkpointState) {
        // Preserve the original schedule and assigned subtasks when restoring the enumerator.
        return new BenchmarkSourceEnumerator(
                enumeratorContext,
                totalRows,
                ratePerSecond,
                payloadSize,
                emitBatchSize,
                checkpointState.getStartEpochMillis(),
                checkpointState.getAssignedSubtasks());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
}
