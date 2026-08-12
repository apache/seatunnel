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

package org.apache.seatunnel.benchmark.connector.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.benchmark.connector.BenchmarkConnectorOptions;

import java.util.Optional;

/** Sink definition for the full-pipeline benchmark observer. */
public final class BenchmarkSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {

    public static final String PLUGIN_NAME = "BenchmarkSink";

    private final CatalogTable catalogTable;
    private final String resultPath;
    private final String runId;
    private final long expectedRows;
    private final long offeredRate;
    private final int payloadSize;
    private final int transformOperations;
    private final int maxTrackedLatencyMillis;
    private final long maxP99LatencyMillis;
    private final double maxLatencyGrowthRatio;

    public BenchmarkSink(CatalogTable catalogTable, ReadonlyConfig config) {
        this.catalogTable = catalogTable;
        this.resultPath = config.get(BenchmarkConnectorOptions.RESULT_PATH);
        this.runId = config.get(BenchmarkConnectorOptions.RUN_ID);
        this.expectedRows = config.get(BenchmarkConnectorOptions.EXPECTED_ROWS);
        this.offeredRate = config.get(BenchmarkConnectorOptions.RATE_PER_SECOND);
        this.payloadSize = config.get(BenchmarkConnectorOptions.PAYLOAD_SIZE);
        this.transformOperations = config.get(BenchmarkConnectorOptions.TRANSFORM_OPERATIONS);
        this.maxTrackedLatencyMillis =
                config.get(BenchmarkConnectorOptions.MAX_TRACKED_LATENCY_MILLIS);
        this.maxP99LatencyMillis = config.get(BenchmarkConnectorOptions.MAX_P99_LATENCY_MILLIS);
        this.maxLatencyGrowthRatio = config.get(BenchmarkConnectorOptions.MAX_LATENCY_GROWTH_RATIO);
    }

    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new BenchmarkSinkWriter(
                context,
                resultPath,
                runId,
                expectedRows,
                offeredRate,
                payloadSize,
                transformOperations,
                maxTrackedLatencyMillis,
                maxP99LatencyMillis,
                maxLatencyGrowthRatio);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }
}
