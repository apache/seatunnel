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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.StartMode;

import java.util.List;
import java.util.function.Supplier;

public class FlussSource
        implements SeaTunnelSource<SeaTunnelRow, FlussSourceSplit, FlussSourceState>,
                SupportParallelism {

    private final ReadonlyConfig readonlyConfig;
    private final FlussSourceConfig sourceConfig;
    private JobContext jobContext;

    public FlussSource(ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.sourceConfig = new FlussSourceConfig(readonlyConfig);
    }

    @Override
    public String getPluginName() {
        return FlussSourceOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return jobContext != null && JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getProducedCatalogTables();
    }

    @Override
    public SourceReader<SeaTunnelRow, FlussSourceSplit> createReader(
            SourceReader.Context readerContext) {
        Supplier<SplitReader<FlussRecord, FlussSourceSplit>> splitReaderSupplier =
                () -> new FlussSourceSplitReader(sourceConfig);
        return new FlussSourceReader(
                splitReaderSupplier,
                new FlussRecordEmitter(),
                new SourceReaderOptions(readonlyConfig),
                readerContext);
    }

    @Override
    public SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> createEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> enumeratorContext) {
        return new FlussSourceSplitEnumerator(sourceConfig, enumeratorContext, null, isStreaming());
    }

    @Override
    public SourceSplitEnumerator<FlussSourceSplit, FlussSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<FlussSourceSplit> enumeratorContext,
            FlussSourceState checkpointState) {
        return new FlussSourceSplitEnumerator(
                sourceConfig, enumeratorContext, checkpointState, isStreaming());
    }

    private boolean isStreaming() {
        return getBoundedness() == Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        checkStartMode(jobContext.getJobMode(), sourceConfig.getStartMode());
        this.jobContext = jobContext;
    }

    static void checkStartMode(JobMode jobMode, StartMode startMode) {
        if (JobMode.BATCH.equals(jobMode) && StartMode.LATEST.equals(startMode)) {
            throw new IllegalArgumentException(
                    "Fluss source option '"
                            + FlussSourceOptions.START_MODE.key()
                            + "=latest' is not supported in BATCH mode.");
        }
    }
}
