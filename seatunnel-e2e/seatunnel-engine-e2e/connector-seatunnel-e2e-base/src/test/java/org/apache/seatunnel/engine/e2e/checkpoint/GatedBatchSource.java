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

package org.apache.seatunnel.engine.e2e.checkpoint;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Collections;
import java.util.List;

/**
 * Test-only BOUNDED source that keeps a BATCH job running until the test releases it.
 *
 * <p>The source is bounded on purpose: switching a failover test to STREAMING would change the
 * checkpoint-enable precondition the test guards. Being bounded would normally let the job drain on
 * its own, so the reader never signals "no more element" while {@link BatchSourceGate} holds it
 * open; releasing the gate restores the normal bounded behaviour and lets the job close.
 *
 * <p>See {@link GatedBatchSourceFactory} for the configuration options.
 */
public class GatedBatchSource
        implements SeaTunnelSource<SeaTunnelRow, GatedBatchSourceSplit, GatedBatchSourceState> {

    private final String holdKey;
    private final long emitIntervalMs;

    public GatedBatchSource(ReadonlyConfig options) {
        this.holdKey = options.get(GatedBatchSourceFactory.HOLD_KEY);
        this.emitIntervalMs = options.get(GatedBatchSourceFactory.EMIT_INTERVAL_MS);
    }

    @Override
    public String getPluginName() {
        return GatedBatchSourceFactory.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("e2e", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        new PhysicalColumn(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        GatedBatchSourceFactory.IDENTIFIER));
    }

    @Override
    public SourceReader<SeaTunnelRow, GatedBatchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new GatedBatchSourceReader(readerContext, holdKey, emitIntervalMs);
    }

    @Override
    public SourceSplitEnumerator<GatedBatchSourceSplit, GatedBatchSourceState> createEnumerator(
            SourceSplitEnumerator.Context<GatedBatchSourceSplit> enumeratorContext) {
        return new GatedBatchSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<GatedBatchSourceSplit, GatedBatchSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<GatedBatchSourceSplit> enumeratorContext,
            GatedBatchSourceState checkpointState) {
        return new GatedBatchSourceSplitEnumerator(enumeratorContext);
    }
}
