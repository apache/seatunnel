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

package org.apache.seatunnel.e2e.source.checkpointable;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
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

import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@AutoService(SeaTunnelSource.class)
public class CheckpointableSequenceSource
        implements SeaTunnelSource<
                SeaTunnelRow, CheckpointableSequenceSplit, CheckpointableSequenceState> {

    private ReadonlyConfig config;

    public CheckpointableSequenceSource() {}

    public CheckpointableSequenceSource(ReadonlyConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "CheckpointableSequenceSource";
    }

    @Override
    @Deprecated
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("e2e", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        new PhysicalColumn(
                                                "offset",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "CheckpointableSequenceSource"));
    }

    @Override
    public SourceReader<SeaTunnelRow, CheckpointableSequenceSplit> createReader(
            SourceReader.Context readerContext) {
        return new CheckpointableSequenceSourceReader(
                readerContext,
                config.get(CheckpointableSequenceSourceFactory.RECORDS_PER_POLL),
                config.get(CheckpointableSequenceSourceFactory.EMIT_INTERVAL_MS));
    }

    @Override
    public SourceSplitEnumerator<CheckpointableSequenceSplit, CheckpointableSequenceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<CheckpointableSequenceSplit> enumeratorContext) {
        return new CheckpointableSequenceSplitEnumerator(
                enumeratorContext, createInitialSplits(), false);
    }

    @Override
    public SourceSplitEnumerator<CheckpointableSequenceSplit, CheckpointableSequenceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<CheckpointableSequenceSplit> enumeratorContext,
                    CheckpointableSequenceState checkpointState) {
        return new CheckpointableSequenceSplitEnumerator(
                enumeratorContext,
                checkpointState.getPendingSplits(),
                checkpointState.getPendingSplits().isEmpty());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    private List<CheckpointableSequenceSplit> createInitialSplits() {
        long startOffset = config.get(CheckpointableSequenceSourceFactory.START_OFFSET);
        long endOffset = config.get(CheckpointableSequenceSourceFactory.END_OFFSET);
        int splitNum = config.get(CheckpointableSequenceSourceFactory.SPLIT_NUM);
        long total = Math.max(endOffset - startOffset, splitNum);
        long step = Math.max(1L, total / splitNum);

        List<CheckpointableSequenceSplit> splits = new ArrayList<>(splitNum);
        long currentStart = startOffset;
        for (int i = 0; i < splitNum; i++) {
            long currentEnd =
                    (i == splitNum - 1) ? endOffset : Math.min(endOffset, currentStart + step);
            splits.add(
                    new CheckpointableSequenceSplit(
                            "split-" + i, currentStart, currentEnd, currentStart));
            currentStart = currentEnd;
        }
        return splits;
    }
}
