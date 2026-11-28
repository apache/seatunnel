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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.api.common.JobContext;
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
import org.apache.seatunnel.common.constants.JobMode;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Native bounded Amazon Advertising collection and asynchronous report source.
 *
 * <p>It advertises a single table shape across profile metadata, ad resources and dated report
 * records.
 */
public final class AmazonAdsSource
        implements SeaTunnelSource<SeaTunnelRow, AmazonAdsSplit, AmazonAdsSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "AmazonAds";

    private final AmazonAdsConfig config;

    public AmazonAdsSource(ReadonlyConfig options) {
        config = new AmazonAdsConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("AmazonAds requires BATCH mode");
        }
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "resource", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "profile_id", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "parent_id", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "window_start", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "window_end", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "record", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    @Override
    public SourceReader<SeaTunnelRow, AmazonAdsSplit> createReader(SourceReader.Context context) {
        return new AmazonAdsReader(config, context);
    }

    @Override
    public SourceSplitEnumerator<AmazonAdsSplit, AmazonAdsSplit> createEnumerator(
            SourceSplitEnumerator.Context<AmazonAdsSplit> context) {
        return new Enumerator(context, config, config.startAt(Instant.now()));
    }

    @Override
    public SourceSplitEnumerator<AmazonAdsSplit, AmazonAdsSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<AmazonAdsSplit> context, AmazonAdsSplit checkpoint) {
        checkpoint.validate(config);
        return new Enumerator(context, config, checkpoint);
    }

    /**
     * Freezes the job clock before reader assignment and returns saved scope on reassignment.
     *
     * <p>This covers a coordinator checkpoint taken before the active subtask receives its split.
     */
    static final class Enumerator implements SourceSplitEnumerator<AmazonAdsSplit, AmazonAdsSplit> {
        private final Context<AmazonAdsSplit> context;
        private final AmazonAdsConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private AmazonAdsSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(
                Context<AmazonAdsSplit> context, AmazonAdsConfig config, AmazonAdsSplit starting) {
            this.context = context;
            this.config = config;
            latest = starting.copy();
        }

        @Override
        public void open() {}

        @Override
        public void run() {
            synchronized (context) {
                running = true;
                context.registeredReaders().forEach(this::dispatch);
            }
        }

        private void dispatch(int reader) {
            if (!running) {
                return;
            }
            if (reader == 0 && !assigned) {
                context.assignSplit(0, latest.copy());
                assigned = true;
            } else if (reader != 0 && inactive.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        @Override
        public void addSplitsBack(List<AmazonAdsSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException("Invalid AmazonAds splits returned by reader");
                }
                AmazonAdsSplit recovered = splits.get(0);
                recovered.validate(config);
                latest = recovered.copy();
                assigned = false;
                if (context.registeredReaders().contains(0)) {
                    dispatch(0);
                }
            }
        }

        @Override
        public void registerReader(int subtask) {
            synchronized (context) {
                dispatch(subtask);
            }
        }

        @Override
        public void handleSplitRequest(int subtask) {
            registerReader(subtask);
        }

        @Override
        public AmazonAdsSplit snapshotState(long checkpointId) {
            synchronized (context) {
                return latest.copy();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}
    }
}
