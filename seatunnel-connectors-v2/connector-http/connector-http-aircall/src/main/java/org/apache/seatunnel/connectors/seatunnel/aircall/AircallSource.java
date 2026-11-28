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
package org.apache.seatunnel.connectors.seatunnel.aircall;

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

public final class AircallSource
        implements SeaTunnelSource<SeaTunnelRow, AircallSplit, AircallSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Aircall";

    private final AircallConfig config;

    public AircallSource(ReadonlyConfig options) {
        config = new AircallConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("Aircall requires BATCH mode");
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
                                        "parent_id", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "call_started_at",
                                        BasicType.LONG_TYPE,
                                        0,
                                        true,
                                        null,
                                        null))
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
    public SourceReader<SeaTunnelRow, AircallSplit> createReader(SourceReader.Context context) {
        return new AircallReader(config, context);
    }

    @Override
    public SourceSplitEnumerator<AircallSplit, AircallSplit> createEnumerator(
            SourceSplitEnumerator.Context<AircallSplit> context) {
        return new Enumerator(context, config, config.start(Instant.now()));
    }

    @Override
    public SourceSplitEnumerator<AircallSplit, AircallSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<AircallSplit> context, AircallSplit checkpoint) {
        config.validate(checkpoint);
        return new Enumerator(context, config, checkpoint);
    }

    /** Preserve the coordinator's frozen cutoff when restoring a pre-assignment checkpoint. */
    static final class Enumerator implements SourceSplitEnumerator<AircallSplit, AircallSplit> {
        private final Context<AircallSplit> context;
        private final AircallConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private AircallSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(Context<AircallSplit> context, AircallConfig config, AircallSplit starting) {
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
        public void addSplitsBack(List<AircallSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException("Invalid Aircall splits returned by reader");
                }
                AircallSplit recovered = splits.get(0);
                config.validate(recovered);
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
        public AircallSplit snapshotState(long checkpointId) {
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
