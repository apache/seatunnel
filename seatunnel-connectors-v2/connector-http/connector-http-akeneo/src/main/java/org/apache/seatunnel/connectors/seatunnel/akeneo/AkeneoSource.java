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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class AkeneoSource implements SeaTunnelSource<SeaTunnelRow, AkeneoSplit, AkeneoSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Akeneo";

    private final AkeneoConfig config;

    public AkeneoSource(ReadonlyConfig options) {
        config = new AkeneoConfig(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("Akeneo requires BATCH mode");
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
    public SourceReader<SeaTunnelRow, AkeneoSplit> createReader(SourceReader.Context context) {
        return new AkeneoReader(config, context);
    }

    @Override
    public SourceSplitEnumerator<AkeneoSplit, AkeneoSplit> createEnumerator(
            SourceSplitEnumerator.Context<AkeneoSplit> context) {
        return new Enumerator(context, config, new AkeneoSplit(config.scope));
    }

    @Override
    public SourceSplitEnumerator<AkeneoSplit, AkeneoSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<AkeneoSplit> context, AkeneoSplit checkpoint) {
        checkpoint.validate(config);
        return new Enumerator(context, config, checkpoint);
    }

    /** The coordinator preserves the original scope before reader assignment. */
    static final class Enumerator implements SourceSplitEnumerator<AkeneoSplit, AkeneoSplit> {
        private final Context<AkeneoSplit> context;
        private final AkeneoConfig config;
        private final Set<Integer> inactive = new HashSet<>();
        private AkeneoSplit latest;
        private boolean running;
        private boolean assigned;

        Enumerator(Context<AkeneoSplit> context, AkeneoConfig config, AkeneoSplit starting) {
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
        public void addSplitsBack(List<AkeneoSplit> splits, int subtask) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (subtask != 0 || splits.size() != 1) {
                    throw new IllegalStateException("Invalid Akeneo splits returned by reader");
                }
                AkeneoSplit recovered = splits.get(0);
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
        public AkeneoSplit snapshotState(long checkpointId) {
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
