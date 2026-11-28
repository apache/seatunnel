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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

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

/** A finite, serial export of the selected account collections. */
public final class ActiveCampaignSource
        implements SeaTunnelSource<SeaTunnelRow, ActiveCampaignSplit, ActiveCampaignSplit> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "ActiveCampaign";
    private final ActiveCampaignSourceOptions.Settings settings;

    public ActiveCampaignSource(ReadonlyConfig options) {
        settings = new ActiveCampaignSourceOptions.Settings(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("ActiveCampaign requires BATCH mode");
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
        TableSchema.Builder schema = TableSchema.builder();
        for (String field : new String[] {"resource", "id", "record"}) {
            schema.column(PhysicalColumn.of(field, BasicType.STRING_TYPE, 0, false, null, null));
        }
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.DEFAULT),
                        schema.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    @Override
    public SourceReader<SeaTunnelRow, ActiveCampaignSplit> createReader(
            SourceReader.Context context) {
        return new ActiveCampaignReader(settings, context);
    }

    @Override
    public SourceSplitEnumerator<ActiveCampaignSplit, ActiveCampaignSplit> createEnumerator(
            SourceSplitEnumerator.Context<ActiveCampaignSplit> context) {
        return new Enumerator(context, settings, settings.initial());
    }

    @Override
    public SourceSplitEnumerator<ActiveCampaignSplit, ActiveCampaignSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<ActiveCampaignSplit> context, ActiveCampaignSplit state) {
        settings.verify(state);
        return new Enumerator(context, settings, state);
    }

    /**
     * Retains the initial fallback through early checkpoints until a reader position is available.
     */
    static final class Enumerator
            implements SourceSplitEnumerator<ActiveCampaignSplit, ActiveCampaignSplit> {
        private final Context<ActiveCampaignSplit> context;
        private final ActiveCampaignSourceOptions.Settings settings;
        private final Set<Integer> ended = new HashSet<>();
        private ActiveCampaignSplit fallback;
        private boolean running;
        private boolean assigned;

        Enumerator(
                Context<ActiveCampaignSplit> context,
                ActiveCampaignSourceOptions.Settings settings,
                ActiveCampaignSplit fallback) {
            this.context = context;
            this.settings = settings;
            this.fallback = fallback.copy();
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
                context.assignSplit(reader, fallback.copy());
                assigned = true;
            } else if (reader != 0 && ended.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }

        @Override
        public void addSplitsBack(List<ActiveCampaignSplit> splits, int subtaskId) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (splits.size() != 1 || subtaskId != 0) {
                    throw new IllegalStateException(
                            "Invalid ActiveCampaign restored split assignment");
                }
                ActiveCampaignSplit restored = splits.get(0);
                settings.verify(restored);
                fallback = restored.copy();
                assigned = false;
                if (context.registeredReaders().contains(0)) {
                    dispatch(0);
                }
            }
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        @Override
        public void registerReader(int subtaskId) {
            synchronized (context) {
                dispatch(subtaskId);
            }
        }

        @Override
        public void handleSplitRequest(int subtaskId) {
            registerReader(subtaskId);
        }

        @Override
        public ActiveCampaignSplit snapshotState(long checkpointId) {
            synchronized (context) {
                return fallback.copy();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}
    }
}
