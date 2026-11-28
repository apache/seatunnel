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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

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

public final class AdobeCommerceSource
        implements SeaTunnelSource<SeaTunnelRow, AdobeCommerceSplit, AdobeCommerceSplit> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "AdobeCommerce";

    private final AdobeCommerceSettings settings;

    public AdobeCommerceSource(ReadonlyConfig options) {
        settings = new AdobeCommerceSettings(options);
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.STREAMING.equals(context.getJobMode())) {
            throw new IllegalArgumentException("AdobeCommerce source requires BATCH mode");
        }
    }

    @Override
    public String getPluginName() {
        return NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(requiredString("resource"))
                        .column(requiredString("id"))
                        .column(requiredString("record"))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(NAME, TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    private static PhysicalColumn requiredString(String name) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 0, false, null, null);
    }

    @Override
    public SourceReader<SeaTunnelRow, AdobeCommerceSplit> createReader(
            SourceReader.Context context) {
        return new AdobeCommerceReader(settings, context);
    }

    @Override
    public SourceSplitEnumerator<AdobeCommerceSplit, AdobeCommerceSplit> createEnumerator(
            SourceSplitEnumerator.Context<AdobeCommerceSplit> context) {
        return new ScanAssignment(context, settings, settings.initial(Instant.now()));
    }

    @Override
    public SourceSplitEnumerator<AdobeCommerceSplit, AdobeCommerceSplit> restoreEnumerator(
            SourceSplitEnumerator.Context<AdobeCommerceSplit> context, AdobeCommerceSplit saved) {
        settings.verify(saved);
        return new ScanAssignment(context, settings, saved);
    }

    /** Keep the original end time even when the earliest checkpoint has no reader state. */
    static final class ScanAssignment
            implements SourceSplitEnumerator<AdobeCommerceSplit, AdobeCommerceSplit> {
        private final Context<AdobeCommerceSplit> context;
        private final AdobeCommerceSettings settings;
        private final Set<Integer> idleReaders = new HashSet<>();
        private AdobeCommerceSplit position;
        private boolean started;
        private boolean assigned;

        ScanAssignment(
                Context<AdobeCommerceSplit> context,
                AdobeCommerceSettings settings,
                AdobeCommerceSplit position) {
            this.context = context;
            this.settings = settings;
            this.position = position.copy();
        }

        @Override
        public void open() {}

        @Override
        public void run() {
            synchronized (context) {
                started = true;
                context.registeredReaders().forEach(this::send);
            }
        }

        private void send(int reader) {
            if (!started) {
                return;
            }
            if (reader == 0 && !assigned) {
                context.assignSplit(0, position.copy());
                assigned = true;
            } else if (reader != 0 && idleReaders.add(reader)) {
                context.signalNoMoreSplits(reader);
            }
        }

        @Override
        public void addSplitsBack(List<AdobeCommerceSplit> splits, int reader) {
            synchronized (context) {
                if (splits.isEmpty()) {
                    return;
                }
                if (reader != 0 || splits.size() != 1) {
                    throw new IllegalStateException(
                            "Invalid AdobeCommerce restored reader position");
                }
                AdobeCommerceSplit checkpoint = splits.get(0);
                settings.verify(checkpoint);
                position = checkpoint.copy();
                assigned = false;
                if (context.registeredReaders().contains(0)) {
                    send(0);
                }
            }
        }

        @Override
        public void registerReader(int reader) {
            synchronized (context) {
                send(reader);
            }
        }

        @Override
        public void handleSplitRequest(int reader) {
            registerReader(reader);
        }

        @Override
        public int currentUnassignedSplitSize() {
            synchronized (context) {
                return assigned ? 0 : 1;
            }
        }

        @Override
        public AdobeCommerceSplit snapshotState(long checkpointId) {
            synchronized (context) {
                return position.copy();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void close() {}
    }
}
