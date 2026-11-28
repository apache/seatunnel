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

package org.apache.seatunnel.connectors.seatunnel.confluence;

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
import java.util.List;

/**
 * Extracts one Confluence Cloud resource per bounded source.
 *
 * @see ConfluenceReader
 */
public final class ConfluenceSource
        implements SeaTunnelSource<SeaTunnelRow, ConfluenceSplit, ConfluenceEnumerator.State> {
    private static final long serialVersionUID = 1L;

    private final ConfluenceConfig config;

    public ConfluenceSource(ReadonlyConfig options) {
        config = new ConfluenceConfig(options);
    }

    @Override
    public String getPluginName() {
        return ConfluenceSourceFactory.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    /*
     * Rejects streaming execution because the connector exposes a single bounded full refresh.
     */
    @Override
    public void setJobContext(JobContext context) {
        if (context.getJobMode() != JobMode.BATCH) {
            throw ConfluenceConfig.invalid("job.mode must be BATCH");
        }
    }

    /**
     * Keeps one fixed schema across resources, retaining provider-specific fields inside content
     * JSON.
     */
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "resource", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(PhysicalColumn.of("id", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "audit_created_at",
                                        BasicType.LONG_TYPE,
                                        0,
                                        true,
                                        null,
                                        null))
                        .column(
                                PhysicalColumn.of(
                                        "content", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of(getPluginName(), TablePath.DEFAULT),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null));
    }

    /**
     * Creates the sole pagination reader; a scan cannot be partitioned across independent cursors.
     */
    @Override
    public SourceReader<SeaTunnelRow, ConfluenceSplit> createReader(SourceReader.Context context) {
        if (context.getIndexOfSubtask() != 0) {
            throw ConfluenceConfig.invalid("source parallelism must be 1");
        }
        return new ConfluenceReader(config, context);
    }

    @Override
    public SourceSplitEnumerator<ConfluenceSplit, ConfluenceEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<ConfluenceSplit> context) {
        return new ConfluenceEnumerator(context, ConfluenceSplit.initial(config), null);
    }

    /*
     * Restores unassigned work while the engine returns committed reader progress separately.
     */
    @Override
    public SourceSplitEnumerator<ConfluenceSplit, ConfluenceEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<ConfluenceSplit> context,
            ConfluenceEnumerator.State restored) {
        return new ConfluenceEnumerator(context, ConfluenceSplit.initial(config), restored);
    }
}
