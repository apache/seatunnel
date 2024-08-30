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
package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceReader.Context;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class TableStoreDBSource
        implements SeaTunnelSource<SeaTunnelRow, TableStoreDBSourceSplit, TableStoreDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private TablestoreOptions tablestoreOptions;
    private SeaTunnelRowType typeInfo;
    private JobContext jobContext;

    @Override
    public String getPluginName() {
        return "Tablestore";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return SeaTunnelSource.super.getProducedCatalogTables();
    }

    public TableStoreDBSource(ReadonlyConfig config) {
        this.tablestoreOptions = TablestoreOptions.of(config);
        CatalogTableUtil.buildWithConfig(config);
        this.typeInfo = CatalogTableUtil.buildWithConfig(config).getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, TableStoreDBSourceSplit> createReader(Context readerContext)
            throws Exception {
        return new TableStoreDBSourceReader(readerContext, tablestoreOptions, typeInfo);
    }

    @Override
    public SourceSplitEnumerator<TableStoreDBSourceSplit, TableStoreDBSourceState> createEnumerator(
            org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<TableStoreDBSourceSplit>
                    enumeratorContext)
            throws Exception {
        return new TableStoreDBSourceSplitEnumerator(enumeratorContext, tablestoreOptions);
    }

    @Override
    public SourceSplitEnumerator<TableStoreDBSourceSplit, TableStoreDBSourceState>
            restoreEnumerator(
                    org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<
                                    TableStoreDBSourceSplit>
                            enumeratorContext,
                    TableStoreDBSourceState checkpointState)
                    throws Exception {
        return new TableStoreDBSourceSplitEnumerator(
                enumeratorContext, tablestoreOptions, checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
