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
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreConfig;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreSourceOptions;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class TableStoreSource
        implements SeaTunnelSource<SeaTunnelRow, TableStoreSourceSplit, TableStoreSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final TableStoreConfig tableStoreConfig;
    private final CatalogTable catalogTable;
    private JobContext jobContext;

    public TableStoreSource(ReadonlyConfig config) {
        this.tableStoreConfig = new TableStoreConfig(config);
        this.catalogTable = CatalogTableUtil.buildWithConfig(config);
    }

    @Override
    public String getPluginName() {
        return TableStoreSourceOptions.identifier;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, TableStoreSourceSplit> createReader(Context readerContext)
            throws Exception {
        return new TableStoreSourceReader(
                readerContext, tableStoreConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public SourceSplitEnumerator<TableStoreSourceSplit, TableStoreSourceState> createEnumerator(
            org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<TableStoreSourceSplit>
                    enumeratorContext)
            throws Exception {
        return new TableStoreSourceSplitEnumerator(enumeratorContext, tableStoreConfig);
    }

    @Override
    public SourceSplitEnumerator<TableStoreSourceSplit, TableStoreSourceState> restoreEnumerator(
            org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<TableStoreSourceSplit>
                    enumeratorContext,
            TableStoreSourceState checkpointState)
            throws Exception {
        return new TableStoreSourceSplitEnumerator(
                enumeratorContext, tableStoreConfig, checkpointState);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
