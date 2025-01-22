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

package org.apache.seatunnel.connectors.tailfile.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class TailFileSource
        implements SeaTunnelSource<SeaTunnelRow, TailFileSourceSplit, TailFileSourceState>,
                SupportParallelism {

    private final TailFileSourceConfig config;
    private JobContext jobContext;

    public TailFileSource(TailFileSourceConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return TailFileSourceFactory.IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        // todo
        // timestamp
        // metadata: file, pos, inode
        // env: ip, hostname
        // tags
        // message
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"file", "pos", "inode", "timestamp", "message"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.STRING_TYPE
                        });

        return Collections.singletonList(CatalogTableUtil.getCatalogTable("default", rowType));
    }

    @Override
    public SourceReader<SeaTunnelRow, TailFileSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new TailFileSourceReader(readerContext, config);
    }

    @Override
    public SourceSplitEnumerator<TailFileSourceSplit, TailFileSourceState> createEnumerator(
            SourceSplitEnumerator.Context<TailFileSourceSplit> enumeratorContext) {
        return new TailFileSourceSplitEnumerator(enumeratorContext, config);
    }

    @Override
    public SourceSplitEnumerator<TailFileSourceSplit, TailFileSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<TailFileSourceSplit> enumeratorContext,
            TailFileSourceState state) {
        return new TailFileSourceSplitEnumerator(enumeratorContext, config, state);
    }
}
