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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBConfig;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfo;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitInfoSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.committer.SelectDBCommitter;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkState;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkStateSerializer;
import org.apache.seatunnel.connectors.selectdb.sink.writer.SelectDBSinkWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SelectDBSink
        implements SeaTunnelSink<
                SeaTunnelRow, SelectDBSinkState, SelectDBCommitInfo, SelectDBCommitInfo> {

    private final SelectDBConfig dbConfig;
    private final CatalogTable catalogTable;
    private String jobId;

    public SelectDBSink(ReadonlyConfig pluginConfig, CatalogTable catalogTable) {
        this.dbConfig = SelectDBConfig.loadConfig(pluginConfig);
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return SelectDBSinkOptions.IDENTIFIER;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        SelectDBSinkWriter selectDBSinkWriter =
                new SelectDBSinkWriter(
                        context,
                        Collections.emptyList(),
                        catalogTable.getSeaTunnelRowType(),
                        dbConfig,
                        jobId);
        selectDBSinkWriter.initializeLoad(Collections.emptyList());
        return selectDBSinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, SelectDBCommitInfo, SelectDBSinkState> restoreWriter(
            SinkWriter.Context context, List<SelectDBSinkState> states) throws IOException {
        SelectDBSinkWriter selectDBSinkWriter =
                new SelectDBSinkWriter(
                        context, states, catalogTable.getSeaTunnelRowType(), dbConfig, jobId);
        selectDBSinkWriter.initializeLoad(states);
        return selectDBSinkWriter;
    }

    @Override
    public Optional<Serializer<SelectDBSinkState>> getWriterStateSerializer() {
        return Optional.of(new SelectDBSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<SelectDBCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new SelectDBCommitter(dbConfig));
    }

    @Override
    public Optional<Serializer<SelectDBCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new SelectDBCommitInfoSerializer());
    }

    @Override
    public Optional<SinkAggregatedCommitter<SelectDBCommitInfo, SelectDBCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<SelectDBCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
