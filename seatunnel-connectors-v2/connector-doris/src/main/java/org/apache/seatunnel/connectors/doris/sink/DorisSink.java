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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfoSerializer;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitter;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkStateSerializer;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DorisSink
        implements SeaTunnelSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo>,
                SupportSaveMode {

    private final DorisConfig dorisConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private String jobId;

    public DorisSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.dorisConfig = DorisConfig.of(config);
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new DorisSinkWriter(
                context, Collections.emptyList(), seaTunnelRowType, dorisConfig, jobId);
    }

    @Override
    public SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> restoreWriter(
            SinkWriter.Context context, List<DorisSinkState> states) throws IOException {
        return new DorisSinkWriter(context, states, seaTunnelRowType, dorisConfig, jobId);
    }

    @Override
    public Optional<Serializer<DorisSinkState>> getWriterStateSerializer() {
        return Optional.of(new DorisSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<DorisCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new DorisCommitter(dorisConfig));
    }

    @Override
    public Optional<Serializer<DorisCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DorisCommitInfoSerializer());
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        return Optional.empty();
    }
}
