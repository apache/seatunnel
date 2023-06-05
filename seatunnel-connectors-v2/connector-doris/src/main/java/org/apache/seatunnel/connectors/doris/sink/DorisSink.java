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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfoSerializer;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitter;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkStateSerializer;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class DorisSink
        implements SeaTunnelSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private String jobId;

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        DorisConfig.FENODES.key(),
                        DorisConfig.USERNAME.key(),
                        DorisConfig.TABLE_IDENTIFIER.key());
        if (!result.isSuccess()) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        DorisSinkWriter dorisSinkWriter =
                new DorisSinkWriter(
                        context, Collections.emptyList(), seaTunnelRowType, pluginConfig, jobId);
        dorisSinkWriter.initializeLoad(Collections.emptyList());
        return dorisSinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, DorisCommitInfo, DorisSinkState> restoreWriter(
            SinkWriter.Context context, List<DorisSinkState> states) throws IOException {
        DorisSinkWriter dorisWriter =
                new DorisSinkWriter(context, states, seaTunnelRowType, pluginConfig, jobId);
        dorisWriter.initializeLoad(states);
        return dorisWriter;
    }

    @Override
    public Optional<Serializer<DorisSinkState>> getWriterStateSerializer() {
        return Optional.of(new DorisSinkStateSerializer());
    }

    @Override
    public Optional<SinkCommitter<DorisCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new DorisCommitter(pluginConfig));
    }

    @Override
    public Optional<Serializer<DorisCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DorisCommitInfoSerializer());
    }

    @Override
    public Optional<SinkAggregatedCommitter<DorisCommitInfo, DorisCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<DorisCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }
}
