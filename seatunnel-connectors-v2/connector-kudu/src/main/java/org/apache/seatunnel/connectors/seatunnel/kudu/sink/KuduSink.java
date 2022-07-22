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

package org.apache.seatunnel.connectors.seatunnel.kudu.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Kudu Sink implementation by using SeaTunnel sink API.
 * This class contains the method to create {@link KuduSinkWriter} and {@link KuduSinkAggregatedCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class KuduSink implements SeaTunnelSink<SeaTunnelRow, KuduSinkState, KuduCommitInfo, KuduAggregatedCommitInfo> {

    private Config config;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "kuduSink";
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
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
    }

    @Override
    public SinkWriter<SeaTunnelRow, KuduCommitInfo, KuduSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new KuduSinkWriter(seaTunnelRowType, config, context, System.currentTimeMillis());
    }

    @Override
    public SinkWriter<SeaTunnelRow, KuduCommitInfo, KuduSinkState> restoreWriter(SinkWriter.Context context, List<KuduSinkState> states) throws IOException {
        return new KuduSinkWriter(seaTunnelRowType, config, context, System.currentTimeMillis());
    }

    @Override
    public Optional<Serializer<KuduCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<KuduCommitInfo, KuduAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return Optional.of(new KuduSinkAggregatedCommitter());
    }

    @Override
    public Optional<Serializer<KuduAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }


}
