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

package org.apache.seatunnel.connectors.seatunnel.pulsar.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarAdminConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.pulsar.state.PulsarSinkState;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.common.PropertiesUtil.setOption;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.ADMIN_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.AUTH_PLUGIN_CLASS;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.CLIENT_SERVICE_URL;
import static org.apache.seatunnel.connectors.seatunnel.pulsar.config.SourceProperties.TOPIC;

/**
 * Pulsar Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link PulsarSinkWriter} and {@link PulsarSinkCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class PulsarSink
        implements SeaTunnelSink<
                SeaTunnelRow, PulsarSinkState, PulsarCommitInfo, PulsarAggregatedCommitInfo> {

    private PulsarAdminConfig adminConfig;
    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private PulsarClientConfig clientConfig;

    @Override
    public void prepare(Config config) throws PrepareFailException {
        this.pluginConfig = config;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        TOPIC.key(),
                        CLIENT_SERVICE_URL.key(),
                        ADMIN_SERVICE_URL.key());
        if (!result.isSuccess()) {
            throw new PulsarConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        // admin config
        PulsarAdminConfig.Builder adminConfigBuilder =
                PulsarAdminConfig.builder().adminUrl(config.getString(ADMIN_SERVICE_URL.key()));
        setOption(
                config,
                AUTH_PLUGIN_CLASS.key(),
                config::getString,
                adminConfigBuilder::authPluginClassName);
        setOption(config, AUTH_PARAMS.key(), config::getString, adminConfigBuilder::authParams);
        this.adminConfig = adminConfigBuilder.build();

        // client config
        PulsarClientConfig.Builder clientConfigBuilder =
                PulsarClientConfig.builder().serviceUrl(config.getString(CLIENT_SERVICE_URL.key()));
        setOption(
                config,
                AUTH_PLUGIN_CLASS.key(),
                config::getString,
                clientConfigBuilder::authPluginClassName);
        setOption(config, AUTH_PARAMS.key(), config::getString, clientConfigBuilder::authParams);
        this.clientConfig = clientConfigBuilder.build();
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
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> createWriter(
            SinkWriter.Context context) {
        return new PulsarSinkWriter(
                context, clientConfig, seaTunnelRowType, pluginConfig, Collections.emptyList());
    }

    @Override
    public SinkWriter<SeaTunnelRow, PulsarCommitInfo, PulsarSinkState> restoreWriter(
            SinkWriter.Context context, List<PulsarSinkState> states) {
        return new PulsarSinkWriter(context, clientConfig, seaTunnelRowType, pluginConfig, states);
    }

    @Override
    public Optional<Serializer<PulsarSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<PulsarCommitInfo>> createCommitter() {
        return Optional.of(new PulsarSinkCommitter(clientConfig));
    }

    @Override
    public Optional<Serializer<PulsarCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return PulsarConfigUtil.IDENTIFIER;
    }
}
