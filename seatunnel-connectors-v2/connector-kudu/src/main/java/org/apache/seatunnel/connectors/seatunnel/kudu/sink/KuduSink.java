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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSinkState;

import com.google.auto.service.AutoService;

import java.io.IOException;

/**
 * Kudu Sink implementation by using SeaTunnel sink API. This class contains the method to create
 * {@link AbstractSimpleSink}.
 */
@AutoService(SeaTunnelSink.class)
public class KuduSink
        implements SeaTunnelSink<
                SeaTunnelRow, KuduSinkState, KuduCommitInfo, KuduAggregatedCommitInfo> {

    private KuduSinkConfig kuduSinkConfig;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "kudu";
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
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        ConfigValidator.of(config).validate(new KuduSinkFactory().optionRule());
        try {
            kuduSinkConfig = new KuduSinkConfig(config);
        } catch (Exception e) {
            throw new KuduConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, ExceptionUtils.getMessage(e)));
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, KuduCommitInfo, KuduSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new KuduSinkWriter(seaTunnelRowType, kuduSinkConfig);
    }
}
