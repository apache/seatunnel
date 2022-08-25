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

package org.apache.seatunnel.connectors.seatunnel.phoenix.sink;

import static org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig.CONNECT_URL;
import static org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig.TABLE;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;

@AutoService(SeaTunnelSink.class)
public class PhoenixSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    private PhoenixSinkConfig phoenixWriteConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private Config pluginConfig;

    @Override
    public String getPluginName() {
        return "Phoenix";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {

        CheckResult result = CheckConfigUtil.checkAllExists(config, TABLE, CONNECT_URL);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
        }
        this.phoenixWriteConfig = PhoenixSinkConfig.parse(config);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new PhoenixSinkWriter(phoenixWriteConfig, seaTunnelRowType);
    }
}
