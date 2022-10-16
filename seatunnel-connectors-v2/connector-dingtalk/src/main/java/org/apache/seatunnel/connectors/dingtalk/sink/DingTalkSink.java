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

package org.apache.seatunnel.connectors.dingtalk.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter.Context;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.common.sink.AbstractSinkWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;


/**
 * DingTalk sink class
 */
@AutoService(SeaTunnelSink.class)
public class DingTalkSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private final String dtURL = "url";
    private final String dtSecret = "secret";

    @Override
    public String getPluginName() {
        return "DingTalk";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.getIsNull(dtURL)) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK,
                String.format("Config must include column : %s", dtURL));
        }
        if (pluginConfig.getIsNull(dtSecret)) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK,
                String.format("Config must include column : %s", dtSecret));
        }
        this.pluginConfig = pluginConfig;
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(Context context) throws IOException {
        return new DingTalkWriter(pluginConfig.getString(dtURL), pluginConfig.getString(dtSecret));
    }
}
