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

package org.apache.seatunnel.connectors.seatunnel.sentry.sink;

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
 * @description: SentrySink class
 */
@AutoService(SeaTunnelSink.class)
public class SentrySink extends AbstractSimpleSink<SeaTunnelRow, SentrySinkState> {

    private SeaTunnelRowType seaTunnelRowType;
    private Config pluginConfig;
    @Override
    public String getPluginName() {
        return SentryConfig.SENTRY;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (!pluginConfig.hasPath(SentryConfig.DSN)) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK,
                    String.format("Config must include column : %s", SentryConfig.DSN));
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
    public AbstractSinkWriter<SeaTunnelRow, SentrySinkState> createWriter(Context context) throws IOException {
        return new SentrySinkWriter(seaTunnelRowType, context, pluginConfig);
    }
}
