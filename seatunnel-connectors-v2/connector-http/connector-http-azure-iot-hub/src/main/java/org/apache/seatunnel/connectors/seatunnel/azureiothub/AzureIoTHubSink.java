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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

/**
 * Writes INSERT telemetry under one registered device identity. Restored writers have no pending
 * transactions, so replayed source rows can produce duplicate endpoint messages.
 */
public final class AzureIoTHubSink implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void> {
    private static final long serialVersionUID = 1L;

    private final AzureIoTHubConfig config;
    private final SeaTunnelRowType rowType;

    AzureIoTHubSink(ReadonlyConfig options, SeaTunnelRowType rowType) {
        this.config = new AzureIoTHubConfig(options, rowType);
        this.rowType = rowType;
    }

    @Override
    public String getPluginName() {
        return AzureIoTHubSinkFactory.IDENTIFIER;
    }

    /**
     * Each worker subtask owns its transport. Serializing the Sink must never transfer an open
     * socket, executor or mutable writer between coordinator and worker processes.
     */
    @Override
    public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
        return new AzureIoTHubSinkWriter(config, rowType);
    }
}
