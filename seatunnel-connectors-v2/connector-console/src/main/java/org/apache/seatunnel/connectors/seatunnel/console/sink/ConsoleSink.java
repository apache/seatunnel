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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import static org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkFactory.LOG_PRINT_DATA;
import static org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkFactory.LOG_PRINT_DELAY;

@NoArgsConstructor
@AutoService(SeaTunnelSink.class)
public class ConsoleSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {
    private SeaTunnelRowType seaTunnelRowType;
    private boolean isPrintData = true;
    private int delayMs = 0;

    public ConsoleSink(SeaTunnelRowType seaTunnelRowType, ReadonlyConfig options) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.isPrintData = options.get(LOG_PRINT_DATA);
        this.delayMs = options.get(LOG_PRINT_DELAY);
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowType, context, isPrintData, delayMs);
    }

    @Override
    public String getPluginName() {
        return "Console";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.isPrintData = ReadonlyConfig.fromConfig(pluginConfig).get(LOG_PRINT_DATA);
        this.delayMs = ReadonlyConfig.fromConfig(pluginConfig).get(LOG_PRINT_DELAY);
    }
}
