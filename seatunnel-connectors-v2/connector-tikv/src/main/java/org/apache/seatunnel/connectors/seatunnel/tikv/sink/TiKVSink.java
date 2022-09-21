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

package org.apache.seatunnel.connectors.seatunnel.tikv.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.Serializer;
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
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVConfig;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Xuxiaotuan
 * @since 2022-09-15 18:12
 */
@AutoService(SeaTunnelSink.class)
public class TiKVSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final TiKVParameters tiKVParameters = new TiKVParameters();
    private SeaTunnelRowType seaTunnelRowType;
    private Config config;

    @Override
    public String getPluginName() {
        return TiKVConfig.NAME;
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        this.config = config;
        CheckResult result = CheckConfigUtil.checkAllExists(config, TiKVConfig.HOST, TiKVConfig.PD_PORT, TiKVConfig.DATA_TYPE);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SINK, result.getMsg());
        }
        this.tiKVParameters.initConfig(config);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public Optional<Serializer<Void>> getWriterStateSerializer() {
        return super.getWriterStateSerializer();
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new TiKVSinkWriter(seaTunnelRowType, tiKVParameters);

    }
}
