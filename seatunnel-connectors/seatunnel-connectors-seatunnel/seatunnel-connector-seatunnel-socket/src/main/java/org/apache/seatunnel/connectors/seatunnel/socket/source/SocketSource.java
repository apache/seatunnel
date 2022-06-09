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

package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.socket.state.SocketState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class SocketSource implements SeaTunnelSource<SeaTunnelRow, SocketSourceSplit, SocketState> {
    private SocketSourceParameter parameter;
    private SeaTunnelContext seaTunnelContext;
    @Override
    public String getPluginName() {
        return "SocketSource";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.parameter = ConfigBeanFactory.create(pluginConfig, SocketSourceParameter.class);
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return this.seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return new SeaTunnelRowTypeInfo(
                new String[]{"value"},
                new SeaTunnelDataType<?>[]{BasicType.STRING});
    }

    @Override
    public SourceReader<SeaTunnelRow, SocketSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new SocketSourceReader(this.parameter, readerContext);
    }

    @Override
    public SourceSplitEnumerator<SocketSourceSplit, SocketState> createEnumerator(SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext) throws Exception {
        return new SocketSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SocketSourceSplit, SocketState> restoreEnumerator(SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext, SocketState checkpointState) throws Exception {
        return null;
    }

    @Override
    public Serializer<SocketState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
