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

package org.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.seatunnel.connectors.cdc.base.option.StartupMode;
import org.seatunnel.connectors.cdc.base.option.StopMode;
import org.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import java.io.Serializable;

public abstract class IncrementalSource<T, C extends SourceConfig> implements SeaTunnelSource<T, SourceSplit, Serializable> {

    protected OffsetFactory offsetFactory;
    protected Offset startupOffset;

    protected Offset stopOffset;

    protected StopMode stopMode;
    protected DebeziumDeserializationSchema<T> deserializationSchema;

    @Override
    public final void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);

        this.offsetFactory = createOffsetFactory();
        this.startupOffset = getStartupOffset(config);
        this.stopOffset = getStopOffset(config);
        this.stopMode = config.get(SourceOptions.STOP_MODE);
    }

    private Offset getStartupOffset(ReadonlyConfig config) {
        StartupMode startupMode = config.get(SourceOptions.STARTUP_MODE);
        switch (startupMode) {
            case EARLIEST:
                return offsetFactory.earliest();
            case LATEST:
                return offsetFactory.latest();
            case INITIAL:
                return null;
            case TIMESTAMP:
                return offsetFactory.timstamp(config.get(SourceOptions.STOP_TIMESTAMP));
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", startupMode));
        }
    }

    private Offset getStopOffset(ReadonlyConfig config) {
        StopMode stopMode = config.get(SourceOptions.STOP_MODE);
        switch (stopMode) {
            case LATEST:
                return offsetFactory.latest();
            case NEVER:
                return offsetFactory.neverStop();
            case TIMESTAMP:
                return offsetFactory.timstamp(config.get(SourceOptions.STOP_TIMESTAMP));
            default:
                throw new IllegalArgumentException(String.format("The %s mode is not supported.", stopMode));
        }
    }

    public abstract OffsetFactory createOffsetFactory();

    @Override
    public Boundedness getBoundedness() {
        return stopMode == StopMode.NEVER ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public SourceReader<T, SourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        // TODO: https://github.com/apache/incubator-seatunnel/issues/3255
        // https://github.com/apache/incubator-seatunnel/issues/3256
        return null;
    }

    @Override
    public SourceSplitEnumerator<SourceSplit, Serializable> createEnumerator(SourceSplitEnumerator.Context<SourceSplit> enumeratorContext) throws Exception {
        // TODO: https://github.com/apache/incubator-seatunnel/issues/3253
        // https://github.com/apache/incubator-seatunnel/issues/3254
        return null;
    }

    @Override
    public SourceSplitEnumerator<SourceSplit, Serializable> restoreEnumerator(SourceSplitEnumerator.Context<SourceSplit> enumeratorContext, Serializable checkpointState) throws Exception {
        // TODO: https://github.com/apache/incubator-seatunnel/issues/3253
        // https://github.com/apache/incubator-seatunnel/issues/3254
        return null;
    }
}
