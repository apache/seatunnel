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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.doris.client.DorisFlushTuple;
import org.apache.seatunnel.connectors.doris.client.DorisSinkManager;
import org.apache.seatunnel.connectors.doris.config.DorisSinkSemantics;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.state.StarRocksSinkState;
import org.apache.seatunnel.connectors.doris.util.DelimiterParserUtil;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DorisSinkWriter implements SinkWriter<SeaTunnelRow, Void, StarRocksSinkState> {

    private final SerializationSchema serializationSchema;
    private final DorisSinkManager manager;

    private final DorisSinkSemantics dorisSinkSemantics;

    public DorisSinkWriter(Config pluginConfig,
                           SeaTunnelRowType seaTunnelRowType,
                           SinkWriter.Context context) {
        this(pluginConfig, seaTunnelRowType,  context, null);
    }

    public DorisSinkWriter(Config pluginConfig,
                           SeaTunnelRowType seaTunnelRowType,
                           SinkWriter.Context context,
                           List<StarRocksSinkState> states) {
        SinkConfig sinkConfig = SinkConfig.loadConfig(pluginConfig);
        List<String> fieldNames = Arrays.stream(seaTunnelRowType.getFieldNames()).collect(Collectors.toList());
        this.serializationSchema = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager = new DorisSinkManager(sinkConfig, fieldNames);
        dorisSinkSemantics = sinkConfig.getDorisSinkSemantic();

        if (DorisSinkSemantics.AT_LEAST_ONCE.equals(dorisSinkSemantics)) {
            //start AsyncFlush thread
            manager.startAsyncFlushing();

            //restore states in manager
            if (states != null) {
                for (StarRocksSinkState state : states) {
                    manager.setBufferedBatchMap(state.getBufferMap());
                }
            }
        }

    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record = new String(serializationSchema.serialize(element));
        manager.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() throws IOException {
        //Flush to storage before snapshot state is performed
        if (DorisSinkSemantics.NON.equals(dorisSinkSemantics)) {
            manager.flush();
        }
        if (DorisSinkSemantics.AT_LEAST_ONCE.equals(dorisSinkSemantics)) {
            //async process previous buffer
            manager.flushPreviousBuffer(true);
            //sync process current buffer
            DorisFlushTuple sinkBuffer = manager.getCurrentSinkBuffer();
            manager.addBufferedBatchMap(sinkBuffer);
            manager.resetCurrentSinkBuffer();
            manager.flushPreviousBuffer(true);
        }
        return Optional.empty();
    }

    @Override
    public List<StarRocksSinkState> snapshotState(long checkpointId) throws IOException {
        if (DorisSinkSemantics.AT_LEAST_ONCE.equals(dorisSinkSemantics)) {
            // save previousBufferMap to state
            return Collections.singletonList(new StarRocksSinkState(checkpointId, manager.getPreviousBufferMap()));
        }
        return null;
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            throw new DorisConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED,
                    "Close doris manager failed.", e);
        }
    }

    public static SerializationSchema createSerializer(SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            String columnSeparator = DelimiterParserUtil.parse(sinkConfig.getColumnSeparator(), "\t");
            return TextSerializationSchema.builder()
                    .seaTunnelRowType(seaTunnelRowType)
                    .delimiter(columnSeparator)
                    .build();
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new JsonSerializationSchema(seaTunnelRowType);
        }
        throw new DorisConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
