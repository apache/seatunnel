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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.client.DorisSinkManager;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.serialize.DorisCsvSerializer;
import org.apache.seatunnel.connectors.doris.serialize.DorisISerializer;
import org.apache.seatunnel.connectors.doris.serialize.DorisJsonSerializer;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DorisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final DorisISerializer serializer;
    private final DorisSinkManager manager;

    public DorisSinkWriter(Config pluginConfig,
                           SeaTunnelRowType seaTunnelRowType) {
        SinkConfig sinkConfig = SinkConfig.loadConfig(pluginConfig);
        List<String> fieldNames = Arrays.stream(seaTunnelRowType.getFieldNames()).collect(Collectors.toList());
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager = new DorisSinkManager(sinkConfig, fieldNames);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record = serializer.serialize(element);
        manager.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        manager.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            log.error("Close doris manager failed.", e);
            throw new IOException("Close doris manager failed.", e);
        }
    }

    public static DorisISerializer createSerializer(SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return new DorisCsvSerializer(sinkConfig.getColumnSeparator(), seaTunnelRowType);
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new DorisJsonSerializer(seaTunnelRowType);
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
