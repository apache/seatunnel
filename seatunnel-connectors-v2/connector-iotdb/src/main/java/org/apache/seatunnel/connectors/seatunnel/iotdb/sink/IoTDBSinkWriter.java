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

package org.apache.seatunnel.connectors.seatunnel.iotdb.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.IoTDBRecord;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.SeaTunnelRowSerializer;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class IoTDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SeaTunnelRowSerializer serializer;
    private final IoTDBSinkClient sinkClient;

    public IoTDBSinkWriter(Config pluginConfig,
                           SeaTunnelRowType seaTunnelRowType) {
        SinkConfig sinkConfig = SinkConfig.loadConfig(pluginConfig);
        this.serializer = new DefaultSeaTunnelRowSerializer(
            seaTunnelRowType, sinkConfig.getStorageGroup(), sinkConfig.getKeyTimestamp(),
            sinkConfig.getKeyDevice(), sinkConfig.getKeyMeasurementFields());
        this.sinkClient = new IoTDBSinkClient(sinkConfig);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        IoTDBRecord record = serializer.serialize(element);
        sinkClient.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        sinkClient.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        sinkClient.close();
    }
}
