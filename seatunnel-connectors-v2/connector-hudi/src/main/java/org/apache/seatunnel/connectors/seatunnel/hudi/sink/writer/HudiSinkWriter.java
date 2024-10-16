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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiClientManager;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiMultiTableResourceManager;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.WriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class HudiSinkWriter
        implements SinkWriter<SeaTunnelRow, HudiCommitInfo, HudiSinkState>,
                SupportMultiTableSinkWriter<HudiClientManager> {

    private WriteClientProvider writeClientProvider;

    private final HudiSinkConfig sinkConfig;

    private final HudiTableConfig tableConfig;

    private final SeaTunnelRowType seaTunnelRowType;

    private HudiRecordWriter hudiRecordWriter;

    private transient boolean isOpen;

    public HudiSinkWriter(
            Context context,
            SeaTunnelRowType seaTunnelRowType,
            HudiSinkConfig sinkConfig,
            HudiTableConfig tableConfig,
            List<HudiSinkState> hudiSinkState) {
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.writeClientProvider =
                new HudiWriteClientProvider(
                        sinkConfig, tableConfig.getTableName(), seaTunnelRowType);
        if (!hudiSinkState.isEmpty()) {
            this.hudiRecordWriter =
                    new HudiRecordWriter(
                            sinkConfig,
                            tableConfig,
                            writeClientProvider,
                            seaTunnelRowType,
                            hudiSinkState.get(0).getHudiCommitInfo());
        } else {
            this.hudiRecordWriter =
                    new HudiRecordWriter(
                            sinkConfig, tableConfig, writeClientProvider, seaTunnelRowType);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        hudiRecordWriter.writeRecord(element);
    }

    @Override
    public List<HudiSinkState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(
                new HudiSinkState(checkpointId, hudiRecordWriter.snapshotState()));
    }

    @Override
    public Optional<HudiCommitInfo> prepareCommit() throws IOException {
        tryOpen();
        return hudiRecordWriter.prepareCommit();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        hudiRecordWriter.close();
    }

    @Override
    public MultiTableResourceManager<HudiClientManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return new HudiMultiTableResourceManager(new HudiClientManager(sinkConfig));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<HudiClientManager> multiTableResourceManager,
            int queueIndex) {
        log.info("multi table resource manager is {}", multiTableResourceManager);
        this.hudiRecordWriter.close();
        this.writeClientProvider =
                new HudiWriteClientProviderProxy(
                        multiTableResourceManager.getSharedResource().get(),
                        seaTunnelRowType,
                        queueIndex,
                        tableConfig.getTableName());
        this.hudiRecordWriter =
                new HudiRecordWriter(
                        sinkConfig, tableConfig, writeClientProvider, seaTunnelRowType);
    }

    private void tryOpen() {
        if (!isOpen) {
            isOpen = true;
            hudiRecordWriter.open();
        }
    }
}
