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
            HudiTableConfig tableConfig) {
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig;
        this.seaTunnelRowType = seaTunnelRowType;
        this.writeClientProvider =
                new HudiWriteClientProvider(
                        sinkConfig, tableConfig.getTableName(), seaTunnelRowType);
        this.hudiRecordWriter =
                new HudiRecordWriter(
                        tableConfig,
                        writeClientProvider,
                        seaTunnelRowType,
                        sinkConfig.isExactlyOnce());
        context.registerFlushAction(this::timerFlush);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        hudiRecordWriter.writeRecord(element);
    }

    /**
     * Prepares the commit of the records written since the last checkpoint.
     *
     * <p>With the exactly-once semantics the records are written into a Hudi instant that is not
     * committed yet, and the returned commit info makes the aggregated committer commit the instant
     * once the checkpoint completes.
     *
     * @param checkpointId the checkpoint id
     * @return the commit info of this checkpoint, empty when there is nothing to commit
     */
    @Override
    public Optional<HudiCommitInfo> prepareCommit(long checkpointId) throws IOException {
        tryOpen();
        return hudiRecordWriter.prepareCommit(checkpointId);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Optional<HudiCommitInfo> prepareCommit() throws IOException {
        return prepareCommit(-1L);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Nothing is rolled back here on purpose. The data written for a checkpoint that never
     * completed is replayed by the source after the pipeline restarts, and the instants that are
     * left behind by the failed attempt are rolled back by Hudi itself, because their writer
     * heartbeat expires. Rolling the instant back here could also race with the aggregated
     * committer, which commits the instants of the checkpoints that did complete.
     */
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
                        tableConfig,
                        writeClientProvider,
                        seaTunnelRowType,
                        sinkConfig.isExactlyOnce());
    }

    /**
     * Flushes buffered records when the sink receives a timer-generated FlushSignal. The signal is
     * processed on the sink task thread in order with data records and checkpoint barriers.
     */
    private void timerFlush() {
        hudiRecordWriter.flush();
    }

    private void tryOpen() {
        if (!isOpen) {
            isOpen = true;
            hudiRecordWriter.open();
        }
    }
}
