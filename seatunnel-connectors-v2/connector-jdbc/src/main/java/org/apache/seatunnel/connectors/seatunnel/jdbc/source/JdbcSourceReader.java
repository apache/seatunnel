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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * JDBC Source reader.
 *
 * <p>Keeps only a bounded local split queue. When the queue runs low and the enumerator has not
 * signaled {@code NoMoreSplits}, the reader requests the next assignment batch.
 */
@Slf4j
public class JdbcSourceReader implements SourceReader<SeaTunnelRow, JdbcSourceSplit> {
    private final Context context;
    private final JdbcInputFormat inputFormat;
    private final Deque<JdbcSourceSplit> splits = new ConcurrentLinkedDeque<>();
    private final int assignBatchSize;
    private final int requestWatermark;
    private volatile boolean noMoreSplit;
    private volatile boolean splitRequestPending;

    public JdbcSourceReader(
            Context context, JdbcSourceConfig config, Map<TablePath, CatalogTable> tables) {
        this.inputFormat = new JdbcInputFormat(config, tables);
        this.context = context;
        int configuredBatchSize = config.getSplitAssignBatchSize();
        this.assignBatchSize =
                configuredBatchSize > 0
                        ? configuredBatchSize
                        : JdbcSourceOptions.SPLIT_ASSIGN_BATCH_SIZE.defaultValue();
        // Request the next batch before the local queue is fully drained to reduce idle gaps.
        this.requestWatermark = Math.max(1, assignBatchSize / 2);
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
        requestSplitsIfNeeded();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();
    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            requestSplitsIfNeeded();
            JdbcSourceSplit split = splits.poll();
            if (null != split) {
                try {
                    inputFormat.open(split);
                    while (!inputFormat.reachedEnd()) {
                        SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                        output.collect(seaTunnelRow);
                    }
                } finally {
                    inputFormat.close();
                }
                requestSplitsIfNeeded();
            } else if (noMoreSplit && splits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded jdbc source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(100L);
            }
        }
    }

    @Override
    public List<JdbcSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<JdbcSourceSplit> splits) {
        this.splits.addAll(splits);
        splitRequestPending = false;
        requestSplitsIfNeeded();
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
        splitRequestPending = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void requestSplitsIfNeeded() {
        if (noMoreSplit || splitRequestPending) {
            return;
        }
        if (splits.size() >= requestWatermark) {
            return;
        }
        splitRequestPending = true;
        context.sendSplitRequest();
    }
}
