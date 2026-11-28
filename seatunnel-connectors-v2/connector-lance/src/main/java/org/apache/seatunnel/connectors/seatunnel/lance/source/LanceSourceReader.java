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

package org.apache.seatunnel.connectors.seatunnel.lance.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.lance.config.LanceSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.lance.exception.LanceConnectorException;
import org.apache.seatunnel.connectors.seatunnel.lance.utils.ArrowRowConverter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.ipc.LanceScanner;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Reads one lance fragment per split and emits its rows as SeaTunnelRow. */
@Slf4j
public class LanceSourceReader implements SourceReader<SeaTunnelRow, LanceSourceSplit> {

    private final Deque<LanceSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final SourceReader.Context context;
    private final LanceSourceConfig config;
    private final SeaTunnelRowType rowType;

    private BufferAllocator allocator;
    private Dataset dataset;

    private volatile boolean noMoreSplit;

    public LanceSourceReader(
            SourceReader.Context context, LanceSourceConfig config, SeaTunnelRowType rowType) {
        this.context = context;
        this.config = config;
        this.rowType = rowType;
    }

    @Override
    public void open() throws Exception {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void close() throws IOException {
        log.info("Close lance source reader");
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                log.warn("Failed to close dataset: {}", e.getMessage());
            }
            dataset = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
        log.info("Close lance source reader success");
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        boolean processedSplit = false;
        synchronized (output.getCheckpointLock()) {
            LanceSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: {}", split);
                    readSplit(split, output);
                    processedSplit = true;
                } catch (Exception e) {
                    log.error("Read data from split {} failed", split, e);
                    throw new LanceConnectorException(
                            LanceConnectorErrorCode.DATASET_READ_EXCEPTION,
                            String.format(
                                    "Failed to read fragment %s of dataset %s: %s",
                                    split.getFragmentId(), config.getDatasetPath(), e.getMessage()),
                            e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("Lance source wait split!");
                }
            }
        }
        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded lance source");
            context.signalNoMoreElement();
        }
        if (!processedSplit) {
            // Avoid busy-waiting between polls when no split is available yet.
            Thread.sleep(1000L);
        }
    }

    private void readSplit(LanceSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        if (dataset == null || dataset.closed()) {
            dataset = Dataset.open(config.getDatasetPath(), config.toReadOptions());
        }
        Fragment fragment = dataset.getFragment(split.getFragmentId());

        try (LanceScanner scanner = fragment.newScan();
                ArrowReader batchReader = scanner.scanBatches()) {
            while (batchReader.loadNextBatch()) {
                VectorSchemaRoot root = batchReader.getVectorSchemaRoot();
                List<SeaTunnelRow> rows = new ArrayList<>(root.getRowCount());
                ArrowRowConverter.convertBatch(root, rowType, 0, rows);
                rows.forEach(output::collect);
            }
        }
    }

    @Override
    public void addSplits(List<LanceSourceSplit> splits) {
        log.info("Adding lance splits to reader: " + splits);
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Receive no more splits message, this lance reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public List<LanceSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
