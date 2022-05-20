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

package org.apache.seatunnel.translation.spark.source.batch;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.Handover;
import org.apache.seatunnel.translation.spark.source.InternalRowCollector;
import org.apache.seatunnel.translation.state.EmptyLock;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BatchPartitionReader implements InputPartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPartitionReader.class);

    protected static final Integer INTERVAL = 100;

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;
    protected final StructType rowType;

    protected final ExecutorService executorService;
    protected final Handover<InternalRow> handover;

    protected volatile boolean running = true;
    protected volatile boolean prepare = true;

    protected volatile ParallelSource<SeaTunnelRow, ?, ?> parallelSource;
    protected volatile Collector<SeaTunnelRow> collector;

    public BatchPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer subtaskId, StructType rowType) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
        this.rowType = rowType;
        this.executorService = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, String.format("parallel-split-enumerator-executor-%s", subtaskId));
        this.handover = new Handover<>();
    }

    @Override
    public boolean next() throws IOException {
        prepare();
        while (running && handover.isEmpty()) {
            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return running || !handover.isEmpty();
    }

    protected void prepare() {
        if (!prepare) {
            return;
        }
        this.collector = createCollector();
        this.parallelSource = createParallelSource();
        try {
            this.parallelSource.open();
        } catch (Exception e) {
            running = false;
            throw new RuntimeException("");
        }
        executorService.execute(() -> {
            try {
                parallelSource.run(collector);
            } catch (Exception e) {
                handover.reportError(e);
                LOGGER.error("ParallelSource execute failed.", e);
                running = false;
            }
        });
        prepare = false;
    }

    protected Collector<SeaTunnelRow> createCollector() {
        return new InternalRowCollector(handover, new EmptyLock(), rowType);
    }

    protected ParallelSource<SeaTunnelRow, ?, ?> createParallelSource() {
        return new InternalParallelSource<>(source,
                null,
                parallelism,
                subtaskId);
    }

    @Override
    public InternalRow get() {
        try {
            return handover.pollNext().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        parallelSource.close();
        executorService.shutdown();
    }

    public class InternalParallelSource<SplitT extends SourceSplit, StateT> extends ParallelSource<SeaTunnelRow, SplitT, StateT> {

        public InternalParallelSource(SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source, List<byte[]> restoredState, int parallelism, int subtaskId) {
            super(source, restoredState, parallelism, subtaskId);
        }

        @Override
        protected void handleNoMoreElement() {
            super.handleNoMoreElement();
            running = false;
        }
    }
}
