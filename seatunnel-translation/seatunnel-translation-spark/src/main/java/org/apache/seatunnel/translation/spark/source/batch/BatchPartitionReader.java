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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.Handover;
import org.apache.seatunnel.translation.spark.source.InternalRowCollector;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class BatchPartitionReader implements InputPartitionReader<InternalRow> {
    protected static final Integer INTERVAL = 100;

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;

    protected final ExecutorService executorService;
    protected final Handover<InternalRow> handover;

    protected volatile boolean running = true;
    protected volatile boolean prepare = true;

    protected volatile InternalParallelSource<?, ?> parallelSource;

    public BatchPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer subtaskId) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
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
        return running;
    }

    private void prepare() {
        if (!prepare) {
            return;
        }
        this.parallelSource = createInternalParallelSource();
        try {
            this.parallelSource.open();
        } catch (Exception e) {
            running = false;
            throw new RuntimeException("");
        }
        executorService.execute(() -> {
            try {
                parallelSource.run(new InternalRowCollector(handover));
            } catch (Exception e) {
                handover.reportError(e);
            }
        });
        prepare = false;
    }

    protected InternalParallelSource<?, ?> createInternalParallelSource() {
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
