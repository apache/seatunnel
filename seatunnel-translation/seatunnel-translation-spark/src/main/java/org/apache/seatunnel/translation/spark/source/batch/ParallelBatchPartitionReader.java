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
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.Handover;
import org.apache.seatunnel.translation.spark.source.InternalRowCollector;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ParallelBatchPartitionReader implements InputPartitionReader<InternalRow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelBatchPartitionReader.class);

    protected static final Integer INTERVAL = 100;

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;

    protected final ExecutorService executorService;
    protected final Handover<InternalRow> handover;

    protected final Object checkpointLock = new Object();

    protected volatile boolean running = true;
    protected volatile boolean prepare = true;

    protected volatile BaseSourceFunction<SeaTunnelRow> internalSource;

    public ParallelBatchPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer subtaskId) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
        this.executorService = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, getEnumeratorThreadName());
        this.handover = new Handover<>();
    }

    protected String getEnumeratorThreadName() {
        return String.format("parallel-split-enumerator-executor-%s", subtaskId);
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

        this.internalSource = createInternalSource();
        try {
            this.internalSource.open();
        } catch (Exception e) {
            running = false;
            throw new RuntimeException("");
        }
        executorService.execute(() -> {
            try {
                internalSource.run(new InternalRowCollector(handover, checkpointLock, source.getProducedType()));
            } catch (Exception e) {
                handover.reportError(e);
                LOGGER.error("BatchPartitionReader execute failed.", e);
                running = false;
            }
        });
        prepare = false;
    }

    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
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
        try {
            internalSource.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        executorService.shutdown();
    }

    public class InternalParallelSource<SplitT extends SourceSplit, StateT> extends ParallelSource<SeaTunnelRow, SplitT, StateT> {

        public InternalParallelSource(SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source, Map<Integer, List<byte[]>> restoredState, int parallelism, int subtaskId) {
            super(source, restoredState, parallelism, subtaskId);
        }

        @Override
        protected void handleNoMoreElement() {
            super.handleNoMoreElement();
            running = false;
        }
    }
}
