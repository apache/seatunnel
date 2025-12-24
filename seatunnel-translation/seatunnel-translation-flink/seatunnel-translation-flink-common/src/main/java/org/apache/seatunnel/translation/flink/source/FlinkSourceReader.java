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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.event.ReaderCloseEvent;
import org.apache.seatunnel.api.source.event.ReaderOpenEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The implementation of {@link SourceReader}, used for proxy all {@link
 * org.apache.seatunnel.api.source.SourceReader} in flink.
 *
 * @param <SplitT>
 */
public class FlinkSourceReader<SplitT extends SourceSplit>
        implements SourceReader<SeaTunnelRow, SplitWrapper<SplitT>> {

    private final Logger LOGGER = LoggerFactory.getLogger(FlinkSourceReader.class);

    private final org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, SplitT> sourceReader;

    private final org.apache.seatunnel.api.source.SourceReader.Context context;

    private final FlinkRowCollector flinkRowCollector;

    private InputStatus inputStatus = InputStatus.MORE_AVAILABLE;

    private volatile CompletableFuture<Void> availabilityFuture;

    private static final long DEFAULT_WAIT_TIME_MILLIS = 1000L;

    private final ScheduledExecutorService scheduledExecutor;

    public FlinkSourceReader(
            org.apache.seatunnel.api.source.SourceReader<SeaTunnelRow, SplitT> sourceReader,
            org.apache.seatunnel.api.source.SourceReader.Context context,
            Config envConfig) {
        this.scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat(
                                        String.format(
                                                "source-reader-scheduler-%d",
                                                context.getIndexOfSubtask()))
                                .build());
        this.sourceReader = sourceReader;
        this.context = context;
        this.flinkRowCollector = new FlinkRowCollector(envConfig, context.getMetricsContext());
    }

    @Override
    public void start() {
        try {
            sourceReader.open();
            context.getEventListener().onEvent(new ReaderOpenEvent());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<SeaTunnelRow> output) throws Exception {
        if (!((FlinkSourceReaderContext) context).isSendNoMoreElementEvent()) {
            sourceReader.pollNext(flinkRowCollector.withReaderOutput(output));
            if (flinkRowCollector.isEmptyThisPollNext()) {
                synchronized (this) {
                    if (availabilityFuture == null || availabilityFuture.isDone()) {
                        availabilityFuture = new CompletableFuture<>();
                        scheduleComplete(availabilityFuture);
                        LOGGER.debug("No data available, wait for next poll.");
                    }
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        } else {
            // reduce CPU idle
            Thread.sleep(DEFAULT_WAIT_TIME_MILLIS);
        }
        return inputStatus;
    }

    @Override
    public List<SplitWrapper<SplitT>> snapshotState(long checkpointId) {
        try {
            List<SplitT> splitTS = sourceReader.snapshotState(checkpointId);
            return splitTS.stream().map(SplitWrapper::new).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        CompletableFuture<Void> future = availabilityFuture;
        return future != null ? future : CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<SplitWrapper<SplitT>> splits) {
        sourceReader.addSplits(
                splits.stream().map(SplitWrapper::getSourceSplit).collect(Collectors.toList()));
    }

    @Override
    public void notifyNoMoreSplits() {
        sourceReader.handleNoMoreSplits();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof NoMoreElementEvent) {
            inputStatus = InputStatus.END_OF_INPUT;
        }
        if (sourceEvent instanceof SourceEventWrapper) {
            sourceReader.handleSourceEvent((((SourceEventWrapper) sourceEvent).getSourceEvent()));
        }
    }

    @Override
    public void close() throws Exception {
        CompletableFuture<Void> future = availabilityFuture;
        if (future != null && !future.isDone()) {
            future.complete(null);
        }
        sourceReader.close();
        context.getEventListener().onEvent(new ReaderCloseEvent());
        scheduledExecutor.shutdown();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        sourceReader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        sourceReader.notifyCheckpointAborted(checkpointId);
    }

    private void scheduleComplete(CompletableFuture<Void> future) {
        scheduledExecutor.schedule(
                () -> future.complete(null), DEFAULT_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
    }
}
