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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.tracing.MDCTracer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiTableSinkWriter
        implements SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> {

    private final Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext;
    private final Map<String, Optional<Integer>> sinkPrimaryKeys = new HashMap<>();
    private final List<ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>>
            sinkWritersWithIndex;
    private final List<MultiTableWriterRunnable> runnable = new ArrayList<>();
    private final Random random = new Random();
    private final List<BlockingQueue<SeaTunnelRow>> blockingQueues = new ArrayList<>();
    private final ExecutorService executorService;
    private volatile boolean submitted = false;
    private MultiTableResourceManager resourceManager;

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters,
            int queueSize,
            Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext) {
        this.sinkWriters = sinkWriters;
        this.sinkWritersContext = sinkWritersContext;
        AtomicInteger cnt = new AtomicInteger(0);
        executorService =
                MDCTracer.tracing(
                        Executors.newFixedThreadPool(
                                // we use it in `MultiTableWriterRunnable` and `prepare commit
                                // task`, so it
                                // should be double.
                                queueSize * 2,
                                runnable -> {
                                    Thread thread = new Thread(runnable);
                                    thread.setDaemon(true);
                                    thread.setName(
                                            "st-multi-table-sink-writer"
                                                    + "-"
                                                    + cnt.incrementAndGet());
                                    return thread;
                                }));
        sinkWritersWithIndex = new ArrayList<>();
        for (int i = 0; i < queueSize; i++) {
            BlockingQueue<SeaTunnelRow> queue = new LinkedBlockingQueue<>(1024);
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
            ConcurrentMap<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierMap =
                    new ConcurrentHashMap<>();
            int queueIndex = i;
            sinkWriters.entrySet().stream()
                    .filter(entry -> entry.getKey().getIndex() % queueSize == queueIndex)
                    .forEach(
                            entry -> {
                                tableIdWriterMap.put(
                                        entry.getKey().getTableIdentifier(), entry.getValue());
                                sinkIdentifierMap.put(entry.getKey(), entry.getValue());
                            });

            sinkWritersWithIndex.add(sinkIdentifierMap);
            blockingQueues.add(queue);
            MultiTableWriterRunnable r = new MultiTableWriterRunnable(tableIdWriterMap, queue);
            runnable.add(r);
        }
        log.info("init multi table sink writer, queue size: {}", queueSize);
        initResourceManager(queueSize);
    }

    private void initResourceManager(int queueSize) {
        for (SinkIdentifier tableIdentifier : sinkWriters.keySet()) {
            SinkWriter<SeaTunnelRow, ?, ?> sink = sinkWriters.get(tableIdentifier);
            resourceManager =
                    ((SupportMultiTableSinkWriter<?>) sink)
                            .initMultiTableResourceManager(sinkWriters.size(), queueSize);
            break;
        }

        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writerMap =
                    sinkWritersWithIndex.get(i);
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> entry :
                    writerMap.entrySet()) {
                SupportMultiTableSinkWriter<?> sink =
                        ((SupportMultiTableSinkWriter<?>) entry.getValue());
                sink.setMultiTableResourceManager(resourceManager, i);
                sinkPrimaryKeys.put(entry.getKey().getTableIdentifier(), sink.primaryKey());
            }
        }
    }

    private void subSinkErrorCheck() {
        for (MultiTableWriterRunnable writerRunnable : runnable) {
            if (writerRunnable.getThrowable() != null) {
                throw new RuntimeException(writerRunnable.getThrowable());
            }
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        subSinkErrorCheck();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                if (sinkWriterEntry
                        .getKey()
                        .getTableIdentifier()
                        .equals(event.tablePath().getFullName())) {
                    log.info(
                            "Start apply schema change for table {} sub-writer {}",
                            sinkWriterEntry.getKey().getTableIdentifier(),
                            sinkWriterEntry.getKey().getIndex());
                    synchronized (runnable.get(i)) {
                        sinkWriterEntry.getValue().applySchemaChange(event);
                    }
                    log.info(
                            "Finish apply schema change for table {} sub-writer {}",
                            sinkWriterEntry.getKey().getTableIdentifier(),
                            sinkWriterEntry.getKey().getIndex());
                }
            }
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (!submitted) {
            submitted = true;
            runnable.forEach(executorService::submit);
        }
        subSinkErrorCheck();
        Optional<Integer> primaryKey = sinkPrimaryKeys.get(element.getTableId());
        try {
            if ((primaryKey == null && sinkPrimaryKeys.size() == 1)
                    || (primaryKey != null && !primaryKey.isPresent())) {
                int index = random.nextInt(blockingQueues.size());
                BlockingQueue<SeaTunnelRow> queue = blockingQueues.get(index);
                while (!queue.offer(element, 500, TimeUnit.MILLISECONDS)) {
                    subSinkErrorCheck();
                }
            } else if (primaryKey == null) {
                throw new RuntimeException(
                        "multi table sink can not write table: " + element.getTableId());
            } else {
                Object object = element.getField(primaryKey.get());
                int index = 0;
                if (object != null) {
                    index = Math.abs(object.hashCode()) % blockingQueues.size();
                }
                BlockingQueue<SeaTunnelRow> queue = blockingQueues.get(index);
                while (!queue.offer(element, 500, TimeUnit.MILLISECONDS)) {
                    subSinkErrorCheck();
                }
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<MultiTableState> snapshotState(long checkpointId) throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        List<MultiTableState> multiTableStates = new ArrayList<>();
        MultiTableState multiTableState = new MultiTableState(new HashMap<>());
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                synchronized (runnable.get(i)) {
                    List states = sinkWriterEntry.getValue().snapshotState(checkpointId);
                    multiTableState.getStates().put(sinkWriterEntry.getKey(), states);
                }
            }
        }
        multiTableStates.add(multiTableState);
        return multiTableStates;
    }

    @Override
    public Optional<MultiTableCommitInfo> prepareCommit() throws IOException {
        checkQueueRemain();
        subSinkErrorCheck();
        MultiTableCommitInfo multiTableCommitInfo =
                new MultiTableCommitInfo(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            int subWriterIndex = i;
            futures.add(
                    executorService.submit(
                            () -> {
                                synchronized (runnable.get(subWriterIndex)) {
                                    for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>
                                            sinkWriterEntry :
                                                    sinkWritersWithIndex
                                                            .get(subWriterIndex)
                                                            .entrySet()) {
                                        Optional<?> commit;
                                        try {
                                            commit = sinkWriterEntry.getValue().prepareCommit();
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                        commit.ifPresent(
                                                o ->
                                                        multiTableCommitInfo
                                                                .getCommitInfo()
                                                                .put(sinkWriterEntry.getKey(), o));
                                    }
                                }
                            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (multiTableCommitInfo.getCommitInfo().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(multiTableCommitInfo);
    }

    //    private void flushMetrics(SinkWriter<SeaTunnelRow, ?, ?> writer) {
    //        if (writer instanceof SinkMetricsCalc) {
    //            taskMetricsCalcContext.collectMetrics(
    //                    ((SinkMetricsCalc) writer).collectMetricsContext());
    //            ((CycleMetricsContext)
    // temporaryTaskMetricsCalcContext.getMetricsContext()).clear();
    //        } else {
    //            taskMetricsCalcContext.collectMetrics(collectMetricsContext());
    //            ((CycleMetricsContext) collectMetricsContext()).clear();
    //        }
    //    }

    @Override
    public void abortPrepare() {
        Throwable firstE = null;
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE = e;
        }
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                for (SinkWriter<SeaTunnelRow, ?, ?> sinkWriter :
                        sinkWritersWithIndex.get(i).values()) {
                    try {
                        sinkWriter.abortPrepare();
                    } catch (Throwable e) {
                        if (firstE == null) {
                            firstE = e;
                        }
                        log.error("abortPrepare error", e);
                    }
                }
            }
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
        }
    }

    @Override
    public void close() throws IOException {
        // The variables used in lambda expressions should be final or valid final, so they are
        // modified to arrays

        //        sinkWritersWithIndex.forEach(
        //                sinkWriterMap -> {
        //                    sinkWriterMap.forEach(
        //                            (key, value) -> {
        //                                flushMetrics(value);
        //                            });
        //                });

        final Throwable[] firstE = {null};
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE[0] = e;
        }
        executorService.shutdownNow();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierSinkWriterMap =
                        sinkWritersWithIndex.get(i);
                sinkIdentifierSinkWriterMap.forEach(
                        (identifier, sinkWriter) -> {
                            try {
                                sinkWriter.close();
                                sinkWritersContext
                                        .get(identifier)
                                        .getEventListener()
                                        .onEvent(new WriterCloseEvent());
                            } catch (Throwable e) {
                                if (firstE[0] == null) {
                                    firstE[0] = e;
                                }
                                log.error("close error", e);
                            }
                        });
            }
        }
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        if (firstE[0] != null) {
            throw new RuntimeException(firstE[0]);
        }
    }

    private void checkQueueRemain() {
        try {
            for (BlockingQueue<SeaTunnelRow> blockingQueue : blockingQueues) {
                while (!blockingQueue.isEmpty()) {
                    Thread.sleep(100);
                    subSinkErrorCheck();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
