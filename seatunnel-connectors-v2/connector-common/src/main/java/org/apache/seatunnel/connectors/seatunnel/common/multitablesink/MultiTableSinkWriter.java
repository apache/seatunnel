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

package org.apache.seatunnel.connectors.seatunnel.common.multitablesink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MultiTableSinkWriter
        implements SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> {

    private final Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters;
    private final Map<String, Optional<Integer>> sinkPrimaryKeys = new HashMap<>();
    private final List<Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>> sinkWritersWithIndex;
    private final List<MultiTableWriterRunnable> runnable = new ArrayList<>();
    private final Random random = new Random();
    private final List<BlockingQueue<SeaTunnelRow>> blockingQueues = new ArrayList<>();
    private final ExecutorService executorService;
    private Optional<? extends MultiTableResourceManager<?>> resourceManager = Optional.empty();
    private volatile boolean submitted = false;

    public MultiTableSinkWriter(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters, int queueSize) {
        this.sinkWriters = sinkWriters;
        executorService =
                Executors.newFixedThreadPool(
                        queueSize,
                        runnable -> {
                            AtomicInteger cnt = new AtomicInteger(0);
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            thread.setName(
                                    "st-multi-table-sink-writer" + "-" + cnt.incrementAndGet());
                            return thread;
                        });
        sinkWritersWithIndex = new ArrayList<>();
        for (int i = 0; i < queueSize; i++) {
            BlockingQueue<SeaTunnelRow> queue = new LinkedBlockingQueue<>(1024);
            Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierMap = new HashMap<>();
            int finalI = i;
            sinkWriters.entrySet().stream()
                    .filter(entry -> entry.getKey().getIndex() % queueSize == finalI)
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
                sink.setMultiTableResourceManager((Optional) resourceManager, i);
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
        MultiTableCommitInfo multiTableCommitInfo = new MultiTableCommitInfo(new HashMap<>());
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            for (Map.Entry<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriterEntry :
                    sinkWritersWithIndex.get(i).entrySet()) {
                synchronized (runnable.get(i)) {
                    Optional<?> commit = sinkWriterEntry.getValue().prepareCommit();
                    commit.ifPresent(
                            o ->
                                    multiTableCommitInfo
                                            .getCommitInfo()
                                            .put(sinkWriterEntry.getKey(), o));
                }
            }
        }
        return Optional.of(multiTableCommitInfo);
    }

    @Override
    public void abortPrepare() {
        checkQueueRemain();
        Throwable firstE = null;
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
        checkQueueRemain();
        executorService.shutdownNow();
        Throwable firstE = null;
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                for (SinkWriter<SeaTunnelRow, ?, ?> sinkWriter :
                        sinkWritersWithIndex.get(i).values()) {
                    try {
                        sinkWriter.close();
                    } catch (Throwable e) {
                        if (firstE == null) {
                            firstE = e;
                        }
                        log.error("close error", e);
                    }
                }
            }
        }
        try {
            resourceManager.ifPresent(MultiTableResourceManager::close);
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        if (firstE != null) {
            throw new RuntimeException(firstE);
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
