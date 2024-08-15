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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.cache.RemovalCause.EXPIRED;

@Slf4j
public class MultiTableSinkWriter
        implements SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> {

    private final List<Cache<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>>> sinkWritersWithIndex;
    private final List<MultiTableWriterRunnable> runnable = new ArrayList<>();
    private final Random random = new Random();
    private final List<BlockingQueue<SeaTunnelRow>> blockingQueues = new ArrayList<>();
    private final ExecutorService executorService;
    private MultiTableResourceManager resourceManager;
    private volatile boolean submitted = false;
    private volatile boolean firstTimeCreatedWriter = true;
    private final int queueSize;
    private final SinkWriter.Context context;
    private final Map<String, SeaTunnelSink> sinks;

    public MultiTableSinkWriter(
            Map<String, SeaTunnelSink> sinks,
            SinkWriter.Context context,
            ReadonlyConfig config,
            List<MultiTableState> states)
            throws IOException {
        this.sinks = sinks;
        AtomicInteger cnt = new AtomicInteger(0);
        this.queueSize = config.get(SinkCommonOptions.MULTI_TABLE_SINK_REPLICA);
        this.context = context;
        executorService =
                Executors.newFixedThreadPool(
                        // we use it in `MultiTableWriterRunnable` and `prepare commit task`, so it
                        // should be double.
                        queueSize * 2,
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            thread.setName(
                                    "st-multi-table-sink-writer" + "-" + cnt.incrementAndGet());
                            return thread;
                        });
        sinkWritersWithIndex = new CopyOnWriteArrayList<>();
        for (int i = 0; i < queueSize; i++) {
            BlockingQueue<SeaTunnelRow> queue = new LinkedBlockingQueue<>(1024);
            Cache<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkIdentifierCache =
                    CacheBuilder.newBuilder()
                            .expireAfterAccess(
                                    config.get(SinkCommonOptions.MULTI_TABLE_SINK_WRITER_TTL),
                                    TimeUnit.MINUTES)
                            .removalListener(
                                    writer -> {
                                        if (writer.getCause().equals(EXPIRED)) {
                                            try {
                                                ((SinkWriter<SeaTunnelRow, ?, ?>) writer.getValue())
                                                        .close();
                                            } catch (IOException e) {
                                                log.error("close writer error", e);
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    })
                            .build();
            sinkWritersWithIndex.add(sinkIdentifierCache);
            for (MultiTableState state : states) {
                for (Map.Entry<SinkIdentifier, List<?>> entry : state.getStates().entrySet()) {
                    List<?> stateList =
                            entry.getValue().stream()
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                    if (!stateList.isEmpty() && entry.getKey().getIndex() % queueSize == i) {
                        SeaTunnelSink sink = sinks.get(entry.getKey().getTableIdentifier());
                        SinkWriter<SeaTunnelRow, ?, ?> writer =
                                sink.restoreWriter(
                                        new SinkContextProxy(entry.getKey().getIndex(), context),
                                        stateList);
                        sinkIdentifierCache.put(entry.getKey(), writer);
                    }
                }
            }
            blockingQueues.add(queue);
            MultiTableWriterRunnable r = new MultiTableWriterRunnable(i, queue, this, sinks.size());
            runnable.add(r);
        }
    }

    SinkWriter<SeaTunnelRow, ?, ?> getWriter(String tableIdentifier, int queueIndex)
            throws IOException {
        int index = context.getIndexOfSubtask() * queueSize + queueIndex;
        SinkIdentifier identifier = SinkIdentifier.of(tableIdentifier, index);
        SinkWriter<SeaTunnelRow, ?, ?> writer =
                sinkWritersWithIndex.get(queueIndex).getIfPresent(identifier);
        if (writer != null) {
            return writer;
        }
        SeaTunnelSink sink;
        if (StringUtils.isNotEmpty(tableIdentifier)) {
            sink = sinks.get(tableIdentifier);
            if (sink == null && sinks.size() == 1) {
                sink = sinks.values().stream().findFirst().get();
            }
        } else {
            sink = sinks.values().stream().findFirst().get();
        }
        if (sink == null) {
            throw new RuntimeException(
                    "MultiTableWriterRunnable can't find writer for tableId: " + tableIdentifier);
        }
        writer = sink.createWriter(new SinkContextProxy(index, context));
        synchronized (this) {
            if (firstTimeCreatedWriter) {
                firstTimeCreatedWriter = false;
                log.info("init multi table sink writer, queue size: {}", queueSize);
                // first sink
                resourceManager =
                        ((SupportMultiTableSinkWriter<?>) writer)
                                .initMultiTableResourceManager(sinks.size() * queueSize, queueSize);
            }
        }
        sinkWritersWithIndex.get(queueIndex).put(identifier, writer);
        SupportMultiTableSinkWriter<?> supportMultiTableSinkWriter =
                ((SupportMultiTableSinkWriter<?>) writer);
        supportMultiTableSinkWriter.setMultiTableResourceManager(resourceManager, queueIndex);
        return writer;
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
                    sinkWritersWithIndex.get(i).asMap().entrySet()) {
                if (sinkWriterEntry
                        .getKey()
                        .getTableIdentifier()
                        .equals(event.tablePath().getFullName())) {
                    synchronized (runnable.get(i)) {
                        sinkWriterEntry.getValue().applySchemaChange(event);
                    }
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
        SeaTunnelSink sink = sinks.get(element.getTableId());
        Optional<Integer> primaryKey =
                sink != null ? ((SupportMultiTableSink) sink).primaryKey() : Optional.empty();
        try {
            if ((sink == null && sinks.size() == 1) || (sink != null && !primaryKey.isPresent())) {
                int index = random.nextInt(blockingQueues.size());
                BlockingQueue<SeaTunnelRow> queue = blockingQueues.get(index);
                while (!queue.offer(element, 500, TimeUnit.MILLISECONDS)) {
                    subSinkErrorCheck();
                }
            } else if (sink == null) {
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
                    sinkWritersWithIndex.get(i).asMap().entrySet()) {
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
                                                            .asMap()
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
        return Optional.of(multiTableCommitInfo);
    }

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
                        sinkWritersWithIndex.get(i).asMap().values()) {
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
        Throwable firstE = null;
        try {
            checkQueueRemain();
        } catch (Exception e) {
            firstE = e;
        }
        executorService.shutdownNow();
        for (int i = 0; i < sinkWritersWithIndex.size(); i++) {
            synchronized (runnable.get(i)) {
                sinkWritersWithIndex.get(i).cleanUp();
                for (SinkWriter<SeaTunnelRow, ?, ?> sinkWriter :
                        sinkWritersWithIndex.get(i).asMap().values()) {
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
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        for (Cache<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> cache : sinkWritersWithIndex) {
            cache.invalidateAll();
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
