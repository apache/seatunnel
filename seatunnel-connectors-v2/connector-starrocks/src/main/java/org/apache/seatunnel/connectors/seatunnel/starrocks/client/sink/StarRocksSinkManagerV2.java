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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.LabelGenerator;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StreamLoadResponse;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.committer.StarRocksCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.state.StarRocksSinkState;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class StarRocksSinkManagerV2 implements StreamLoadManager {

    private Thread manager;
    private Thread current;
    private volatile Throwable flushException;
    private TableRegion tableRegion;
    private final long flushFrequencyMs;
    private SinkConfig sinkConfig;
    private TransactionStreamLoader streamLoader;
    protected long checkpointId = 0L;
    protected String transactionId;

    private final Lock lock = new ReentrantLock();

    private Condition writable = lock.newCondition();
    private Condition flushable = lock.newCondition();

    private volatile boolean savepoint;

    private final long maxCacheBytes;
    private long rowCountForOneTransaction;
    protected int subTaskIndex;
    private LabelGenerator labelGenerator;

    public StarRocksSinkManagerV2(
            LabelGenerator labelGenerator, SinkConfig sinkConfig, SinkWriter.Context context) {
        this.sinkConfig = sinkConfig;
        this.flushFrequencyMs = sinkConfig.getFlushFrequencyMs();
        this.maxCacheBytes = sinkConfig.getBatchMaxBytes();
        this.streamLoader = new TransactionStreamLoader(sinkConfig, this);
        this.tableRegion = new TransactionTableRegion(this, sinkConfig, streamLoader);
        this.subTaskIndex = context.getIndexOfSubtask();
        this.labelGenerator = labelGenerator;
        init();
    }

    @Override
    public void init() {
        this.manager =
                new Thread(
                        () -> {
                            while (Boolean.TRUE) {
                                lock.lock();
                                try {
                                    log.info("wait to trigger flush table region");
                                    flushable.await(flushFrequencyMs, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException interruptedException) {
                                    if (savepoint) {
                                        savepoint = false;
                                        LockSupport.unpark(current);
                                    }
                                    break;
                                } finally {
                                    lock.unlock();
                                }

                                if (savepoint) {
                                    boolean flushSuccess = tableRegion.flush();
                                    log.info(
                                            "trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                            tableRegion.getLabel(),
                                            tableRegion.getCacheBytes(),
                                            flushSuccess);
                                    LockSupport.unpark(current);

                                } else if (tableRegion.getCacheBytes() >= maxCacheBytes) {
                                    // when currentCacheBytes > maxCacheBytes to flush
                                    boolean flushSuccess = tableRegion.flush();
                                    log.info(
                                            "trigger flush table region {} because of selection, region cache bytes: {},"
                                                    + " flush: {}, max cache bytes: {}, ",
                                            tableRegion.getLabel(),
                                            tableRegion.getCacheBytes(),
                                            flushSuccess,
                                            maxCacheBytes);
                                }
                            }
                        });
        manager.setDaemon(true);
        manager.start();
        manager.setUncaughtExceptionHandler(
                (Thread t, Throwable e) -> {
                    log.error("StarRocks-Sink-ManagerV2 Error", e);
                    flushException = e;
                });
    }

    @Override
    public void write(String record) {
        tableRegion.write(record.getBytes(StandardCharsets.UTF_8));
        rowCountForOneTransaction++;
        int idx = 0;
        lock.lock();
        try {
            while (tableRegion.getCacheBytes() >= maxCacheBytes) {
                flushable.signal();
                log.info(
                        "sink manager v2 write loop maxCacheBytes: {}, tableRegin current cacheBytes: {}",
                        maxCacheBytes,
                        tableRegion.getCacheBytes());
                writable.await(Math.min(++idx, 5), TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            this.flushException = e;
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void callback(StreamLoadResponse response) {

        lock.lock();
        try {
            writable.signal();
        } finally {
            lock.unlock();
        }
        if (response.getException() != null) {
            log.error(
                    "Stream load failed, body : " + JsonUtils.toJsonString(response.getBody()),
                    response.getException());
            this.flushException = response.getException();
        }
    }

    @Override
    public void callback(Throwable e) {
        log.error("Stream load failed", e);
        this.flushException = e;
    }

    @Override
    public void flush() {
        log.info("Stream load manager flush");
        savepoint = true;
        current = Thread.currentThread();
        while (!isSavepointFinished()) {
            lock.lock();
            try {
                flushable.signal();
            } finally {
                lock.unlock();
            }
            LockSupport.park(current);
            if (!savepoint) {
                break;
            }
            try {
                Future<?> result = tableRegion.getResult();
                if (result != null) {
                    result.get();
                }
            } catch (ExecutionException | InterruptedException ex) {
                log.warn("Flush get result failed", ex);
            }
            savepoint = false;
        }
    }

    @Override
    public void beginTransaction(long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = labelGenerator.genLabel(checkpointId, subTaskIndex);
        log.info("begin transaction, transactionId: {}", transactionId);
        streamLoader.begin(transactionId);
        tableRegion.setLabel(transactionId);
    }

    @Override
    public ArrayList<StarRocksSinkState> snapshot(long checkpointId) {
        log.info("begin snapshot, checkpointId: {}", checkpointId);
        ArrayList<StarRocksSinkState> starRocksSinkStates =
                Lists.newArrayList(
                        new StarRocksSinkState(
                                sinkConfig.getLabelPrefix(), checkpointId, subTaskIndex));
        beginTransaction(checkpointId + 1);
        return starRocksSinkStates;
    }

    @Override
    public Optional<StarRocksCommitInfo> prepareCommit() {
        log.info("begin prepareCommit, transactionId: {}", transactionId);
        flush();

        if (rowCountForOneTransaction == 0) {
            log.info(
                    "no data write to current transaction, rollback transactionId: {}",
                    transactionId);
            streamLoader.rollback(transactionId);
            return Optional.empty();
        }
        rowCountForOneTransaction = 0;

        if (streamLoader.prepare(transactionId)) {
            return Optional.of(
                    new StarRocksCommitInfo(
                            sinkConfig.getDatabase(), sinkConfig.getTable(), transactionId));
        } else {
            streamLoader.rollback(transactionId);
            throw new RuntimeException("snapshot state failed by prepare");
        }
    }

    @Override
    public boolean commit(String transactionId) {
        log.info("begin commit, transactionId: {}", transactionId);
        return streamLoader.commit(transactionId);
    }

    @Override
    public boolean abort() throws Exception {
        log.info(
                "begin abortPreCommit, checkpointId: {}, subTaskIndex: {}",
                checkpointId,
                subTaskIndex);
        streamLoader.abortPreCommit(checkpointId, subTaskIndex);
        return true;
    }

    @Override
    public boolean abort(long checkpointId, int subTaskIndex) throws Exception {
        log.info("begin abort, transactionId: {}", transactionId);
        streamLoader.abortPreCommit(checkpointId, subTaskIndex);
        return true;
    }

    @Override
    public void close() {
        log.info("begin close");
        manager.interrupt();
    }

    private boolean isSavepointFinished() {
        return tableRegion.getCacheBytes() == 0L;
    }
}
