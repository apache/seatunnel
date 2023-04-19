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

import org.apache.seatunnel.common.utils.JsonUtils;
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
import java.util.concurrent.atomic.AtomicLong;
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
    private final Integer batchIntervalMs;
    private SinkConfig sinkConfig;
    private TransactionStreamLoader streamLoader;
    protected Long checkpointId = 0L;
    protected String transactionId;

    public StarRocksSinkManagerV2(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.batchIntervalMs = sinkConfig.getBatchIntervalMs();
        this.maxCacheBytes = sinkConfig.getBatchMaxBytes();
        this.streamLoader = new TransactionStreamLoader(sinkConfig);
    }

    private final Lock lock = new ReentrantLock();

    private Condition writable = lock.newCondition();
    private Condition flushable = lock.newCondition();

    private volatile boolean savepoint;

    private final long maxCacheBytes;
    private final AtomicLong currentCacheBytes = new AtomicLong(0L);

    @Override
    public void init() {
        this.manager =
                new Thread(
                        () -> {
                            while (Boolean.TRUE) {
                                lock.lock();
                                try {
                                    flushable.await(batchIntervalMs, TimeUnit.MILLISECONDS);
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
                                    log.debug(
                                            "Trigger flush table region {} because of savepoint, region cache bytes: {}, flush: {}",
                                            tableRegion.getCacheBytes(),
                                            flushSuccess);
                                    LockSupport.unpark(current);

                                } else if (currentCacheBytes.get() >= maxCacheBytes) {
                                    // when currentCacheBytes > maxCacheBytes to flush
                                    boolean flushSuccess = tableRegion.flush();
                                    log.debug(
                                            "Trigger flush table region {} because of selection, region cache bytes: {},"
                                                    + " flush: {}",
                                            tableRegion.getCacheBytes(),
                                            flushSuccess);
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
        TableRegion region = getRegion();
        int bytes = region.write(record.getBytes(StandardCharsets.UTF_8));
        long cacheBytes = currentCacheBytes.addAndGet(bytes);
        int idx = 0;
        lock.lock();
        try {
            while (cacheBytes >= maxCacheBytes) {
                flushable.signal();
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

    public void beginTransaction(long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = streamLoader.genLabel(checkpointId);
        this.tableRegion.setLabel(transactionId);
    }

    @Override
    public ArrayList<StarRocksSinkState> snapshot(long checkpointId) {
        ArrayList<StarRocksSinkState> starRocksSinkStates =
                Lists.newArrayList(new StarRocksSinkState(tableRegion.getLabel(), checkpointId));
        beginTransaction(checkpointId);
        return starRocksSinkStates;
    }

    @Override
    public Optional<StarRocksCommitInfo> prepareCommit() {
        flush();
        streamLoader.prepare(transactionId);
        return Optional.of(
                new StarRocksCommitInfo(
                        sinkConfig.getDatabase(), sinkConfig.getTable(), transactionId));
    }

    @Override
    public boolean commit(long checkpointId) {
        return streamLoader.commit(streamLoader.genLabel(checkpointId));
    }

    @Override
    public boolean abort() {
        return streamLoader.rollback(streamLoader.genLabel(checkpointId));
    }

    @Override
    public void close() {
        manager.interrupt();
    }

    protected TableRegion getRegion() {
        if (tableRegion == null) {
            tableRegion = new TransactionTableRegion(this, sinkConfig, streamLoader);
        }
        return tableRegion;
    }

    private boolean isSavepointFinished() {
        return currentCacheBytes.compareAndSet(0L, 0L);
    }
}
