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

package org.apache.seatunnel.connectors.doris.client;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.doris.config.DorisSinkSemantics;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DorisSinkManager {

    private final SinkConfig sinkConfig;
    private final List<byte[]> batchList;

    private final DorisStreamLoadVisitor dorisStreamLoadVisitor;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private volatile boolean initialize;
    private volatile Exception flushException;
    private int batchRowCount = 0;
    private long batchBytesSize = 0;
    AtomicInteger integer = new AtomicInteger(0);
    private static final long FLUSH_QUEUE_POLL_TIMEOUT = 3000;

    private final Integer batchIntervalMs;
    private final LinkedBlockingDeque<DorisFlushTuple> flushQueue = new LinkedBlockingDeque<>(1);
    private DorisFlushTuple currentSinkBuffer = null;
    private volatile boolean asyncFlushThreadAlive = false;
    private final Map<String, DorisFlushTuple> previousBufferMap = new ConcurrentHashMap<>();
    private final DorisSinkSemantics sinkSemantics;

    public DorisSinkManager(SinkConfig sinkConfig, List<String> fileNames) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        this.batchIntervalMs = sinkConfig.getBatchIntervalMs();
        dorisStreamLoadVisitor = new DorisStreamLoadVisitor(sinkConfig, fileNames);
        sinkSemantics = sinkConfig.getDorisSinkSemantic();
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        initialize = true;

        scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("doris-sink-output-%s").build());
        scheduledFuture = scheduler.scheduleAtFixedRate(() -> {
            try {
                flush();
            } catch (IOException e) {
                flushException = e;
            }
        }, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void startAsyncFlushing() {
        // start flush thread
        Thread flushThread = new Thread(() -> {
            while (true) {
                try {
                    if (!asyncFlush()) {
                        log.info("doris flush thread is about to exit.");
                        asyncFlushThreadAlive = false;
                        break;
                    }
                } catch (Exception e) {
                    flushException = e;
                }
            }
        });

        flushThread.setUncaughtExceptionHandler((t, e) -> {
            log.error("doris flush thread uncaught exception occurred: " + e.getMessage(), e);
            flushException = (Exception) e;
            asyncFlushThreadAlive = false;
        });
        flushThread.setName("doris-flush");
        flushThread.setDaemon(true);
        flushThread.start();
        asyncFlushThreadAlive = true;
    }

    private boolean asyncFlush() throws Exception {
        DorisFlushTuple flushData = flushQueue.poll(FLUSH_QUEUE_POLL_TIMEOUT, TimeUnit.MILLISECONDS);

        if (flushData == null || (0 == flushData.getBytes() && !flushData.isEof())) {
            return true;
        }
        if (flushData.isEof()) {
            return false;
        }
        flush(flushData);
        log.info(String.format("Async stream load finished: label[%s].", flushData.getLabel()));
        //remove finished flushData from previousBufferMap
        previousBufferMap.remove(flushData.getLabel());

        return true;
    }

    public synchronized void write(String record) throws IOException {
        checkFlushException();
        byte[] bts = record.getBytes(StandardCharsets.UTF_8);

        if (sinkSemantics.equals(DorisSinkSemantics.AT_LEAST_ONCE)) {
            if (currentSinkBuffer == null) {
                String newLabel = createBatchLabel();
                currentSinkBuffer = new DorisFlushTuple(newLabel);
            }
            currentSinkBuffer.addToBuffer(bts);
            return;
        }

        tryInit();
        batchList.add(bts);
        batchRowCount++;
        batchBytesSize += bts.length;
        if (batchRowCount >= sinkConfig.getBatchMaxSize() || batchBytesSize >= sinkConfig.getBatchMaxBytes()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        log.info("DorisSinkManager close");

        if (sinkSemantics.equals(DorisSinkSemantics.AT_LEAST_ONCE)) {
            //draining the remaining buffer
            flushPreviousBuffer(true);
            //close async flush thread
            offerEOF();
            asyncFlushThreadAlive = false;
            return;
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduler.shutdown();
        }

        flush();

    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        String label = createBatchLabel();
        DorisFlushTuple tuple = new DorisFlushTuple(label, batchBytesSize, batchList);
        loadFlushTuple(tuple);
        batchList.clear();
        batchRowCount = 0;
        batchBytesSize = 0;
    }

    public void flush(DorisFlushTuple tuple) throws IOException {
        checkFlushException();
        loadFlushTuple(tuple);
    }

    public void loadFlushTuple(DorisFlushTuple tuple) throws IOException {
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                Boolean successFlag = dorisStreamLoadVisitor.doStreamLoad(tuple);
                if (successFlag) {
                    break;
                }
            } catch (Exception e) {
                log.warn("Writing records to Doris failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new DorisConnectorException(DorisConnectorErrorCode.WRITE_RECORDS_FAILED, "The number of retries was exceeded,writing records to Doris failed.", e);
                }

                if (e instanceof DorisConnectorException && ((DorisConnectorException) e).needReCreateLabel()) {
                    String newLabel = createBatchLabel();
                    log.warn(String.format("Batch label changed from [%s] to [%s]", tuple.getLabel(), newLabel));
                    tuple.setLabel(newLabel);
                }

                try {
                    long backoff = Math.min(sinkConfig.getRetryBackoffMultiplierMs() * i,
                            sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new DorisConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                            "Unable to flush, interrupted while doing another attempt.", e);
                }
            }
        }

    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new DorisConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, flushException);
        }
    }

    public String createBatchLabel() {
        String labelPrefix = "";
        if (!Strings.isNullOrEmpty(sinkConfig.getLabelPrefix())) {
            labelPrefix = sinkConfig.getLabelPrefix();
        }
        return String.format("%s%s", labelPrefix, UUID.randomUUID().toString());
    }

    void offer(DorisFlushTuple bufferEntity) {
        if (!asyncFlushThreadAlive) {
            log.info(String.format("Flush thread already exit, ignore offer request for label[%s]", bufferEntity.getLabel()));
            return;
        }
        try {
            if (!flushQueue.offer(bufferEntity, sinkConfig.getSinkBufferEnqueueTimeoutMs(), TimeUnit.MILLISECONDS)) {
                throw new DorisConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, flushException);
            }
        } catch (Exception e) {
            throw new DorisConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                    String.format("Timeout while offering data to flushQueue, exceed  %s ms", sinkConfig.getSinkBufferEnqueueTimeoutMs()), e);
        }

    }

    private void offerEOF() {
        try {
            //close async flush thread
            offer(new DorisFlushTuple().asEOF());
        } catch (Exception e) {
            log.warn("Writing EOF failed.", e);
        }
    }

    private void waitAsyncFlushingDone() {
        // offer two empty flushTuple to flushQueue to wait for previous flush finished
        offer(new DorisFlushTuple());
        offer(new DorisFlushTuple());
        checkFlushException();
    }

    public synchronized void flushPreviousBuffer(boolean waitUtilDone)  {
        if (previousBufferMap.isEmpty()) {
            flushInternal(null, waitUtilDone);
            return;
        }

        for (String key : previousBufferMap.keySet()) {
            flushInternal(key, waitUtilDone);
        }
    }

    private synchronized void flushInternal(String bufferKey, boolean waitUtilDone) {
        checkFlushException();
        if (null == bufferKey || previousBufferMap.isEmpty() || !previousBufferMap.containsKey(bufferKey)) {
            if (waitUtilDone) {
                waitAsyncFlushingDone();
            }
            return;
        }
        offer(previousBufferMap.get(bufferKey));
        previousBufferMap.remove(bufferKey);
        if (waitUtilDone) {
            // wait the last flush
            waitAsyncFlushingDone();
        }
    }

    public void setBufferedBatchMap(Map<String, DorisFlushTuple> bufferMap) {
        this.previousBufferMap.clear();
        this.previousBufferMap.putAll(bufferMap);
    }

    public Map<String, DorisFlushTuple> getPreviousBufferMap() {
        Map<String, DorisFlushTuple> clone = new HashMap<>();
        clone.putAll(previousBufferMap);
        return clone;
    }

    public void addBufferedBatchMap(DorisFlushTuple buffer) {
        if (null != buffer) {
            this.previousBufferMap.put(buffer.getLabel(), buffer);
        }
    }

    public DorisFlushTuple getCurrentSinkBuffer() {
        return currentSinkBuffer;
    }

    public void resetCurrentSinkBuffer() {
        this.currentSinkBuffer = null;
    }
}
