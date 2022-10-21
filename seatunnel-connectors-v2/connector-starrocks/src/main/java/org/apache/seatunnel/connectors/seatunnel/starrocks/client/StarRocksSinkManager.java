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

package org.apache.seatunnel.connectors.seatunnel.starrocks.client;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StarRocksSinkManager {

    private final SinkConfig sinkConfig;
    private final List<String> batchList;

    private StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private volatile boolean initialize;
    private volatile Exception flushException;

    public StarRocksSinkManager(SinkConfig sinkConfig, List<String> fileNames) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        starrocksStreamLoadVisitor = new StarRocksStreamLoadVisitor(sinkConfig, fileNames);
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }

        if (sinkConfig.getBatchIntervalMs() != null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("IoTDB-sink-output-%s").build());
            scheduledFuture = scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        flush();
                    } catch (IOException e) {
                        flushException = e;
                    }
                },
                    sinkConfig.getBatchIntervalMs(),
                    sinkConfig.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
        initialize = true;
    }

    public synchronized void write(String record) throws IOException {
        tryInit();
        checkFlushException();

        batchList.add(record);
        if (sinkConfig.getBatchSize() > 0
                && batchList.size() >= sinkConfig.getBatchSize()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduler.shutdown();
        }

        flush();
    }

    synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }

        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                starrocksStreamLoadVisitor.doStreamLoad(null);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                log.error("Writing records to IoTDB failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new IOException("Writing records to IoTDB failed.", e);
                }

                try {
                    long backoff = Math.min(sinkConfig.getRetryBackoffMultiplierMs() * i,
                            sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt.", e);
                }
            }
        }

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to IoTDB failed.", flushException);
        }
    }

    public String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(sinkConfig.getLabelPrefix())) {
            sb.append(sinkConfig.getLabelPrefix());
        }
        return sb.append(UUID.randomUUID().toString())
                .toString();
    }
}
