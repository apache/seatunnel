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

import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class StarRocksSinkManager {

    private final SinkConfig sinkConfig;
    private final List<byte[]> batchList;

    private final StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;

    /**
     * Snapshot whose outcome is not confirmed yet. Its rows and label must survive failed flush
     * calls so a later retry cannot bypass StarRocks label-based idempotency.
     */
    private StarRocksFlushTuple pendingFlush;

    private volatile boolean initialize;
    private volatile Exception flushException;
    private int batchRowCount = 0;
    private long batchBytesSize = 0;

    public StarRocksSinkManager(SinkConfig sinkConfig, TableSchema tableSchema) {
        this(sinkConfig, tableSchema, new StarRocksStreamLoadVisitor(sinkConfig, tableSchema));
    }

    StarRocksSinkManager(
            SinkConfig sinkConfig,
            TableSchema tableSchema,
            StarRocksStreamLoadVisitor streamLoadVisitor) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        starrocksStreamLoadVisitor = streamLoadVisitor;
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        initialize = true;
    }

    /**
     * Buffers a record after resolving any previously failed flush.
     *
     * <p>A new record cannot join a pending snapshot because clearing that snapshot after a retry
     * would otherwise clear a record that was never sent.
     */
    public synchronized void write(String record) throws IOException {
        tryInit();
        checkFlushException();
        if (pendingFlush != null) {
            flush();
        }
        byte[] bts = record.getBytes(StandardCharsets.UTF_8);
        batchList.add(bts);
        batchRowCount++;
        batchBytesSize += bts.length;
        if (batchRowCount >= sinkConfig.getBatchMaxSize()
                || batchBytesSize >= sinkConfig.getBatchMaxBytes()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        flush();
    }

    /**
     * Flushes buffered rows and releases them only after StarRocks confirms success or commit.
     *
     * <p>Failed or unknown outcomes retain both the batch snapshot and its label across calls. The
     * label changes only when StarRocks explicitly reports that the previous transaction aborted.
     */
    public synchronized void flush() throws IOException {
        checkFlushException();
        if (pendingFlush == null) {
            if (batchList.isEmpty()) {
                return;
            }
            pendingFlush =
                    new StarRocksFlushTuple(
                            createBatchLabel(), batchBytesSize, new ArrayList<>(batchList));
        }
        StarRocksFlushTuple tuple = pendingFlush;
        boolean loadSucceeded = false;
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                Boolean successFlag = starrocksStreamLoadVisitor.doStreamLoad(tuple);
                if (Boolean.TRUE.equals(successFlag)) {
                    loadSucceeded = true;
                    break;
                }
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                        String.format(
                                "Stream Load returned a non-success result for %s.%s with label [%s].",
                                sinkConfig.getDatabase(), sinkConfig.getTable(), tuple.getLabel()));
            } catch (Exception e) {
                log.warn("Writing records to StarRocks failed, retry times = {}", i, e);

                if (i >= sinkConfig.getMaxRetries()) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "The number of retries was exceeded, writing records to StarRocks failed.",
                            e);
                }

                if (e instanceof StarRocksConnectorException
                        && ((StarRocksConnectorException) e).needReCreateLabel()) {
                    String newLabel = createBatchLabel();
                    log.warn(
                            String.format(
                                    "Batch label changed from [%s] to [%s]",
                                    tuple.getLabel(), newLabel));
                    tuple.setLabel(newLabel);
                }

                try {
                    long backoff =
                            Math.min(
                                    sinkConfig.getRetryBackoffMultiplierMs() * i,
                                    sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED,
                            "Interrupted while waiting to retry Stream Load.",
                            ex);
                }
            }
        }
        if (!loadSucceeded) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.WRITE_RECORDS_FAILED,
                    "Stream Load did not complete successfully; buffered records were retained.");
        }
        // Buffered rows can only be released after StarRocks confirms a successful or committed
        // load.
        pendingFlush = null;
        batchList.clear();
        batchRowCount = 0;
        batchBytesSize = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, flushException);
        }
    }

    public String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(sinkConfig.getLabelPrefix())) {
            sb.append(sinkConfig.getLabelPrefix());
        }
        return sb.append(UUID.randomUUID()).toString();
    }
}
