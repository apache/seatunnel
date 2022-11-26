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

package org.apache.seatunnel.connectors.seatunnel.tablestore.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.exception.TablestoreConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.tablestore.exception.TablestoreConnectorException;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TablestoreSinkClient {
    private final TablestoreOptions tablestoreOptions;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private volatile boolean initialize;
    private volatile Exception flushException;
    private SyncClient syncClient;
    private final List<RowPutChange> batchList;

    public TablestoreSinkClient(TablestoreOptions tablestoreOptions, SeaTunnelRowType typeInfo) {
        this.tablestoreOptions = tablestoreOptions;
        this.batchList = new ArrayList<>();
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        syncClient = new SyncClient(
            tablestoreOptions.getEndpoint(),
            tablestoreOptions.getAccessKeyId(),
            tablestoreOptions.getAccessKeySecret(),
            tablestoreOptions.getInstanceName());

        scheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("Tablestore-sink-output-%s").build());
        scheduledFuture = scheduler.scheduleAtFixedRate(
            () -> {
                try {
                    flush();
                } catch (IOException e) {
                    flushException = e;
                }
            },
            tablestoreOptions.getBatchIntervalMs(),
            tablestoreOptions.getBatchIntervalMs(),
            TimeUnit.MILLISECONDS);

        initialize = true;
    }

    public void write(RowPutChange rowPutChange) throws IOException {
        tryInit();
        checkFlushException();
        batchList.add(rowPutChange);
        if (tablestoreOptions.getBatchSize() > 0
            && batchList.size() >= tablestoreOptions.getBatchSize()) {
            flush();
        }
    }

    public void close() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduler.shutdown();
        }
        if (syncClient != null) {
            flush();
            syncClient.shutdown();
        }
    }

    synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        batchList.forEach(batchWriteRowRequest::addRowChange);
        BatchWriteRowResponse response = syncClient.batchWriteRow(batchWriteRowRequest);

        if (!response.isAllSucceed()) {
            throw new TablestoreConnectorException(TablestoreConnectorErrorCode.WRITE_ROW_FAILED,
                String.format("Failed to send these rows of data: '%s'.", response.getFailedRows()));
        }

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new TablestoreConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                    "Writing items to Tablestore failed.", flushException);
        }
    }

}
