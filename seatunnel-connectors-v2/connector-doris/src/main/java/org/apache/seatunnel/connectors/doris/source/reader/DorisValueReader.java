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

package org.apache.seatunnel.connectors.doris.source.reader;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.doris.backend.BackendClient;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.rest.models.Schema;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.serialization.Routing;
import org.apache.seatunnel.connectors.doris.source.serialization.RowBatch;
import org.apache.seatunnel.connectors.doris.util.SchemaUtils;

import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_DEFAULT_CLUSTER;
import static org.apache.seatunnel.connectors.doris.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

@Slf4j
public class DorisValueReader {

    protected BackendClient client;
    protected Lock clientLock = new ReentrantLock();

    private PartitionDefinition partition;
    private DorisSourceTable dorisSourceTable;
    private DorisSourceConfig config;

    protected int offset = 0;
    protected AtomicBoolean eos = new AtomicBoolean(false);
    protected RowBatch rowBatch;

    // flag indicate if support deserialize Arrow to RowBatch asynchronously
    protected boolean deserializeArrowToRowBatchAsync;

    protected BlockingQueue<RowBatch> rowBatchBlockingQueue;
    private TScanOpenParams openParams;
    protected String contextId;
    protected Schema schema;

    protected SeaTunnelRowType seaTunnelRowType;
    protected boolean asyncThreadStarted;

    public DorisValueReader(
            PartitionDefinition partition,
            DorisSourceConfig config,
            DorisSourceTable dorisSourceTable) {
        this.partition = partition;
        this.config = config;
        this.dorisSourceTable = dorisSourceTable;
        this.client = backendClient();
        this.deserializeArrowToRowBatchAsync = config.getDeserializeArrowAsync();
        this.seaTunnelRowType = dorisSourceTable.getCatalogTable().getSeaTunnelRowType();
        int blockingQueueSize = config.getDeserializeQueueSize();
        if (this.deserializeArrowToRowBatchAsync) {
            this.rowBatchBlockingQueue = new ArrayBlockingQueue<>(blockingQueueSize);
        }
        init();
    }

    private void init() {
        clientLock.lock();
        try {
            this.openParams = openParams();
            TScanOpenResult openResult = this.client.openScanner(this.openParams);
            this.contextId = openResult.getContextId();
            this.schema = SchemaUtils.convertToSchema(openResult.getSelectedColumns());
        } finally {
            clientLock.unlock();
        }
        this.asyncThreadStarted = asyncThreadStarted();
        log.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
    }

    private BackendClient backendClient() {
        try {
            return new BackendClient(new Routing(partition.getBeAddress()), config);
        } catch (IllegalArgumentException e) {
            log.error("init backend:{} client failed,", partition.getBeAddress(), e);
            throw new DorisConnectorException(DorisConnectorErrorCode.BACKEND_CLIENT_FAILED, e);
        }
    }

    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.cluster = DORIS_DEFAULT_CLUSTER;
        params.database = partition.getDatabase();
        params.table = partition.getTable();

        params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[] {}));
        params.opaqued_query_plan = partition.getQueryPlan();
        // max row number of one read batch
        Integer batchSize = dorisSourceTable.getBatchSize();
        Integer queryDorisTimeout = config.getRequestQueryTimeoutS();
        Long execMemLimit = dorisSourceTable.getExecMemLimit();
        params.setBatchSize(batchSize);
        params.setQueryTimeout(queryDorisTimeout);
        params.setMemLimit(execMemLimit);
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword());
        log.debug(
                "Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
                params.getCluster(),
                params.getDatabase(),
                params.getTable(),
                params.getTabletIds(),
                params.getBatchSize(),
                params.getQueryTimeout(),
                params.getMemLimit(),
                params.getUser(),
                params.getOpaquedQueryPlan());
        return params;
    }

    protected Thread asyncThread =
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            clientLock.lock();
                            try {
                                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                                nextBatchParams.setContextId(contextId);
                                while (!eos.get()) {
                                    nextBatchParams.setOffset(offset);
                                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                                    eos.set(nextResult.isEos());
                                    if (!eos.get()) {
                                        RowBatch rowBatch =
                                                new RowBatch(nextResult, seaTunnelRowType)
                                                        .readArrow();
                                        offset += rowBatch.getReadRowCount();
                                        rowBatch.close();
                                        try {
                                            rowBatchBlockingQueue.put(rowBatch);
                                        } catch (InterruptedException e) {
                                            throw new DorisConnectorException(
                                                    DorisConnectorErrorCode.ROW_BATCH_GET_FAILED,
                                                    e);
                                        }
                                    }
                                }
                            } finally {
                                clientLock.unlock();
                            }
                        }
                    });

    protected boolean asyncThreadStarted() {
        boolean started = false;
        if (deserializeArrowToRowBatchAsync) {
            asyncThread.start();
            started = true;
        }
        return started;
    }

    /**
     * read data and cached in rowBatch.
     *
     * @return true if hax next value
     */
    public boolean hasNext() {
        boolean hasNext = false;
        if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
            // support deserialize Arrow to RowBatch asynchronously
            if (rowBatch == null || !rowBatch.hasNext()) {
                while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
                    if (!rowBatchBlockingQueue.isEmpty()) {
                        try {
                            rowBatch = rowBatchBlockingQueue.take();
                        } catch (InterruptedException e) {
                            throw new DorisConnectorException(
                                    DorisConnectorErrorCode.ROW_BATCH_GET_FAILED, e);
                        }
                        hasNext = true;
                        break;
                    } else {
                        // wait for rowBatch put in queue or eos change
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            } else {
                hasNext = true;
            }
        } else {
            clientLock.lock();
            try {
                // Arrow data was acquired synchronously during the iterative process
                if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                    if (rowBatch != null) {
                        offset += rowBatch.getReadRowCount();
                        rowBatch.close();
                    }
                    TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                    nextBatchParams.setContextId(contextId);
                    nextBatchParams.setOffset(offset);
                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                    eos.set(nextResult.isEos());
                    if (!eos.get()) {
                        rowBatch = new RowBatch(nextResult, seaTunnelRowType).readArrow();
                    }
                }
                hasNext = !eos.get();
            } finally {
                clientLock.unlock();
            }
        }
        return hasNext;
    }

    /**
     * get next value.
     *
     * @return next value
     */
    public SeaTunnelRow next() {
        if (!hasNext()) {
            log.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.SHOULD_NEVER_HAPPEN, "never happen error.");
        }
        SeaTunnelRow next = rowBatch.next();
        next.setTableId(dorisSourceTable.getTablePath().toString());
        return next;
    }

    public void close() {
        clientLock.lock();
        try {
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContextId(contextId);
            client.closeScanner(closeParams);
        } catch (Exception e) {
            log.error("Failed to close reader with context id {}", contextId, e);
            throw new DorisConnectorException(DorisConnectorErrorCode.RESOURCE_CLOSE_FAILED, e);
        } finally {
            clientLock.unlock();
        }
    }
}
