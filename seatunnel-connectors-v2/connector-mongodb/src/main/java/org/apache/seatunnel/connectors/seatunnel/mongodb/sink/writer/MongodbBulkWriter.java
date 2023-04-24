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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbSinkState;

import org.bson.BsonDocument;

import com.mongodb.MongoException;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.seatunnel.common.exception.CommonErrorCode.WRITER_OPERATION_FAILED;

@Slf4j
public class MongodbBulkWriter
        implements SinkWriter<SeaTunnelRow, MongodbCommitInfo, MongodbSinkState> {

    private MongoClientProvider collectionProvider;

    private final DocumentSerializer<SeaTunnelRow> serializer;

    private long bulkActions;

    private List<WriteModel<BsonDocument>> bulkRequests;

    private int maxRetries;

    private long retryIntervalMs;

    private long batchIntervalMs;

    private boolean transactionEnable;

    private volatile long lastSendTime = 0L;

    public MongodbBulkWriter(
            DocumentSerializer<SeaTunnelRow> serializer,
            MongodbWriterOptions options,
            List<MongodbSinkState> mongodbSinkState) {
        super();
        initOptions(options);
        initBulkRequests(mongodbSinkState);
        this.serializer = serializer;
    }

    public MongodbBulkWriter(
            DocumentSerializer<SeaTunnelRow> serializer, MongodbWriterOptions options) {
        super();
        initOptions(options);
        this.serializer = serializer;
        this.bulkRequests = new ArrayList<>();
    }

    private void initOptions(MongodbWriterOptions options) {
        this.maxRetries = options.getRetryMax();
        this.retryIntervalMs = options.getRetryInterval();
        this.collectionProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
        this.bulkActions = options.getFlushSize();
        this.batchIntervalMs = options.getBatchIntervalMs();
        this.transactionEnable = options.isTransactionEnable();
    }

    private void initBulkRequests(List<MongodbSinkState> mongodbSinkState) {
        this.bulkRequests =
                mongodbSinkState.stream()
                        .map(MongodbSinkState::getWriteModels)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
    }

    @Override
    public void write(SeaTunnelRow o) throws IOException {
        bulkRequests.add(serializer.serialize(o));
        if (!transactionEnable) {
            if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
                doBulkWrite();
            }
        }
    }

    @Override
    public Optional<MongodbCommitInfo> prepareCommit() throws IOException {
        if (transactionEnable) {
            return Optional.of(new MongodbCommitInfo(bulkRequests));
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        doBulkWrite();
        if (collectionProvider != null) {
            collectionProvider.close();
        }
    }

    @Override
    public List<MongodbSinkState> snapshotState(long checkpointId) throws IOException {
        return Collections.singletonList(new MongodbSinkState(bulkRequests));
    }

    @Override
    public void abortPrepare() {}

    void doBulkWrite() throws IOException {
        if (bulkRequests.isEmpty()) {
            // no records to write
            return;
        }

        boolean success =
                IntStream.rangeClosed(0, maxRetries)
                        .anyMatch(
                                i -> {
                                    try {
                                        lastSendTime = System.currentTimeMillis();
                                        collectionProvider
                                                .getDefaultCollection()
                                                .bulkWrite(bulkRequests);
                                        bulkRequests.clear();
                                        return true;
                                    } catch (MongoException e) {
                                        log.debug(
                                                "Bulk Write to MongoDB failed, retry times = {}",
                                                i,
                                                e);
                                        if (i >= maxRetries) {
                                            throw new MongodbConnectorException(
                                                    WRITER_OPERATION_FAILED,
                                                    "Bulk Write to MongoDB failed",
                                                    e);
                                        }
                                        try {
                                            TimeUnit.MILLISECONDS.sleep(retryIntervalMs * (i + 1));
                                        } catch (InterruptedException ex) {
                                            Thread.currentThread().interrupt();
                                            throw new MongodbConnectorException(
                                                    WRITER_OPERATION_FAILED,
                                                    "Unable to flush; interrupted while doing another attempt",
                                                    e);
                                        }
                                        return false;
                                    }
                                });

        if (!success) {
            throw new MongodbConnectorException(
                    WRITER_OPERATION_FAILED, "Bulk Write to MongoDB failed after max retries");
        }
    }

    private boolean isOverMaxBatchSizeLimit() {
        return bulkActions != -1 && bulkRequests.size() >= bulkActions;
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long lastSentInterval = System.currentTimeMillis() - lastSendTime;
        return batchIntervalMs != -1 && lastSentInterval >= batchIntervalMs;
    }
}
