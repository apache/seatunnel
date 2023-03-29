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
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MongodbBulkWriter
        implements SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> {

    private final MongoClientProvider collectionProvider;

    private final MongoCollection<Document> collection;

    private final ConcurrentLinkedQueue<Document> currentBulk = new ConcurrentLinkedQueue<>();

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private final DocumentSerializer<SeaTunnelRow> serializer;

    private ScheduledExecutorService scheduler;

    private ScheduledFuture scheduledFuture;

    private volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy;

    private volatile boolean closed = false;

    private final boolean upsertEnable;
    private final String[] upsertKeys;

    public MongodbBulkWriter(
            DocumentSerializer<SeaTunnelRow> serializer, MongodbWriterOptions options) {
        this.retryPolicy = new RetryPolicy(options.getRetryMax(), options.getFlushInterval());
        this.upsertEnable = options.isUpsertEnable();
        this.upsertKeys = options.getUpsertKey();
        this.collectionProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(options.getConnectString())
                        .database(options.getDatabase())
                        .collection(options.getCollection())
                        .build();
        this.collection = collectionProvider.getDefaultCollection();
        this.serializer = serializer;
        this.maxSize = options.getFlushSize();
        this.flushOnCheckpoint = options.isFlushOnCheckpoint();
        if (!flushOnCheckpoint && options.getFlushInterval() > 0) {
            this.scheduler = Executors.newScheduledThreadPool(1);
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongodbBulkWriter.this) {
                                    if (!closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            options.getFlushInterval(),
                            options.getFlushInterval(),
                            TimeUnit.SECONDS);
        }
    }

    private synchronized void flush() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            if (this.upsertEnable) {
                flushUpsert(iterator);
            } else {
                while (iterator.hasNext()) {
                    DocumentBulk bulk = iterator.next();
                    do {
                        try {
                            // ordered, non-bypass mode
                            if (bulk.size() > 0) {
                                collection.insertMany(bulk.getDocuments());
                            }
                            iterator.remove();
                            break;
                        } catch (MongoException e) {
                            // maybe partial failure
                            log.error("Failed to flush data to MongoDB", e);
                        }
                    } while (!closed && retryPolicy.shouldBackoffRetry());
                }
            }
        }
    }

    private void ensureConnection() {
        try {
            collection.listIndexes();
        } catch (MongoException e) {
            log.warn("Connection is not available, try to reconnect", e);
            collectionProvider.recreateClient();
        }
    }

    static class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
        }
    }

    @Override
    public void write(SeaTunnelRow o) throws IOException {
        checkFlushException();
        currentBulk.add(serializer.serialize(o));
        rollBulkIfNeeded();
    }

    @Override
    public Optional<MongodbCommitInfo> prepareCommit() throws IOException {
        if (flushOnCheckpoint) {
            rollBulkIfNeeded(true);
        }

        return Optional.of(new MongodbCommitInfo(pendingBulks));
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    @Override
    public List<DocumentBulk> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
        int size = currentBulk.size();

        if (force || size >= maxSize) {
            DocumentBulk bulk = new DocumentBulk(maxSize);
            for (int i = 0; i < size; i++) {
                if (bulk.size() >= maxSize) {
                    pendingBulks.add(bulk);
                    bulk = new DocumentBulk(maxSize);
                }
                bulk.add(currentBulk.poll());
            }
            pendingBulks.add(bulk);
        }
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        // flush all cache data before close in non-transaction mode
        if (!flushOnCheckpoint) {
            synchronized (this) {
                if (!closed) {
                    try {
                        rollBulkIfNeeded(true);
                        flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }

        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }

        if (collectionProvider != null) {
            collectionProvider.close();
        }
    }

    private void flushUpsert(Iterator<DocumentBulk> iterator) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(true);
        while (iterator.hasNext()) {
            DocumentBulk bulk = iterator.next();
            do {
                try {
                    if (bulk.size() > 0) {
                        List<Document> documents = bulk.getDocuments();
                        List<UpdateOneModel<Document>> upserts = new ArrayList<>();
                        for (Document document : documents) {
                            List<Bson> filters = new ArrayList<>(upsertKeys.length);
                            for (String upsertKey : upsertKeys) {
                                Object o = document.get(upsertKey);
                                Bson eq = Filters.eq(upsertKey, o);
                                filters.add(eq);
                            }
                            Document update = new Document();
                            update.append("$set", document);
                            Bson filter = Filters.and(filters);
                            UpdateOneModel<Document> updateOneModel =
                                    new UpdateOneModel<>(filter, update, updateOptions);
                            upserts.add(updateOneModel);
                        }
                        collection.bulkWrite(upserts, bulkWriteOptions);
                    }

                    iterator.remove();
                    break;
                } catch (MongoException e) {
                    // maybe partial failure
                    log.error("Failed to flush data to MongoDB", e);
                }
            } while (!closed && retryPolicy.shouldBackoffRetry());
        }
    }
}
