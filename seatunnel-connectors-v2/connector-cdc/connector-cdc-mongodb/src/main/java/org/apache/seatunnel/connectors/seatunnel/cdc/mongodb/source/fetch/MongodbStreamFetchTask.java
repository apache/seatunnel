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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Optional;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_OPERATION;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.CLUSTER_TIME_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FAILED_TO_PARSE_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FALSE_FALSE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ILLEGAL_OPERATION_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.UNAUTHORIZED_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.UNKNOWN_FIELD_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset.NO_STOPPING_OFFSET;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createHeartbeatPartitionMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createPartitionMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createSourceOffsetMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createWatermarkPartitionMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.currentBsonTimestamp;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.getResumeToken;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.createMongoClient;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getChangeStreamIterable;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getCurrentClusterTime;

@Slf4j
public class MongodbStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit streamSplit;
    private volatile boolean taskRunning = false;

    private MongodbSourceConfig sourceConfig;
    private final Time time = new SystemTime();
    private boolean supportsStartAtOperationTime = true;
    private boolean supportsStartAfter = true;

    public MongodbStreamFetchTask(IncrementalSplit streamSplit) {
        this.streamSplit = streamSplit;
    }

    @Override
    public void execute(Context context) {
        MongodbFetchTaskContext taskContext = (MongodbFetchTaskContext) context;
        this.sourceConfig = taskContext.getSourceConfig();

        ChangeStreamDescriptor descriptor = taskContext.getChangeStreamDescriptor();
        ChangeEventQueue<DataChangeEvent> queue = taskContext.getQueue();

        MongoClient mongoClient = createMongoClient(sourceConfig);
        MongoChangeStreamCursor<BsonDocument> changeStreamCursor =
                openChangeStreamCursor(descriptor);
        HeartbeatManager heartbeatManager = openHeartbeatManagerIfNeeded(changeStreamCursor);

        final long startPoll = time.milliseconds();
        long nextUpdate = startPoll + sourceConfig.getPollAwaitTimeMillis();
        this.taskRunning = true;
        try {
            while (taskRunning) {
                Optional<BsonDocument> next = Optional.ofNullable(changeStreamCursor.tryNext());
                SourceRecord changeRecord = null;
                if (!next.isPresent()) {
                    long untilNext = nextUpdate - time.milliseconds();
                    if (untilNext > 0) {
                        log.debug("Waiting {} ms to poll change records", untilNext);
                        time.sleep(untilNext);
                        continue;
                    }

                    if (heartbeatManager != null) {
                        changeRecord =
                                heartbeatManager
                                        .heartbeat()
                                        .map(this::normalizeHeartbeatRecord)
                                        .orElse(null);
                    }
                    // update nextUpdateTime
                    nextUpdate = time.milliseconds() + sourceConfig.getPollAwaitTimeMillis();
                } else {
                    BsonDocument changeStreamDocument = next.get();
                    MongoNamespace namespace = getMongoNamespace(changeStreamDocument);

                    BsonDocument resumeToken = changeStreamDocument.getDocument(ID_FIELD);
                    BsonDocument valueDocument =
                            normalizeChangeStreamDocument(changeStreamDocument);

                    log.trace("Adding {} to {}", valueDocument, namespace.getFullName());

                    changeRecord =
                            MongodbRecordUtils.buildSourceRecord(
                                    createPartitionMap(
                                            sourceConfig.getHosts(),
                                            namespace.getDatabaseName(),
                                            namespace.getCollectionName()),
                                    createSourceOffsetMap(resumeToken, false),
                                    namespace.getFullName(),
                                    changeStreamDocument.getDocument(ID_FIELD),
                                    valueDocument);
                }

                if (changeRecord != null) {
                    queue.enqueue(new DataChangeEvent(changeRecord));
                }

                if (isBoundedRead()) {
                    ChangeStreamOffset currentOffset;
                    if (changeRecord != null) {
                        currentOffset = new ChangeStreamOffset(getResumeToken(changeRecord));
                    } else {
                        // Heartbeat is not turned on or there is no update event
                        currentOffset = new ChangeStreamOffset(getCurrentClusterTime(mongoClient));
                    }

                    // Reach the high watermark, the binlog fetcher should be finished
                    if (currentOffset.isAtOrAfter(streamSplit.getStopOffset())) {
                        // send watermark end event
                        SourceRecord watermark =
                                WatermarkEvent.create(
                                        createWatermarkPartitionMap(descriptor.toString()),
                                        "__mongodb_watermarks",
                                        streamSplit.splitId(),
                                        WatermarkKind.END,
                                        currentOffset);

                        queue.enqueue(new DataChangeEvent(watermark));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Poll change stream records failed");
        } finally {
            taskRunning = false;
            if (changeStreamCursor != null) {
                changeStreamCursor.close();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public IncrementalSplit getSplit() {
        return streamSplit;
    }

    private MongoChangeStreamCursor<BsonDocument> openChangeStreamCursor(
            ChangeStreamDescriptor changeStreamDescriptor) {
        ChangeStreamOffset offset =
                new ChangeStreamOffset(streamSplit.getStartupOffset().getOffset());

        ChangeStreamIterable<Document> changeStreamIterable =
                getChangeStreamIterable(sourceConfig, changeStreamDescriptor);

        BsonDocument resumeToken = offset.getResumeToken();
        BsonTimestamp timestamp = offset.getTimestamp();

        if (resumeToken != null) {
            if (supportsStartAfter) {
                log.info("Open the change stream after the previous offset: {}", resumeToken);
                changeStreamIterable.startAfter(resumeToken);
            } else {
                log.info(
                        "Open the change stream after the previous offset using resumeAfter: {}",
                        resumeToken);
                changeStreamIterable.resumeAfter(resumeToken);
            }
        } else {
            if (supportsStartAtOperationTime) {
                log.info("Open the change stream at the timestamp: {}", timestamp);
                changeStreamIterable.startAtOperationTime(timestamp);
            } else {
                log.warn("Open the change stream of the latest offset");
            }
        }

        try {
            return (MongoChangeStreamCursor<BsonDocument>)
                    changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == FAILED_TO_PARSE_ERROR
                    || e.getErrorCode() == UNKNOWN_FIELD_ERROR) {
                if (e.getErrorMessage().contains("startAtOperationTime")) {
                    supportsStartAtOperationTime = false;
                    return openChangeStreamCursor(changeStreamDescriptor);
                } else if (e.getErrorMessage().contains("startAfter")) {
                    supportsStartAfter = false;
                    return openChangeStreamCursor(changeStreamDescriptor);
                } else {
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT, "Open change stream failed");
                }
            } else if (e.getErrorCode() == ILLEGAL_OPERATION_ERROR) {
                throw new MongodbConnectorException(
                        UNSUPPORTED_OPERATION,
                        String.format(
                                "Illegal $changeStream operation: %s %s",
                                e.getErrorMessage(), e.getErrorCode()));

            } else if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                throw new MongodbConnectorException(
                        UNSUPPORTED_OPERATION,
                        String.format(
                                "Unauthorized $changeStream operation: %s %s",
                                e.getErrorMessage(), e.getErrorCode()));

            } else {
                throw new MongodbConnectorException(ILLEGAL_ARGUMENT, "Open change stream failed");
            }
        }
    }

    @Nullable private HeartbeatManager openHeartbeatManagerIfNeeded(
            MongoChangeStreamCursor<BsonDocument> changeStreamCursor) {
        if (sourceConfig.getHeartbeatIntervalMillis() > 0) {
            return new HeartbeatManager(
                    time,
                    changeStreamCursor,
                    sourceConfig.getHeartbeatIntervalMillis(),
                    "__mongodb_heartbeats",
                    createHeartbeatPartitionMap(sourceConfig.getHosts()));
        }
        return null;
    }

    @Nonnull
    private BsonDocument normalizeChangeStreamDocument(@Nonnull BsonDocument changeStreamDocument) {
        // _id: primary key of change document.
        BsonDocument normalizedDocument = normalizeKeyDocument(changeStreamDocument);
        changeStreamDocument.put(ID_FIELD, normalizedDocument);

        // ts_ms: It indicates the time at which the reader processed the event.
        changeStreamDocument.put(TS_MS_FIELD, new BsonInt64(System.currentTimeMillis()));

        // source
        BsonDocument source = new BsonDocument();
        source.put(SNAPSHOT_FIELD, new BsonString(FALSE_FALSE));

        if (!changeStreamDocument.containsKey(CLUSTER_TIME_FIELD)) {
            log.warn(
                    "Cannot extract clusterTime from change stream event, fallback to current timestamp.");
            changeStreamDocument.put(CLUSTER_TIME_FIELD, currentBsonTimestamp());
        }

        // source.ts_ms
        // It indicates the time that the change was made in the database. If the record is read
        // from snapshot of the table instead of the change stream, the value is always 0.
        BsonTimestamp clusterTime = changeStreamDocument.getTimestamp(CLUSTER_TIME_FIELD);
        Instant clusterInstant = Instant.ofEpochSecond(clusterTime.getTime());
        source.put(TS_MS_FIELD, new BsonInt64(clusterInstant.toEpochMilli()));
        changeStreamDocument.put(SOURCE_FIELD, source);

        return changeStreamDocument;
    }

    @Nonnull
    private BsonDocument normalizeKeyDocument(@Nonnull BsonDocument changeStreamDocument) {
        BsonDocument documentKey = changeStreamDocument.getDocument(DOCUMENT_KEY);
        BsonDocument primaryKey = new BsonDocument(ID_FIELD, documentKey.get(ID_FIELD));
        return new BsonDocument(ID_FIELD, primaryKey);
    }

    @Nonnull
    private SourceRecord normalizeHeartbeatRecord(@Nonnull SourceRecord heartbeatRecord) {
        final Struct heartbeatValue =
                new Struct(SchemaBuilder.struct().field(TS_MS_FIELD, Schema.INT64_SCHEMA).build());
        heartbeatValue.put(TS_MS_FIELD, Instant.now().toEpochMilli());

        return new SourceRecord(
                heartbeatRecord.sourcePartition(),
                heartbeatRecord.sourceOffset(),
                heartbeatRecord.topic(),
                heartbeatRecord.keySchema(),
                heartbeatRecord.key(),
                SchemaBuilder.struct().field(TS_MS_FIELD, Schema.INT64_SCHEMA).build(),
                heartbeatValue);
    }

    @Nonnull
    private MongoNamespace getMongoNamespace(@Nonnull BsonDocument changeStreamDocument) {
        BsonDocument ns = changeStreamDocument.getDocument(NS_FIELD);

        return new MongoNamespace(
                ns.getString(DB_FIELD).getValue(), ns.getString(COLL_FIELD).getValue());
    }

    private boolean isBoundedRead() {
        return !NO_STOPPING_OFFSET.equals(streamSplit.getStopOffset());
    }
}
