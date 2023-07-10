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
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FULL_DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE_INSERT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_TRUE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createPartitionMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createSourceOffsetMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createWatermarkPartitionMap;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.createMongoClient;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getMongoCollection;

@Slf4j
public class MongodbScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit snapshotSplit;

    private volatile boolean taskRunning = false;

    public MongodbScanFetchTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        MongodbFetchTaskContext taskContext = (MongodbFetchTaskContext) context;
        MongodbSourceConfig sourceConfig = taskContext.getSourceConfig();
        MongodbDialect dialect = taskContext.getDialect();
        ChangeEventQueue<DataChangeEvent> changeEventQueue = taskContext.getQueue();
        taskRunning = true;
        TableId collectionId = snapshotSplit.getTableId();
        final ChangeStreamOffset lowWatermark = dialect.displayCurrentOffset(sourceConfig);
        log.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        changeEventQueue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                createWatermarkPartitionMap(collectionId.identifier()),
                                "__mongodb_watermarks",
                                snapshotSplit.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));

        log.info("Snapshot step 2 - Snapshotting data");
        try (MongoCursor<RawBsonDocument> cursor = getSnapshotCursor(snapshotSplit, sourceConfig)) {
            while (cursor.hasNext()) {
                checkTaskRunning();
                BsonDocument valueDocument = normalizeSnapshotDocument(collectionId, cursor.next());
                BsonDocument keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));

                SourceRecord snapshotRecord =
                        buildSourceRecord(sourceConfig, collectionId, keyDocument, valueDocument);

                changeEventQueue.enqueue(new DataChangeEvent(snapshotRecord));
            }

            ChangeStreamOffset highWatermark = dialect.displayCurrentOffset(sourceConfig);
            log.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);
            changeEventQueue.enqueue(
                    new DataChangeEvent(
                            WatermarkEvent.create(
                                    createWatermarkPartitionMap(collectionId.identifier()),
                                    "__mongodb_watermarks",
                                    snapshotSplit.splitId(),
                                    WatermarkKind.HIGH,
                                    highWatermark)));

            log.info(
                    "Snapshot step 4 - Back fill stream split for snapshot split {}",
                    snapshotSplit);
            final IncrementalSplit dataBackfillSplit =
                    createBackfillStreamSplit(lowWatermark, highWatermark);
            final boolean streamBackfillRequired =
                    dataBackfillSplit.getStopOffset().isAfter(dataBackfillSplit.getStartupOffset());

            if (!streamBackfillRequired) {
                changeEventQueue.enqueue(
                        new DataChangeEvent(
                                WatermarkEvent.create(
                                        createWatermarkPartitionMap(collectionId.identifier()),
                                        "__mongodb_watermarks",
                                        dataBackfillSplit.splitId(),
                                        WatermarkKind.END,
                                        dataBackfillSplit.getStopOffset())));
            } else {
                MongodbStreamFetchTask dataBackfillTask =
                        new MongodbStreamFetchTask(dataBackfillSplit);
                dataBackfillTask.execute(taskContext);
            }
        } catch (Exception e) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT,
                    String.format(
                            "Execute snapshot read subtask for mongodb split %s fail",
                            snapshotSplit));
        } finally {
            taskRunning = false;
        }
    }

    @Nonnull
    private MongoCursor<RawBsonDocument> getSnapshotCursor(
            @Nonnull SnapshotSplit snapshotSplit, MongodbSourceConfig sourceConfig) {
        MongoClient mongoClient = createMongoClient(sourceConfig);
        MongoCollection<RawBsonDocument> collection =
                getMongoCollection(mongoClient, snapshotSplit.getTableId(), RawBsonDocument.class);
        BsonDocument startKey = (BsonDocument) snapshotSplit.getSplitStart()[1];
        BsonDocument endKey = (BsonDocument) snapshotSplit.getSplitEnd()[1];
        BsonDocument hint = (BsonDocument) snapshotSplit.getSplitStart()[0];
        log.info(
                "Initializing snapshot split processing: TableId={}, StartKey={}, EndKey={}, Hint={}",
                snapshotSplit.getTableId(),
                startKey,
                endKey,
                hint);
        return collection
                .find()
                .min(startKey)
                .max(endKey)
                .hint(hint)
                .batchSize(sourceConfig.getBatchSize())
                .noCursorTimeout(true)
                .cursor();
    }

    @Nonnull
    private SourceRecord buildSourceRecord(
            @Nonnull MongodbSourceConfig sourceConfig,
            @Nonnull TableId collectionId,
            BsonDocument keyDocument,
            BsonDocument valueDocument) {
        return MongodbRecordUtils.buildSourceRecord(
                createPartitionMap(
                        sourceConfig.getHosts(), collectionId.catalog(), collectionId.table()),
                createSourceOffsetMap(keyDocument.getDocument(ID_FIELD), true),
                collectionId.identifier(),
                keyDocument,
                valueDocument);
    }

    private void checkTaskRunning() {
        if (!taskRunning) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Interrupted while snapshotting collection");
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
    public SnapshotSplit getSplit() {
        return snapshotSplit;
    }

    private IncrementalSplit createBackfillStreamSplit(
            ChangeStreamOffset lowWatermark, ChangeStreamOffset highWatermark) {
        return new IncrementalSplit(
                snapshotSplit.splitId(),
                Collections.singletonList(snapshotSplit.getTableId()),
                lowWatermark,
                highWatermark,
                new ArrayList<>());
    }

    private BsonDocument normalizeSnapshotDocument(
            @Nonnull final TableId collectionId, @Nonnull final BsonDocument originalDocument) {
        return new BsonDocument()
                .append(ID_FIELD, new BsonDocument(ID_FIELD, originalDocument.get(ID_FIELD)))
                .append(OPERATION_TYPE, new BsonString(OPERATION_TYPE_INSERT))
                .append(
                        NS_FIELD,
                        new BsonDocument(DB_FIELD, new BsonString(collectionId.catalog()))
                                .append(COLL_FIELD, new BsonString(collectionId.table())))
                .append(DOCUMENT_KEY, new BsonDocument(ID_FIELD, originalDocument.get(ID_FIELD)))
                .append(FULL_DOCUMENT, originalDocument)
                .append(TS_MS_FIELD, new BsonInt64(System.currentTimeMillis()))
                .append(
                        SOURCE_FIELD,
                        new BsonDocument(SNAPSHOT_FIELD, new BsonString(SNAPSHOT_TRUE))
                                .append(TS_MS_FIELD, new BsonInt64(0L)));
    }
}
