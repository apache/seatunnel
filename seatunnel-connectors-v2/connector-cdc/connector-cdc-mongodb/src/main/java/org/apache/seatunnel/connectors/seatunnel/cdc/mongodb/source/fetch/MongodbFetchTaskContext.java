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

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import com.mongodb.client.model.changestream.OperationType;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_TRUE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.BsonUtils.compareBsonValue;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.getDocumentKey;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.getResumeToken;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.createMongoClient;

public class MongodbFetchTaskContext implements FetchTask.Context {

    private final MongodbDialect dialect;
    private final MongodbSourceConfig sourceConfig;
    private final ChangeStreamDescriptor changeStreamDescriptor;
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public MongodbFetchTaskContext(
            MongodbDialect dialect,
            MongodbSourceConfig sourceConfig,
            ChangeStreamDescriptor changeStreamDescriptor) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
        this.changeStreamDescriptor = changeStreamDescriptor;
    }

    public void configure(@Nonnull SourceSplitBase sourceSplitBase) {
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() ? Integer.MAX_VALUE : sourceConfig.getBatchSize();
        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(sourceConfig.getPollAwaitTimeMillis()))
                        .maxBatchSize(sourceConfig.getPollMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "mongodb-cdc",
                                                "mongodb-cdc-connector",
                                                "mongodb-cdc-connector-task"))
                        .build();
    }

    public MongodbSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public MongodbDialect getDialect() {
        return dialect;
    }

    public ChangeStreamDescriptor getChangeStreamDescriptor() {
        return changeStreamDescriptor;
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return MongodbRecordUtils.getTableId(record);
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        // We have pushed down the filters to server side.
        return Tables.TableFilter.includeAll();
    }

    @Override
    public boolean isExactlyOnce() {
        return true;
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new ChangeStreamOffset(getResumeToken(record));
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return MongodbRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public boolean isRecordBetween(
            SourceRecord record, @Nonnull Object[] splitStart, @Nonnull Object[] splitEnd) {
        BsonDocument documentKey = getDocumentKey(record);
        BsonDocument splitKeys = (BsonDocument) splitStart[0];
        String firstKey = splitKeys.getFirstKey();
        BsonValue keyValue = documentKey.get(firstKey);
        BsonValue lowerBound = ((BsonDocument) splitStart[1]).get(firstKey);
        BsonValue upperBound = ((BsonDocument) splitEnd[1]).get(firstKey);

        if (isFullRange(lowerBound, upperBound)) {
            return true;
        }

        return isValueInRange(lowerBound, keyValue, upperBound);
    }

    private boolean isFullRange(@Nonnull BsonValue lowerBound, BsonValue upperBound) {
        return lowerBound.getBsonType() == BsonType.MIN_KEY
                && upperBound.getBsonType() == BsonType.MAX_KEY;
    }

    private boolean isValueInRange(BsonValue lowerBound, BsonValue value, BsonValue upperBound) {
        return compareBsonValue(lowerBound, value) <= 0 && compareBsonValue(value, upperBound) < 0;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, @Nonnull SourceRecord changeRecord) {
        Struct key = (Struct) changeRecord.key();
        Struct value = (Struct) changeRecord.value();

        if (value != null) {
            String operationType = value.getString(OPERATION_TYPE);

            switch (OperationType.fromString(operationType)) {
                case INSERT:
                case UPDATE:
                case REPLACE:
                    outputBuffer.put(key, changeRecord);
                    break;
                case DELETE:
                    outputBuffer.remove(key);
                    break;
                default:
                    throw new MongodbConnectorException(
                            ILLEGAL_ARGUMENT,
                            "Data change record meet UNKNOWN operation: " + operationType);
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(
            @Nonnull Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .peek(
                        record -> {
                            Struct value = (Struct) record.value();
                            Struct source = new Struct(value.schema().field(SOURCE_FIELD).schema());
                            source.put(TS_MS_FIELD, 0L);
                            source.put(SNAPSHOT_FIELD, SNAPSHOT_TRUE);
                            value.put(SOURCE_FIELD, source);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> createMongoClient(sourceConfig).close()));
    }
}
