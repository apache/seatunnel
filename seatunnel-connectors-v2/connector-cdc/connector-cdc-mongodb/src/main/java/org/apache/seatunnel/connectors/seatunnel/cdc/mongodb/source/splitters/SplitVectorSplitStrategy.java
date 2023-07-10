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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.splitters;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.apache.commons.collections4.CollectionUtils;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMinKey;
import org.bson.BsonValue;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.UNAUTHORIZED_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.boundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.isCommandSucceed;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.splitVector;

@Slf4j
public enum SplitVectorSplitStrategy implements SplitStrategy {
    INSTANCE;

    @Override
    public Collection<SnapshotSplit> split(@Nonnull SplitContext splitContext) {
        MongoClient mongoClient = splitContext.getMongoClient();
        TableId collectionId = splitContext.getCollectionId();
        int chunkSizeMB = splitContext.getChunkSizeMB();

        BsonDocument keyPattern = new BsonDocument(ID_FIELD, new BsonInt32(1));

        BsonDocument splitResult;
        try {
            splitResult = splitVector(mongoClient, collectionId, keyPattern, chunkSizeMB);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                log.warn(
                        "Unauthorized to execute splitVector command: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            } else {
                log.warn(
                        "Execute splitVector command failed: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            }
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        if (!isCommandSucceed(splitResult)) {
            log.warn(
                    "Could not calculate standalone splits: {}, fallback to SampleSplitter",
                    splitResult.getString("errmsg"));
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        BsonArray splitKeys = splitResult.getArray("splitKeys");
        if (CollectionUtils.isEmpty(splitKeys)) {
            // documents size is less than chunk size, treat the entire collection as single chunk.
            return SingleSplitStrategy.INSTANCE.split(splitContext);
        }

        SeaTunnelRowType rowType = shardKeysToRowType(Collections.singleton(ID_FIELD));
        List<SnapshotSplit> snapshotSplits = new ArrayList<>(splitKeys.size() + 1);

        BsonValue lowerValue = new BsonMinKey();
        ;
        for (int i = 0; i < splitKeys.size(); i++) {
            BsonValue splitKeyValue = splitKeys.get(i).asDocument().get(ID_FIELD);
            snapshotSplits.add(
                    new SnapshotSplit(
                            splitId(collectionId, i),
                            collectionId,
                            rowType,
                            boundOfId(lowerValue),
                            boundOfId(splitKeyValue)));
            lowerValue = splitKeyValue;
        }

        SnapshotSplit lastSplit =
                new SnapshotSplit(
                        splitId(collectionId, splitKeys.size()),
                        collectionId,
                        rowType,
                        boundOfId(lowerValue),
                        maxUpperBoundOfId());
        snapshotSplits.add(lastSplit);

        return snapshotSplits;
    }
}
