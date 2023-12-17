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

import org.bson.BsonBoolean;
import org.bson.BsonDocument;

import com.mongodb.MongoQueryException;
import com.mongodb.client.MongoClient;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DROPPED_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MAX_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MIN_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.UNAUTHORIZED_ERROR;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.readChunks;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.readCollectionMetadata;

@Slf4j
public class ShardedSplitStrategy implements SplitStrategy {

    public static final ShardedSplitStrategy INSTANCE = new ShardedSplitStrategy();

    private ShardedSplitStrategy() {}

    @Override
    public Collection<SnapshotSplit> split(@Nonnull SplitContext splitContext) {
        TableId collectionId = splitContext.getCollectionId();
        MongoClient mongoClient = splitContext.getMongoClient();

        List<BsonDocument> chunks;
        BsonDocument collectionMetadata;
        try {
            collectionMetadata = readCollectionMetadata(mongoClient, collectionId);
            if (!isValidShardedCollection(collectionMetadata)) {
                log.warn(
                        "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                        collectionId);
                return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
            }
            chunks = readChunks(mongoClient, collectionMetadata);
        } catch (MongoQueryException e) {
            if (e.getErrorCode() == UNAUTHORIZED_ERROR) {
                log.warn(
                        "Unauthorized to read config.collections or config.chunks: {}, fallback to SampleSplitter.",
                        e.getErrorMessage());
            } else {
                log.warn(
                        "Read config.chunks collection failed: {}, fallback to SampleSplitter",
                        e.getErrorMessage());
            }
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        if (chunks.isEmpty()) {
            log.warn(
                    "Collection {} does not appear to be sharded, fallback to SampleSplitter.",
                    collectionId);
            return SampleBucketSplitStrategy.INSTANCE.split(splitContext);
        }

        BsonDocument splitKeys = collectionMetadata.getDocument("key");
        SeaTunnelRowType rowType = shardKeysToRowType(splitKeys);

        List<SnapshotSplit> snapshotSplits = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument chunk = chunks.get(i);
            snapshotSplits.add(
                    new SnapshotSplit(
                            splitId(collectionId, i),
                            collectionId,
                            rowType,
                            new Object[] {splitKeys, chunk.getDocument(MIN_FIELD)},
                            new Object[] {splitKeys, chunk.getDocument(MAX_FIELD)}));
        }
        return snapshotSplits;
    }

    private boolean isValidShardedCollection(BsonDocument collectionMetadata) {
        return collectionMetadata != null
                && !collectionMetadata.getBoolean(DROPPED_FIELD, BsonBoolean.FALSE).getValue();
    }
}
