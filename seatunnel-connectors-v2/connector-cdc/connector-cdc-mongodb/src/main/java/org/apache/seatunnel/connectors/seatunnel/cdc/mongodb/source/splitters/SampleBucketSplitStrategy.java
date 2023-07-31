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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import io.debezium.relational.TableId;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Aggregates.bucketAuto;
import static com.mongodb.client.model.Aggregates.sample;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MAX_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.MIN_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.boundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.getMongoCollection;

public enum SampleBucketSplitStrategy implements SplitStrategy {
    INSTANCE;

    private static final int DEFAULT_SAMPLING_THRESHOLD = 102400;

    private static final double DEFAULT_SAMPLING_RATE = 0.05;

    @Nonnull
    @Override
    public Collection<SnapshotSplit> split(@Nonnull SplitContext splitContext) {
        long chunkSizeInBytes = (long) splitContext.getChunkSizeMB() * 1024 * 1024;

        long sizeInBytes = splitContext.getSizeInBytes();
        long count = splitContext.getDocumentCount();

        // If collection's total uncompressed size less than chunk size,
        // treat the entire collection as single chunk.
        if (sizeInBytes < chunkSizeInBytes) {
            return SingleSplitStrategy.INSTANCE.split(splitContext);
        }

        int numChunks = (int) (sizeInBytes / chunkSizeInBytes) + 1;
        int numberOfSamples;
        if (count < DEFAULT_SAMPLING_THRESHOLD) {
            // full sampling if document count less than sampling size threshold.
            numberOfSamples = (int) count;
        } else {
            // sampled using sample rate.
            numberOfSamples = (int) Math.floor(count * DEFAULT_SAMPLING_RATE);
        }

        TableId collectionId = splitContext.getCollectionId();

        MongoCollection<BsonDocument> collection =
                getMongoCollection(splitContext.getMongoClient(), collectionId, BsonDocument.class);

        List<Bson> pipeline = new ArrayList<>();
        if (numberOfSamples != count) {
            pipeline.add(sample(numberOfSamples));
        }
        pipeline.add(bucketAuto("$" + ID_FIELD, numChunks));

        List<BsonDocument> chunks =
                collection.aggregate(pipeline).allowDiskUse(true).into(new ArrayList<>());

        SeaTunnelRowType rowType = shardKeysToRowType(Collections.singleton(ID_FIELD));

        List<SnapshotSplit> snapshotSplits = new ArrayList<>(chunks.size() + 2);

        SnapshotSplit firstSplit =
                new SnapshotSplit(
                        splitId(collectionId, 0),
                        collectionId,
                        rowType,
                        minLowerBoundOfId(),
                        boundOfId(lowerBoundOfBucket(chunks.get(0))));
        snapshotSplits.add(firstSplit);

        for (int i = 0; i < chunks.size(); i++) {
            BsonDocument bucket = chunks.get(i);
            snapshotSplits.add(
                    new SnapshotSplit(
                            splitId(collectionId, i + 1),
                            collectionId,
                            rowType,
                            boundOfId(lowerBoundOfBucket(bucket)),
                            boundOfId(upperBoundOfBucket(bucket))));
        }

        SnapshotSplit lastSplit =
                new SnapshotSplit(
                        splitId(collectionId, chunks.size() + 1),
                        collectionId,
                        rowType,
                        boundOfId(upperBoundOfBucket(chunks.get(chunks.size() - 1))),
                        maxUpperBoundOfId());
        snapshotSplits.add(lastSplit);

        return snapshotSplits;
    }

    private BsonDocument bucketBounds(@Nonnull BsonDocument bucket) {
        return bucket.getDocument(ID_FIELD);
    }

    private BsonValue lowerBoundOfBucket(BsonDocument bucket) {
        return bucketBounds(bucket).get(MIN_FIELD);
    }

    private BsonValue upperBoundOfBucket(BsonDocument bucket) {
        return bucketBounds(bucket).get(MAX_FIELD);
    }
}
