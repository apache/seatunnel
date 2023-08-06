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

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNumber;

import com.mongodb.client.MongoClient;
import io.debezium.relational.TableId;

import javax.annotation.Nonnull;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.collStats;

public class SplitContext {

    private final MongoClient mongoClient;
    private final TableId collectionId;
    private final BsonDocument collectionStats;
    private final int chunkSizeMB;

    public SplitContext(
            MongoClient mongoClient,
            TableId collectionId,
            BsonDocument collectionStats,
            int chunkSizeMB) {
        this.mongoClient = mongoClient;
        this.collectionId = collectionId;
        this.collectionStats = collectionStats;
        this.chunkSizeMB = chunkSizeMB;
    }

    @Nonnull
    public static SplitContext of(MongodbSourceConfig sourceConfig, TableId collectionId) {
        MongoClient mongoClient = MongodbUtils.createMongoClient(sourceConfig);
        BsonDocument collectionStats = collStats(mongoClient, collectionId);
        int chunkSizeMB = sourceConfig.getSplitSize();
        return new SplitContext(mongoClient, collectionId, collectionStats, chunkSizeMB);
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public TableId getCollectionId() {
        return collectionId;
    }

    public int getChunkSizeMB() {
        return chunkSizeMB;
    }

    public long getDocumentCount() {
        return getNumberValue(collectionStats, "count");
    }

    public long getSizeInBytes() {
        return getNumberValue(collectionStats, "size");
    }

    public long getAvgObjSizeInBytes() {
        return getNumberValue(collectionStats, "avgObjSize");
    }

    public boolean isShardedCollection() {
        return collectionStats.getBoolean("sharded", BsonBoolean.FALSE).getValue();
    }

    private long getNumberValue(@Nonnull BsonDocument document, String fieldName) {
        BsonNumber number = document.getNumber(fieldName, new BsonInt64(0));
        return number.longValue();
    }
}
