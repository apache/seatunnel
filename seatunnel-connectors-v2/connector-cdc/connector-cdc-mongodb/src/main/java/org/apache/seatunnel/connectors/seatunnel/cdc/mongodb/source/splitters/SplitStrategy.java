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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.bson.BsonDocument;

import io.debezium.relational.TableId;

import javax.annotation.Nonnull;

import java.util.Collection;

import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;

public interface SplitStrategy {

    Collection<SnapshotSplit> split(SplitContext splitContext);

    default String splitId(@Nonnull TableId collectionId, int chunkId) {
        return String.format("%s:%d", collectionId.identifier(), chunkId);
    }

    default SeaTunnelRowType shardKeysToRowType(@Nonnull BsonDocument shardKeys) {
        return shardKeysToRowType(shardKeys.keySet());
    }

    default SeaTunnelRowType shardKeysToRowType(@Nonnull Collection<String> shardKeys) {
        SeaTunnelDataType<?>[] fieldTypes =
                shardKeys.stream()
                        // We cannot get the exact type of the shard key, only the ordering of the
                        // shard index.
                        // Use the INT type as a placeholder.
                        .map(key -> INT_TYPE)
                        .toArray(SeaTunnelDataType[]::new);
        String[] fieldNames = shardKeys.toArray(new String[0]);
        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }
}
