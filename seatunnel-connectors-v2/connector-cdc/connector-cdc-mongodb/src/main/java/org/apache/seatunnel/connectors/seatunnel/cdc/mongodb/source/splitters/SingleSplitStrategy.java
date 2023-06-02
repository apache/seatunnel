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

import org.jetbrains.annotations.NotNull;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect.collectionSchema;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;

public enum SingleSplitStrategy implements SplitStrategy {
    INSTANCE;

    @Override
    public Collection<SnapshotSplit> split(@NotNull SplitContext splitContext) {
        TableId collectionId = splitContext.getCollectionId();
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        schema.put(collectionId, collectionSchema(collectionId));

        SnapshotSplit snapshotSplit = createSnapshotSplit(collectionId);

        return Collections.singletonList(snapshotSplit);
    }

    @NotNull private SnapshotSplit createSnapshotSplit(TableId collectionId) {
        SeaTunnelRowType rowType = shardKeysToRowType(Collections.singleton(ID_FIELD));
        return new SnapshotSplit(
                splitId(collectionId, 0),
                collectionId,
                rowType,
                minLowerBoundOfId(),
                maxUpperBoundOfId(),
                null);
    }
}
