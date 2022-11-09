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

package org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.split;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cdc.base.source.meta.offset.Offset;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.HashMap;
import java.util.Map;

/**
 * A kind of {@link SnapshotSplit} without table schema information, it is useful to reduce memory
 * usage in JobManager.
 */
public class SchemalessSnapshotSplit extends SnapshotSplit {

    public SchemalessSnapshotSplit(
            TableId tableId,
            String splitId,
            SeaTunnelRowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            Offset highWatermark) {
        super(
                tableId,
                splitId,
                splitKeyType,
                splitStart,
                splitEnd,
                highWatermark,
                new HashMap<>(1));
    }

    /**
     * Converts current {@link SchemalessSnapshotSplit} to {@link SnapshotSplit} with given table
     * schema information.
     */
    public final SnapshotSplit toSnapshotSplit(TableChange tableSchema) {
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(getTableId(), tableSchema);
        return new SnapshotSplit(
                getTableId(),
                splitId(),
                getSplitKeyType(),
                getSplitStart(),
                getSplitEnd(),
                getHighWatermark(),
                tableSchemas);
    }
}
