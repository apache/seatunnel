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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@ToString
public class IcebergSplitEnumeratorState implements Serializable {

    private static final long serialVersionUID = -529307606400995298L;

    // TODO: Waiting for migration to complete before remove
    @Deprecated private IcebergEnumeratorPosition lastEnumeratedPosition;

    private Collection<TablePath> pendingTables;
    private Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits;
    private Map<TablePath, IcebergEnumeratorPosition> tableOffsets;

    public IcebergSplitEnumeratorState(
            Collection<TablePath> pendingTables,
            Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits) {
        this(pendingTables, pendingSplits, Collections.emptyMap());
    }

    public IcebergSplitEnumeratorState(
            Collection<TablePath> pendingTables,
            Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits,
            Map<TablePath, IcebergEnumeratorPosition> tableOffsets) {
        this.pendingTables = pendingTables;
        this.pendingSplits = pendingSplits;
        this.tableOffsets = tableOffsets;
    }

    // TODO: Waiting for migration to complete before remove
    @Deprecated
    public IcebergSplitEnumeratorState(
            IcebergEnumeratorPosition lastEnumeratedPosition,
            Map<Integer, List<IcebergFileScanTaskSplit>> pendingSplits) {
        this.lastEnumeratedPosition = lastEnumeratedPosition;
        this.pendingSplits = pendingSplits;
        this.pendingTables = new ArrayList<>();
        this.tableOffsets = new HashMap<>();
    }

    // TODO: Waiting for migration to complete before remove
    @Deprecated
    public IcebergSplitEnumeratorState setPendingTable(TablePath table) {
        if (lastEnumeratedPosition != null) {
            this.pendingTables.add(table);
            this.tableOffsets.put(table, lastEnumeratedPosition);
        }
        return this;
    }
}
