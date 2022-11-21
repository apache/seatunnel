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

package org.apache.seatunnel.connectors.cdc.base.source.split.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import lombok.Getter;
import lombok.Setter;

/**
 * The state of split to describe the snapshot of table(s).
 */
@Getter
@Setter
public class SnapshotSplitState extends SourceSplitStateBase {

    private Offset highWatermark;

    public SnapshotSplitState(SnapshotSplit split) {
        super(split);
    }

    @Override
    public SnapshotSplit toSourceSplit() {
        final SnapshotSplit snapshotSplit = split.asSnapshotSplit();
        return new SnapshotSplit(
            snapshotSplit.splitId(),
            snapshotSplit.getTableId(),
            snapshotSplit.getSplitKeyType(),
            snapshotSplit.getSplitStart(),
            snapshotSplit.getSplitEnd(),
            getHighWatermark());
    }
}
