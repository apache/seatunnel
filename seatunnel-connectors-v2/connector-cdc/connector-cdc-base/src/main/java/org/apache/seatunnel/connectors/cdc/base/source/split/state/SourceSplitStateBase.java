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

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

/** State of the reader, essentially a mutable version of the {@link SourceSplit}. */
public abstract class SourceSplitStateBase {

    protected final SourceSplitBase split;

    public SourceSplitStateBase(SourceSplitBase split) {
        this.split = split;
    }

    /** Checks whether this split state is a snapshot split state. */
    public final boolean isSnapshotSplitState() {
        return getClass() == SnapshotSplitState.class;
    }

    /** Checks whether this split state is a incremental split state. */
    public final boolean isIncrementalSplitState() {
        return getClass() == IncrementalSplitState.class;
    }

    /** Casts this split state into a {@link SnapshotSplitState}. */
    public final SnapshotSplitState asSnapshotSplitState() {
        return (SnapshotSplitState) this;
    }

    /** Casts this split state into a {@link IncrementalSplitState}. */
    public final IncrementalSplitState asIncrementalSplitState() {
        return (IncrementalSplitState) this;
    }

    /** Use the current split state to create a new SourceSplit. */
    public abstract SourceSplitBase toSourceSplit();
}
