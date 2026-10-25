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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator.state;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import lombok.Data;

/** A {@link PendingSplitsState} for pending incremental splits. */
@Data
public class IncrementalPhaseState implements PendingSplitsState {

    // Preserve compatibility with checkpoints written when this state had no fields.
    private static final long serialVersionUID = -6809026812298443356L;

    private final Offset startupOffset;

    /**
     * The stop offset resolved once at the enumerator when the snapshot phase completes ({@code
     * stop.mode = latest}). Stored in the checkpoint so that a restart reuses the same value
     * instead of re-resolving (and drifting) it. {@code null} for other stop modes and for
     * checkpoints written before this field existed.
     */
    private final Offset stopOffset;

    public IncrementalPhaseState() {
        this(null, null);
    }

    public IncrementalPhaseState(Offset startupOffset) {
        this(startupOffset, null);
    }

    public IncrementalPhaseState(Offset startupOffset, Offset stopOffset) {
        this.startupOffset = startupOffset;
        this.stopOffset = stopOffset;
    }
}
