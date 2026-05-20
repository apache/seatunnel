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

package org.apache.seatunnel.api.common.error;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/** Row-level error event reported by connectors during flush/commit/close operations. */
public final class RowErrorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowErrorPhase phase;
    private final Long checkpointId;
    private final SeaTunnelRow row;
    private final Throwable error;

    public RowErrorEvent(RowErrorPhase phase, SeaTunnelRow row, Throwable error) {
        this(phase, null, row, error);
    }

    public RowErrorEvent(
            RowErrorPhase phase, Long checkpointId, SeaTunnelRow row, Throwable error) {
        this.phase = Objects.requireNonNull(phase, "phase must not be null");
        this.row = Objects.requireNonNull(row, "row must not be null");
        this.error = Objects.requireNonNull(error, "error must not be null");
        this.checkpointId = checkpointId;
    }

    public RowErrorPhase getPhase() {
        return phase;
    }

    public Optional<Long> getCheckpointId() {
        return Optional.ofNullable(checkpointId);
    }

    public SeaTunnelRow getRow() {
        return row;
    }

    public Throwable getError() {
        return error;
    }
}
