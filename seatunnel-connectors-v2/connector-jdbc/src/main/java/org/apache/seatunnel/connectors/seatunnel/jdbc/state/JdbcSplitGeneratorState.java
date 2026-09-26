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

package org.apache.seatunnel.connectors.seatunnel.jdbc.state;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceSplit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Checkpointable cursor for lazy JDBC split generation.
 *
 * <p>Instead of persisting every remaining split, the enumerator stores the generator mode and the
 * current boundary so split planning can resume after recovery.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSplitGeneratorState implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Mode {
        /** One remaining full-table or pre-materialized split. */
        QUEUE,
        /** Arithmetic even distribution. */
        EVEN,
        /** Index probing for uneven keys. */
        UNEVEN_PROBE,
        /** Finished; no more splits. */
        FINISHED
    }

    private TablePath tablePath;
    private Mode mode;
    private String splitKeyName;
    private SeaTunnelDataType<?> splitKeyType;
    private Object minValue;
    private Object maxValue;
    /** Start of the next chunk to emit (inclusive lower bound for subsequent ranges). */
    private Object currentBoundary;

    private int chunkSize;
    private int dynamicChunkSize;
    private int nextSplitIndex;
    private boolean finished;
    private boolean emitFinalOpenEnded;
    /**
     * Remaining pre-materialized splits for QUEUE mode (DATE / STRING / Fixed fallback). Kept small
     * relative to full-table eager plans when lazy numeric paths are used.
     */
    @Builder.Default private List<JdbcSourceSplit> remainingQueue = new ArrayList<>();
}
