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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/** Callback for intercepting row-level errors from MultiTableSinkWriter sub-writers. */
@FunctionalInterface
public interface MultiTableRowErrorHandler {

    /**
     * Handles a row-level error from a sink writer.
     *
     * @param writer the sink writer that threw the error
     * @param tableId table identifier; may be null
     * @param row the failed row
     * @param t the exception
     * @return true if error handled, false if fatal
     */
    boolean handleRowError(
            SinkWriter<SeaTunnelRow, ?, ?> writer, String tableId, SeaTunnelRow row, Throwable t);

    /**
     * Marks the beginning of a row write whose terminal outcome may be reported by the writer's row
     * error collector instead of the direct write callback.
     */
    default void beginCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {}

    /**
     * Consumes a terminal row-error outcome that a sub-writer reported internally while returning
     * normally from write.
     *
     * @return true when the row already has an error terminal outcome and must not be counted as a
     *     successful write
     */
    default boolean consumeCollectedRowErrorOutcome(SeaTunnelRow row) {
        return false;
    }

    /** Clears a pending terminal-outcome probe when the direct write path throws before consume. */
    default void clearCollectedRowErrorOutcomeProbe(SeaTunnelRow row) {}
}
