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

package org.apache.seatunnel.engine.server.task.error;

import java.io.Serializable;

/** Counter abstraction for row-level error threshold accounting. */
public interface ErrorHandlerCounter extends Serializable {

    /**
     * Records one successfully observed input row for threshold checks.
     *
     * @return the counter value visible to this handler after the increment
     */
    long incrementTotalRecords();

    /**
     * Records one row-level error for threshold checks.
     *
     * @return the counter value visible to this handler after the increment
     */
    long incrementErrorRecords();

    /** Returns the total-record count visible to this handler. */
    long getTotalRecords();

    /** Returns the error-record count visible to this handler. */
    long getErrorRecords();

    /**
     * Captures local counter deltas for the given checkpoint without publishing them globally yet.
     */
    default void snapshotState(long checkpointId) {}

    /** Publishes deltas captured for the completed checkpoint. */
    default void notifyCheckpointComplete(long checkpointId) {}

    /** Drops deltas captured for an aborted checkpoint. */
    default void notifyCheckpointAborted(long checkpointId) {}
}
