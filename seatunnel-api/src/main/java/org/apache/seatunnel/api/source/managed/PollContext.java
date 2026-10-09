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

package org.apache.seatunnel.api.source.managed;

import org.apache.seatunnel.api.annotation.Experimental;

/**
 * The engine-owned budget for one managed source reader poll turn.
 *
 * <p>The engine updates the record and byte counters when the collector emits a record. A managed
 * reader must stop before starting another potentially blocking unit of work once {@link
 * #shouldYield()} returns {@code true}.
 */
@Experimental
public interface PollContext {

    /** Returns the number of records that may still be emitted in this turn. */
    int remainingRecords();

    /** Returns the estimated number of payload bytes that may still be emitted in this turn. */
    long remainingBytes();

    /** Returns the monotonic deadline, expressed as {@link System#nanoTime()} nanoseconds. */
    long deadlineNanos();

    /** Returns whether the current cooperative poll turn must yield to engine control work. */
    boolean shouldYield();

    /**
     * Records one emitted item.
     *
     * <p>This method is called by the engine collector. Connector implementations must not call it
     * directly.
     */
    void recordEmitted(long estimatedBytes);
}
