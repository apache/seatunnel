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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;

import java.util.concurrent.CompletableFuture;

/**
 * Optional Source Reader contract for the engine-managed serialization lane.
 *
 * <p>All methods that expose or mutate checkpoint-visible reader state are invoked by one engine
 * event-loop owner. Implementations must not wait for remote operations from {@link
 * #pollNextManaged(Collector, PollContext)}.
 */
@Experimental
public interface ManagedSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {

    /**
     * Activates managed-runtime-only behavior before {@link SourceReader#open()} is invoked.
     *
     * <p>The engine invokes this callback exactly once for a Reader selected into the managed lane.
     * Legacy-lane Readers never receive it, which lets shared Reader bases preserve their
     * historical callback and locking behavior.
     */
    default void activateManagedRuntime() {}

    /**
     * Polls records within the supplied cooperative budget.
     *
     * <p>The method must return promptly when {@link PollContext#shouldYield()} becomes true. It
     * must not sleep while waiting for input.
     */
    PollStatus pollNextManaged(Collector<T> output, PollContext pollContext) throws Exception;

    /**
     * Returns a future completed when polling can make progress.
     *
     * <p>The returned future is a notification, not an ownership transfer. Implementations must
     * tolerate a subsequent poll finding no data.
     */
    CompletableFuture<Void> isAvailable();

    /**
     * Interrupts or wakes any connector operation that may delay cancellation beyond its declared
     * hard poll budget.
     */
    void wakeUp();
}
