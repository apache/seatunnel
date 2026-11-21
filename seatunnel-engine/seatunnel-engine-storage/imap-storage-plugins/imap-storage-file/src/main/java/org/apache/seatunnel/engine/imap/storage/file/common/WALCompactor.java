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

package org.apache.seatunnel.engine.imap.storage.file.common;

import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionDeferredException;
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionPlan;
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionStorage;
import org.apache.seatunnel.engine.imap.storage.file.config.CompactionConfig;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.function.BooleanSupplier;

/** Reduces an explicit file set without depending on regions, writer lifecycle, or a backend. */
public final class WALCompactor {
    /**
     * Keeps the same latest-mutation order as replay, including deletes and expired puts. Neither
     * timestamps nor payloads are rewritten. Input reclamation is a separate post-commit operation.
     */
    public Path compact(CompactionPlan plan, CompactionStorage storage) throws IOException {
        return compact(
                plan,
                storage,
                CompactionConfig.MAX_MEMORY_BYTES.defaultValue(),
                CompactionConfig.MAX_KEYS.defaultValue(),
                () -> false);
    }

    /** Bounds retained mutations and observes cooperative cancellation before publication. */
    public Path compact(
            CompactionPlan plan,
            CompactionStorage storage,
            long maxMemoryBytes,
            long maxKeys,
            BooleanSupplier cancelled)
            throws IOException {
        if (maxMemoryBytes <= 0 || maxKeys <= 0) {
            throw new IllegalArgumentException("Compaction memory and key limits must be positive");
        }
        LatestMutationAccumulator accumulator = new LatestMutationAccumulator(null);
        for (Path input : plan.getInputs()) {
            checkCancelled(cancelled, plan);
            try {
                storage.read(
                        input,
                        mutation -> {
                            checkCancelled(cancelled, plan);
                            accumulator.acceptWithinLimits(mutation, maxMemoryBytes, maxKeys);
                        });
            } catch (CompactionDeferredException e) {
                throw new CompactionDeferredException(
                        e.getMessage() + " under " + plan.getOutputDirectory(), e);
            }
        }
        checkCancelled(cancelled, plan);
        if (accumulator.hasAmbiguousExpirationTimes()) {
            throw new CompactionDeferredException(
                    "Compaction deferred: equal WAL mutations have different expiration times in "
                            + plan.getOutputDirectory());
        }
        return storage.publish(plan, accumulator.values());
    }

    private static void checkCancelled(BooleanSupplier cancelled, CompactionPlan plan)
            throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted() || cancelled.getAsBoolean()) {
            throw new InterruptedIOException(
                    "IMap compaction stopped before publication under "
                            + plan.getOutputDirectory());
        }
    }
}
