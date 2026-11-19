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

import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionPlan;
import org.apache.seatunnel.engine.imap.storage.file.compaction.CompactionStorage;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** Reduces an explicit file set without depending on regions, writer lifecycle, or a backend. */
public final class WALCompactor {
    /**
     * Keeps the same latest-mutation order as replay, including deletes and expired puts. Neither
     * timestamps nor payloads are rewritten. Input reclamation is a separate post-commit operation.
     */
    public Path compact(CompactionPlan plan, CompactionStorage storage) throws IOException {
        LatestMutationAccumulator accumulator = new LatestMutationAccumulator(null);
        for (Path input : plan.getInputs()) {
            storage.read(input, accumulator::accept);
        }
        if (accumulator.hasAmbiguousExpirationTimes()) {
            throw new IOException(
                    "Compaction deferred: equal WAL mutations have different expiration times in "
                            + plan.getOutputDirectory());
        }
        return storage.publish(plan, accumulator.values());
    }
}
