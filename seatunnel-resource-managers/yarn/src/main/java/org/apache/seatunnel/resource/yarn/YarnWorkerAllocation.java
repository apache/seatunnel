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

package org.apache.seatunnel.resource.yarn;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/** One RM-granted container, tracked until its worker exits or the driver releases it. */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class YarnWorkerAllocation {
    @Getter(AccessLevel.PACKAGE)
    private final YarnWorkerNode node;

    @Getter(AccessLevel.PACKAGE)
    private final YarnWorkerRequest request;

    /** Unblocks release with whether the NM may have received a start request. */
    private final CompletableFuture<Boolean> launchFinished = new CompletableFuture<>();

    /** Guarded by the driver lock. A failed release remains owned here until a caller retries. */
    private CompletableFuture<Void> release;

    /**
     * Signals that launch has finished and any waiting release can proceed.
     *
     * @param startAttempted whether the start RPC was attempted, including a call that failed after
     *     the NodeManager may have accepted it
     */
    void finishLaunch(boolean startAttempted) {
        launchFinished.complete(startAttempted);
    }

    /** Returns whether an explicit release owns cleanup, including a release awaiting retry. */
    boolean isReleasing() {
        return release != null;
    }

    /**
     * Schedules cleanup after launch completes. Under the driver lock, concurrent callers reuse the
     * pending release and a later caller can retry a failed release.
     *
     * @param executor driver's I/O executor for stopping the container and returning the allocation
     * @param cleanup receives whether the NodeManager start RPC was attempted
     * @return the shared cleanup future owned by the driver
     */
    CompletableFuture<Void> releaseAfterLaunch(Executor executor, Consumer<Boolean> cleanup) {
        if (release == null || release.isCompletedExceptionally()) {
            release = launchFinished.thenAcceptAsync(cleanup, executor);
        }
        return release;
    }
}
