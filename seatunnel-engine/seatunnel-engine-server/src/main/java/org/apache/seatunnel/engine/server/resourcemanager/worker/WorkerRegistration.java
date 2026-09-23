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

package org.apache.seatunnel.engine.server.resourcemanager.worker;

import java.util.Objects;

/**
 * Immutable, thread-safe identity of a launched external worker allocation.
 *
 * <p>The creating driver owns the allocation until release or close. The identity may represent a
 * YARN container or Kubernetes pod; it does not imply that the worker has registered task slots.
 */
public final class WorkerRegistration {
    private final String workerId;

    /**
     * Creates an identity to be returned from a successful worker launch.
     *
     * @param workerId non-empty platform identifier, unique among this application's allocations
     * @throws NullPointerException if workerId is null
     * @throws IllegalArgumentException if workerId is blank
     */
    public WorkerRegistration(String workerId) {
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        if (workerId.trim().isEmpty()) {
            throw new IllegalArgumentException("workerId must not be empty");
        }
    }

    /** @return the platform allocation identity required for release and failure reporting */
    public String getWorkerId() {
        return workerId;
    }
}
