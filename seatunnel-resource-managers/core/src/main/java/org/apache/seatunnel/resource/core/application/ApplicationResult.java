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

package org.apache.seatunnel.resource.core.application;

import lombok.Getter;

import java.util.Objects;

/** Application outcome, including platform diagnostics when available. */
@Getter
public final class ApplicationResult {
    /** Identity of the application whose state was observed. */
    private final ApplicationId applicationId;
    /** Latest observed state, which may be nonterminal. */
    private final ApplicationStatus status;
    /** Platform diagnostics; empty when unavailable. */
    private final String diagnostics;
    /**
     * Creates an immutable state snapshot, normalizing absent diagnostics to an empty string.
     *
     * @param applicationId non-null platform identity
     * @param status non-null current application state
     * @param diagnostics optional platform diagnostics
     */
    public ApplicationResult(
            ApplicationId applicationId, ApplicationStatus status, String diagnostics) {
        this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
        this.status = Objects.requireNonNull(status, "status");
        this.diagnostics = diagnostics == null ? "" : diagnostics;
    }
}
