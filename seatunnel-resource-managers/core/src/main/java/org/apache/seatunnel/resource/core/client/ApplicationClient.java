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

package org.apache.seatunnel.resource.core.client;

import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;

/**
 * Handle for an externally managed, single-job Zeta application.
 *
 * <p>The platform owns the application's identity and terminal status. Instances are connection
 * handles, not owners of application resources: {@link #close()} must not terminate a detached
 * application. Keep the creating deployer open for this handle's lifetime. Implementations are not
 * required to support concurrent method calls.
 */
public interface ApplicationClient extends AutoCloseable {
    /**
     * Returns the stable platform-assigned identity, also usable by a later CLI invocation.
     *
     * @return non-null application identity
     */
    ApplicationId getApplicationId();
    /**
     * Queries the platform for the current lifecycle state without waiting for job completion.
     *
     * @return the current state, or UNKNOWN when the platform cannot determine it
     * @throws Exception if the platform request or required terminal-resource cleanup fails
     */
    ApplicationStatus getStatus() throws Exception;
    /**
     * Retrieves the latest state and available platform diagnostics, including nonterminal states.
     *
     * <p>Providers may reconcile retained staging resources after observing a terminal state. They
     * must not stop an application merely because a client requests its result.
     *
     * @return application identity, state and diagnostics from the latest platform observation
     * @throws Exception if the platform cannot be queried or retained resources cannot be cleaned
     */
    ApplicationResult getResult() throws Exception;
    /**
     * Requests termination and cleanup of this application's external resources.
     *
     * <p>Platform deletion can be asynchronous. Repeated calls must tolerate resources that have
     * already been removed; provider documentation specifies terminal-metadata retention.
     *
     * @throws Exception if termination or cleanup cannot be requested successfully
     */
    void cancel() throws Exception;
    /**
     * Releases local connections and executors without canceling the remote application.
     *
     * @throws Exception if local resources cannot be released
     */
    @Override
    void close() throws Exception;
}
