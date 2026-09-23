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
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

import java.util.Map;

/**
 * Creates and retrieves an external application's control plane.
 *
 * <p>This client-side boundary corresponds to a cluster descriptor: it validates platform options,
 * localizes launch artifacts and submits the application master. Worker allocation after master
 * startup belongs to the server's resource-manager driver. Implementations must roll back resources
 * created by a failed submission, including an ambiguously accepted submission where possible.
 *
 * <p>The caller owns this deployer and all returned clients. Close clients before their deployer;
 * neither close operation may implicitly cancel an application. Concurrent calls are not required.
 */
public interface ApplicationDeployer extends AutoCloseable {
    /**
     * Submits exactly one job and returns after the external platform accepts the application.
     *
     * <p>Acceptance does not imply that workers are ready or the job is running. The returned
     * client exposes later status and cancellation. The specification is immutable and must not be
     * logged because resolved job options can contain credentials.
     *
     * @param specification job content, fixed worker resources and platform launch options
     * @return a client holding the newly assigned external application identity
     * @throws Exception if validation, artifact staging or platform submission fails
     */
    ApplicationClient deploy(ApplicationSpecification specification) throws Exception;
    /**
     * Creates a handle for an existing application without creating a master or any workers.
     *
     * @param applicationId identity previously returned by the selected platform
     * @param options platform connection and location options, including namespace or staging root
     * @return a client for querying or canceling that application
     * @throws Exception if the identity is for another platform or a client cannot be created
     */
    ApplicationClient retrieve(ApplicationId applicationId, Map<String, String> options)
            throws Exception;
    /**
     * Releases deployment-client resources while leaving submitted applications running.
     *
     * @throws Exception if local connections or executors cannot be closed
     */
    @Override
    void close() throws Exception;
}
