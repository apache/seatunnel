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

package org.apache.seatunnel.engine.client.deployment;

import org.apache.seatunnel.api.annotation.Experimental;
import org.apache.seatunnel.engine.common.runtime.DeployType;

import java.util.Map;

/**
 * Service-provider interface for selecting a platform application deployer.
 *
 * <p>Providers register this fully qualified interface name under {@code META-INF/services}. Engine
 * modules depend only on this contract; the provider's optional module owns all native SDK types.
 * Factories need a public no-argument constructor and should not create remote resources during
 * discovery. Exactly one factory may advertise a given deployment type on the selected classpath.
 */
@Experimental
public interface ApplicationClusterDescriptorFactory<ID> {

    /** @return the external resource platform handled by this provider */
    DeployType getDeployType();

    /** Converts a textual configuration or CLI identity to the platform's native ID. */
    ID parseApplicationId(String applicationId);

    /**
     * Creates the locally owned deployer for subsequent submit or retrieve operations.
     *
     * @param options resolved deployment options; the implementation must not modify or log them
     * @return a deployer whose lifetime is owned by the caller
     * @throws Exception if configuration or local platform-client initialization fails
     */
    ClusterDescriptor<ID> create(Map<String, String> options) throws Exception;
}
