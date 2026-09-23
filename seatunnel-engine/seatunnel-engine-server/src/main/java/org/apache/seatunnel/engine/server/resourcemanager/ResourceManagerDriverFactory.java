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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

/**
 * Service-provider boundary for application worker drivers, keeping SDK types outside the engine.
 *
 * <p>Providers are discovered through META-INF/services and require a public no-argument
 * constructor. A factory describes one deployment platform; each create call returns a fresh,
 * uninitialized driver owned by the caller. Factories must not retain mutable per-application
 * state.
 */
public interface ResourceManagerDriverFactory {

    /** @return the single deployment platform implemented by this provider */
    DeployType getDeployType();

    /**
     * Creates the worker driver without registering an application or allocating workers.
     *
     * @param specification immutable application settings for this provider's deployment platform
     * @return a new driver that the application runtime must initialize and eventually close
     * @throws Exception if configuration is invalid or platform client creation fails
     */
    ResourceManagerDriver create(ApplicationSpecification specification) throws Exception;
}
