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

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/** Submits an application through the platform factory selected by the injected service loader. */
public final class ApplicationClusterDeployer {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationClusterDeployer.class);

    private final ClusterClientServiceLoader clientServiceLoader;

    public ApplicationClusterDeployer(ClusterClientServiceLoader clientServiceLoader) {
        this.clientServiceLoader =
                Objects.requireNonNull(clientServiceLoader, "clientServiceLoader");
    }

    /**
     * Deploys one application and closes the local descriptor without stopping the application.
     *
     * <p>Platform connection/deployment options select and configure the descriptor. The separate
     * specification contains only the resolved application fields that must reach the master; the
     * deployer does not copy arbitrary options into that runtime payload.
     *
     * @param target resource platform to deploy to
     * @param options platform deployment settings, owned by the platform descriptor
     * @param specification resolved, platform-independent application requirements
     * @param <ID> native ID type of the selected platform
     * @return the platform ID, without opening an Engine client
     */
    public <ID> ID run(
            DeployType target, Map<String, String> options, ApplicationSpecification specification)
            throws Exception {
        Objects.requireNonNull(specification, "specification");
        LOG.info("Submitting application in Application Mode.");
        ApplicationClusterDescriptorFactory<ID> clientFactory =
                clientServiceLoader.getClusterClientFactory(target);
        try (ClusterDescriptor<ID> descriptor = clientFactory.create(options)) {
            return descriptor.deployApplication(specification);
        }
    }
}
