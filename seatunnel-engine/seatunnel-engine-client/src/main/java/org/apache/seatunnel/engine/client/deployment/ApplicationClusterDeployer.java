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
    private final DeployType deployType;
    private final Map<String, String> options;
    private final ApplicationSpecification specification;

    public ApplicationClusterDeployer(
            DeployType deployType,
            ApplicationSpecification specification,
            Map<String, String> options) {
        this(new ClusterClientServiceLoader(), deployType, specification, options);
    }

    public ApplicationClusterDeployer(
            ClusterClientServiceLoader clientServiceLoader,
            DeployType deployType,
            ApplicationSpecification specification,
            Map<String, String> options) {
        this.clientServiceLoader =
                Objects.requireNonNull(clientServiceLoader, "clientServiceLoader");
        this.specification = Objects.requireNonNull(specification, "specification");
        this.deployType = Objects.requireNonNull(deployType, "deployType;");
        this.options = options;
    }

    /**
     * Deploys one application and closes the local descriptor without stopping the application.
     *
     * <p>Platform connection/deployment options select and configure the descriptor. The separate
     * specification contains only the resolved application fields that must reach the master; the
     * deployer does not copy arbitrary options into that runtime payload.
     *
     * @param <ID> native ID type of the selected platform
     * @return the platform ID, without opening an Engine client
     */
    public <ID> ID run() throws Exception {
        LOG.info("Submitting application in Application Mode.");
        ApplicationClusterDescriptorFactory<ID> clientFactory =
                clientServiceLoader.getClusterClientFactory(deployType);
        try (ClusterDescriptor<ID> descriptor = clientFactory.create(options)) {
            return descriptor.deployApplication(specification);
        }
    }
}
