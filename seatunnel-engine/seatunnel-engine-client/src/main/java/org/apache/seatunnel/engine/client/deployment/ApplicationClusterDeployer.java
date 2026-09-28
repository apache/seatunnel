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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param specification resolved job content and platform deployment options
     * @param <ID> native ID type of the selected platform
     * @return the platform ID, without opening an Engine client
     */
    public <ID> ID run(ApplicationSpecification specification) throws Exception {
        Objects.requireNonNull(specification, "specification");
        LOG.info("Submitting application in Application Mode.");
        ApplicationClusterDescriptorFactory<ID> clientFactory =
                clientServiceLoader.getClusterClientFactory(specification.getDeployType());
        try (ClusterDescriptor<ID> descriptor = clientFactory.create(specification.getOptions())) {
            return descriptor.deployApplication(specification);
        }
    }
}
