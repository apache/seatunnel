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

package org.apache.seatunnel.engine.server.resourcemanager.thirdparty.kubernetes;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.Objects;

/** Creates a Kubernetes resource manager for one application master. */
public final class KubernetesResourceManagerFactory implements ResourceManagerFactory {
    private final ApplicationId applicationId;
    private final ApplicationSpecification specification;
    private final String clusterName;
    private final ResourceManagerDriver driver;

    public KubernetesResourceManagerFactory(
            ApplicationId applicationId,
            ApplicationSpecification specification,
            String clusterName,
            ResourceManagerDriver driver) {
        this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
        this.specification = Objects.requireNonNull(specification, "specification");
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.driver = Objects.requireNonNull(driver, "driver");
    }

    @Override
    public DeployType getDeployType() {
        return DeployType.KUBERNETES;
    }

    @Override
    public ResourceManager createResourceManager(NodeEngine nodeEngine, EngineConfig engineConfig) {
        return new KubernetesResourceManager(
                nodeEngine,
                engineConfig,
                applicationId,
                specification,
                clusterName,
                nodeEngine.getClusterService().getLocalMember().getAddress(),
                driver);
    }
}
