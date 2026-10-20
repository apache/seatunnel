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

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.SessionSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.function.Supplier;

/**
 * Creates the standalone, application or session resource manager owned by one Engine coordinator.
 */
public class ResourceManagerFactory {
    private final DeployType deployType;
    private final String applicationId;
    private final ApplicationSpecification specification;
    private final ResourceManagerDriver<?> driver;
    private final SessionSpecification sessionSpecification;

    /** Creates a factory for the default standalone deployment. */
    public ResourceManagerFactory() {
        this(DeployType.STANDALONE, null, null, null, null);
    }

    /**
     * Creates a factory for a Kubernetes session cluster.
     *
     * @param sessionSpecification immutable session worker bounds and template settings
     * @param driver platform driver that creates and adopts workers
     */
    public ResourceManagerFactory(
            SessionSpecification sessionSpecification, ResourceManagerDriver<?> driver) {
        this(DeployType.KUBERNETES_SESSION, null, null, driver, sessionSpecification);
    }

    /**
     * Receives application dependencies prepared by the platform entrypoint.
     *
     * <p>The factory is passed through member creation before a NodeEngine exists. It neither
     * discovers platform SDKs nor initializes the driver. The caller owns the driver until the
     * created manager is initialized.
     */
    public ResourceManagerFactory(
            DeployType deployType,
            String applicationId,
            ApplicationSpecification specification,
            ResourceManagerDriver<?> driver) {
        this(deployType, applicationId, specification, driver, null);
    }

    private ResourceManagerFactory(
            DeployType deployType,
            String applicationId,
            ApplicationSpecification specification,
            ResourceManagerDriver<?> driver,
            SessionSpecification sessionSpecification) {
        this.deployType = deployType;
        this.applicationId = applicationId;
        this.specification = specification;
        this.driver = driver;
        this.sessionSpecification = sessionSpecification;
    }

    /** @return whether this deployment manages a long-lived worker pool itself */
    public boolean isSessionDeployment() {
        return DeployType.KUBERNETES_SESSION.equals(deployType);
    }

    /**
     * Initializes deployments whose worker pool must exist before the first job is restored.
     *
     * <p>Session managers create their configured {@code min-workers} during initialization.
     * Standalone managers stay lazy because their workers are already cluster members, and
     * application managers stay lazy because their entrypoint owns the application lifecycle. The
     * factory keeps this policy so the coordinator does not need deployment-specific branches.
     *
     * @param resourceManager lazy accessor for the coordinator-owned manager
     */
    public void initializeWorkerPoolOnCoordinatorStart(Supplier<ResourceManager> resourceManager) {
        if (isSessionDeployment()) {
            resourceManager.get();
        }
    }

    /** Creates an uninitialized manager once its node is available; the coordinator owns it. */
    public ResourceManager createResourceManager(NodeEngine nodeEngine, EngineConfig engineConfig) {
        if (DeployType.STANDALONE.equals(deployType)) {
            return new StandaloneResourceManager(nodeEngine, engineConfig);
        }
        if (DeployType.KUBERNETES.equals(deployType)) {
            if (applicationId == null || specification == null || driver == null) {
                throw new IllegalStateException(
                        "Application deployment requires an application ID, specification and driver");
            }
            return new ApplicationResourceManager<>(
                    nodeEngine, engineConfig, applicationId, specification, driver);
        }
        if (DeployType.KUBERNETES_SESSION.equals(deployType)) {
            if (sessionSpecification == null || driver == null) {
                throw new IllegalStateException(
                        "Session deployment requires a session specification and driver");
            }
            return createSessionResourceManager(nodeEngine, engineConfig);
        }
        throw new UnsupportedDeployTypeException(deployType);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ResourceManager createSessionResourceManager(
            NodeEngine nodeEngine, EngineConfig engineConfig) {
        return new SessionResourceManager(nodeEngine, engineConfig, sessionSpecification, driver);
    }
}
