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
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.kubernetes.KubernetesResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.yarn.YarnResourceManager;

import com.hazelcast.spi.impl.NodeEngine;

/**
 * Creates the master's Engine slot manager from the existing deployment-type selection.
 *
 * <p>This factory constructs a fresh, uninitialized manager on every call; the coordinator owns
 * initialization and closing. It does not cache managers, submit applications, or launch worker
 * processes. Application-mode provisioning uses {@link ResourceManagerDriverFactory} separately,
 * while the coordinator continues to use the default {@link StandaloneResourceManager} to allocate
 * slots on registered workers, including workers running in YARN containers or Kubernetes pods.
 *
 * <p>The factory retains references to the node engine and its configuration. Configure them before
 * creating managers and coordinate each returned manager's lifecycle independently.
 */
public class ResourceManagerFactory {

    private final NodeEngine nodeEngine;

    private final EngineConfig engineConfig;

    /**
     * Binds the factory to the master whose cluster operations and slot policy managers will use.
     *
     * @param nodeEngine owning master's Hazelcast node engine
     * @param engineConfig Engine execution mode and slot-allocation configuration
     */
    public ResourceManagerFactory(NodeEngine nodeEngine, EngineConfig engineConfig) {
        this.nodeEngine = nodeEngine;
        this.engineConfig = engineConfig;
    }

    /**
     * Constructs the existing Engine manager selected by deployment type without initializing it.
     *
     * <p>The YARN and Kubernetes branches retain the legacy manager classes for compatibility.
     * Their third-party worker lifecycle methods are placeholders; selecting them does not enable
     * the application-mode driver or deploy an application.
     *
     * @param type existing manager selection: STANDALONE, YARN, or KUBERNETES
     * @return a fresh slot manager whose caller must initialize and eventually close
     * @throws UnsupportedDeployTypeException if the type is null or unsupported
     */
    public ResourceManager getResourceManager(DeployType type) {
        if (DeployType.STANDALONE.equals(type)) {
            return new StandaloneResourceManager(nodeEngine, engineConfig);
        } else if (DeployType.KUBERNETES.equals(type)) {
            return new KubernetesResourceManager(nodeEngine, engineConfig);
        } else if (DeployType.YARN.equals(type)) {
            return new YarnResourceManager(nodeEngine, engineConfig);
        } else {
            throw new UnsupportedDeployTypeException(type);
        }
    }

    /**
     * Constructs the default manager used by the coordinator to schedule registered worker slots.
     *
     * <p>STANDALONE identifies this Engine implementation, not the external platform hosting the
     * workers. Application mode uses the same slot manager after its driver launches the workers.
     *
     * @return a fresh, uninitialized standalone slot manager owned by the caller
     */
    public ResourceManager getResourceManager() {
        return this.getResourceManager(DeployType.STANDALONE);
    }
}
