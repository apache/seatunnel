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

package org.apache.seatunnel.resource.kubernetes.client;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.client.ApplicationDeployer;
import org.apache.seatunnel.resource.core.client.ApplicationDeployerFactory;

import java.util.Map;

/** Discovers the Kubernetes application deployment target. */
public final class KubernetesApplicationDeployerFactory implements ApplicationDeployerFactory {
    /** @return the Kubernetes deployment target advertised by this SPI provider */
    @Override
    public DeployType getDeployType() {
        return DeployType.KUBERNETES;
    }
    /**
     * Opens one SDK connection whose lifetime belongs to the returned deployer.
     *
     * @param options namespace and optional submitter kubeconfig
     * @return deployer for creating or retrieving Kubernetes applications
     * @throws Exception if Kubernetes credentials or configuration cannot be loaded
     */
    @Override
    public ApplicationDeployer create(Map<String, String> options) throws Exception {
        return new KubernetesApplicationDeployer(KubernetesApi.connect(options, false));
    }
}
