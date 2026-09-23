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

package org.apache.seatunnel.resource.kubernetes.cluster;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriverFactory;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApi;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

/** Creates in-cluster workers using only the application pod's service account. */
public final class KubernetesResourceManagerDriverFactory implements ResourceManagerDriverFactory {
    /** @return the Kubernetes deployment target advertised by this SPI provider */
    @Override
    public DeployType getDeployType() {
        return DeployType.KUBERNETES;
    }
    /**
     * Uses only the running master's service account, never the submitter's kubeconfig.
     *
     * @param specification immutable worker image and deployment options
     * @return driver whose SDK connection is released by its close lifecycle
     * @throws Exception if options are invalid or in-cluster credentials are unavailable
     */
    @Override
    public ResourceManagerDriver create(ApplicationSpecification specification) throws Exception {
        KubernetesOptions.validate(specification);
        return new KubernetesResourceManagerDriver(
                KubernetesApi.connect(specification.getOptions(), true), specification);
    }
}
