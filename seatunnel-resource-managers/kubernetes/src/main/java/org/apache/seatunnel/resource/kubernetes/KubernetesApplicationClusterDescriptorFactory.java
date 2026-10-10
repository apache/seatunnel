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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDescriptorFactory;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;

import com.google.auto.service.AutoService;

import java.util.Map;

/** Discovers the Kubernetes application deployment target. */
@AutoService(ApplicationClusterDescriptorFactory.class)
public final class KubernetesApplicationClusterDescriptorFactory
        implements ApplicationClusterDescriptorFactory<String> {

    @Override
    public String parseApplicationId(String applicationId) {
        if (applicationId == null || applicationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Kubernetes Job name must not be empty");
        }
        return applicationId;
    }

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
    public ClusterDescriptor<String> create(Map<String, String> options) throws Exception {
        KubernetesClient kubernetesClient = KubernetesClientFactory.create(options, false);
        return new KubernetesApplicationClusterDescriptor(kubernetesClient, options);
    }

    /**
     * Creates and starts one application without waiting for a live master or rolling it back when
     * its startup later fails. The returned Job name is still queryable through a shared client.
     *
     * @param specification resolved application fields
     * @param options namespace and optional submitter kubeconfig
     * @return native Kubernetes Job name
     * @throws Exception if configuration or Kubernetes resource creation fails
     */
    @VisibleForTesting
    public String deployApplicationWithoutWaitingForStartup(
            ApplicationSpecification specification, Map<String, String> options) throws Exception {
        try (KubernetesApplicationClusterDescriptor descriptor =
                (KubernetesApplicationClusterDescriptor) create(options)) {
            return descriptor.submitApplication(specification);
        }
    }
}
