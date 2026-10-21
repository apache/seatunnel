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

package org.apache.seatunnel.resource.kubernetes.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Read-only Pod-template inputs shared by application and session workload factories.
 *
 * <p>{@link org.apache.seatunnel.resource.kubernetes.kubeclient.factory.AbstractWorkloadFactory}
 * uses this view to build the common container and Pod skeleton: image, service account, image pull
 * secrets, node selectors, tolerations, and optional SeaTunnel/Log4j configuration paths. Role
 * specific fields such as probes, commands, and owner references stay in the concrete factory.
 *
 * <p>Implementations are parsed deployment configurations. Collections may be empty when a feature
 * is not configured; optional configuration paths may be {@code null}, in which case the factory
 * omits the corresponding JVM system property.
 */
public interface KubernetesWorkloadConfig {

    /**
     * Returns the container image used for master and worker Pods.
     *
     * @return image reference
     */
    String getImage();

    /**
     * Returns the Kubernetes image pull policy.
     *
     * @return pull policy such as {@code IfNotPresent} or {@code Never}
     */
    String getImagePullPolicy();

    /**
     * Returns the SeaTunnel installation directory inside the container.
     *
     * @return absolute installation path
     */
    String getSeatunnelHome();

    /**
     * Returns the service account used by workload Pods.
     *
     * @return service account name
     */
    String getServiceAccount();

    /**
     * Returns image pull secret names attached to workload Pods.
     *
     * @return pull secret names, or an empty list when none are configured
     */
    List<String> getImagePullSecrets();

    /**
     * Returns node selector labels applied to master Pods.
     *
     * @return label map, or an empty map when no selector is configured
     */
    default Map<String, String> getMasterNodeSelector() {
        return Collections.emptyMap();
    }

    /**
     * Returns node selector labels applied to worker Pods.
     *
     * @return label map, or an empty map when no selector is configured
     */
    default Map<String, String> getWorkerNodeSelector() {
        return Collections.emptyMap();
    }

    /**
     * Returns tolerations applied to master Pods.
     *
     * @return tolerations, or an empty list when none are configured
     */
    default List<KubernetesToleration> getMasterTolerations() {
        return Collections.emptyList();
    }

    /**
     * Returns tolerations applied to worker Pods.
     *
     * @return tolerations, or an empty list when none are configured
     */
    default List<KubernetesToleration> getWorkerTolerations() {
        return Collections.emptyList();
    }

    /**
     * Returns an optional SeaTunnel configuration file path for a JVM system property.
     *
     * @return configuration path, or {@code null} to use the image default
     */
    default String getSeatunnelConfigFile() {
        return null;
    }

    /**
     * Returns an optional Log4j configuration file path for a JVM system property.
     *
     * @return configuration path, or {@code null} to use the image default
     */
    default String getLog4jConfigFile() {
        return null;
    }
}
