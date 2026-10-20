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

import java.util.Map;

/**
 * Read-only Secret inputs for the shared Kubernetes workload decorator.
 *
 * <p>{@link org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator} uses
 * this view to decide whether a Secret is mounted, which Secret name to reference, and where to
 * mount it. Application mode may create a Secret from inline data or reference an existing one;
 * session mode only references a user-managed Secret. Both modes share the same mount behavior.
 *
 * <p>Implementations expose configuration only. They must not read Secret values from Kubernetes;
 * the decorator puts the reference and optional data into the Pod and Secret specifications.
 */
public interface KubernetesSecretConfig {

    /**
     * Returns whether the workload should mount an application or session Secret.
     *
     * @return {@code true} when a Secret reference or inline Secret data is configured
     */
    boolean hasApplicationSecret();

    /**
     * Resolves the Secret resource name for a deployment.
     *
     * @param applicationId generated deployment identifier used as the default name
     * @return configured Secret name, or the generated name when no explicit name is configured
     */
    String resolvedApplicationSecretName(String applicationId);

    /**
     * Returns inline Secret entries that the decorator may create or merge into the workload.
     *
     * @return key/value data, or an empty map when the Secret is only referenced
     */
    Map<String, String> getApplicationSecretData();

    /**
     * Returns the directory where Secret keys are mounted in the container.
     *
     * @return absolute mount directory
     */
    String getSecretDirectory();
}
