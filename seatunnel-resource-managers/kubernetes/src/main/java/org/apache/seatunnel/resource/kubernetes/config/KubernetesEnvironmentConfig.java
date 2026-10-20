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

import java.util.List;

/**
 * Read-only environment inputs for the shared Kubernetes workload decorator.
 *
 * <p>{@link org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator}
 * reads this view to add explicit {@code env} entries and whole {@code ConfigMap}/{@code Secret}
 * references ({@code envFrom}) to a master or worker Pod. Application and session configurations
 * both implement this interface so they get exactly the same environment handling.
 *
 * <p>Implementations expose parsed configuration only. They may return empty lists when no
 * environment customization is configured, and callers must not mutate the returned collections.
 */
public interface KubernetesEnvironmentConfig {

    /**
     * Returns explicit environment variables and value references for the Pod container.
     *
     * @return ordered environment entries, or an empty list when none are configured
     */
    List<KubernetesEnvironmentVariable> getEnvironmentVariables();

    /**
     * Returns ConfigMap and Secret references whose keys are exposed as environment variables.
     *
     * @return ordered {@code envFrom} sources, or an empty list when none are configured
     */
    List<KubernetesEnvFromReference> getEnvFromReferences();
}
