/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.kubernetes.configuration;


import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/** A class containing all the supported deployment target names for Kubernetes. */
public enum KubernetesDeploymentTarget {
    SESSION("kubernetes-session"),
    APPLICATION("kubernetes-application");

    private final String name;

    KubernetesDeploymentTarget(final String name) {
        this.name = checkNotNull(name);
    }

    public static KubernetesDeploymentTarget fromConfig(final SeaTunnelConfig configuration) {
        return null;
    }

    public String getName() {
        return name;
    }

    public static boolean isValidKubernetesTarget(final String configValue) {
        return configValue != null
                && Arrays.stream(KubernetesDeploymentTarget.values())
                        .anyMatch(
                                kubernetesDeploymentTarget ->
                                        kubernetesDeploymentTarget.name.equalsIgnoreCase(
                                                configValue));
    }

    private static KubernetesDeploymentTarget getFromName(final String deploymentTarget) {
        if (deploymentTarget == null) {
            return null;
        }

        if (SESSION.name.equalsIgnoreCase(deploymentTarget)) {
            return SESSION;
        } else if (APPLICATION.name.equalsIgnoreCase(deploymentTarget)) {
            return APPLICATION;
        }
        return null;
    }

    private static String options() {
        return Arrays.stream(KubernetesDeploymentTarget.values())
                .map(KubernetesDeploymentTarget::getName)
                .collect(Collectors.joining(","));
    }
}
