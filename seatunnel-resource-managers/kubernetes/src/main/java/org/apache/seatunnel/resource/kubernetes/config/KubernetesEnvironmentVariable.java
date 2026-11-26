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

import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import lombok.Getter;

import java.util.Objects;

/**
 * One environment variable declared with {@code kubernetes.env.<NAME>}.
 *
 * <p>The value is either a literal, a Secret reference {@code secret:<name>/<key>} or a ConfigMap
 * reference {@code configmap:<name>/<key>}. Referenced values are injected by Kubernetes and can
 * therefore resolve job configuration placeholders inside the Pod.
 */
@Getter
public final class KubernetesEnvironmentVariable {
    private final String name;
    private final String literalValue;
    private final String secretName;
    private final String configMapName;
    private final String key;

    private KubernetesEnvironmentVariable(
            String name, String literalValue, String secretName, String configMapName, String key) {
        this.name = Objects.requireNonNull(name, "name");
        this.literalValue = literalValue;
        this.secretName = secretName;
        this.configMapName = configMapName;
        this.key = key;
    }

    /**
     * Parses the configured value of one environment variable.
     *
     * @param name validated environment variable name
     * @param value literal value or a {@code secret:}/{@code configmap:} reference
     * @return parsed environment variable
     * @throws IllegalArgumentException if a reference is malformed
     */
    public static KubernetesEnvironmentVariable fromValue(String name, String value) {
        if (value.startsWith(KubernetesConstants.ENV_VALUE_SECRET_PREFIX)) {
            return reference(
                    name,
                    value.substring(KubernetesConstants.ENV_VALUE_SECRET_PREFIX.length()),
                    true);
        }
        if (value.startsWith(KubernetesConstants.ENV_VALUE_CONFIGMAP_PREFIX)) {
            return reference(
                    name,
                    value.substring(KubernetesConstants.ENV_VALUE_CONFIGMAP_PREFIX.length()),
                    false);
        }
        return new KubernetesEnvironmentVariable(name, value, null, null, null);
    }

    /** @return whether the value is injected from a Secret */
    public boolean isSecret() {
        return secretName != null;
    }

    /** @return whether the value is injected from a ConfigMap */
    public boolean isConfigMap() {
        return configMapName != null;
    }

    /** @return the literal value, or {@code null} for referenced variables */
    public String serializeValue() {
        if (isSecret()) {
            return KubernetesConstants.ENV_VALUE_SECRET_PREFIX + secretName + "/" + key;
        }
        if (isConfigMap()) {
            return KubernetesConstants.ENV_VALUE_CONFIGMAP_PREFIX + configMapName + "/" + key;
        }
        return literalValue;
    }

    private static KubernetesEnvironmentVariable reference(
            String name, String reference, boolean secret) {
        int separator = reference.indexOf('/');
        if (separator <= 0
                || separator == reference.length() - 1
                || reference.indexOf('/', separator + 1) >= 0) {
            throw new IllegalArgumentException(
                    KubernetesConstants.ENV_PREFIX
                            + name
                            + " references must use the form name/key");
        }
        String resourceName = reference.substring(0, separator).trim();
        String key = reference.substring(separator + 1).trim();
        if (resourceName.isEmpty() || key.isEmpty()) {
            throw new IllegalArgumentException(
                    KubernetesConstants.ENV_PREFIX
                            + name
                            + " references must use the form name/key");
        }
        return secret
                ? new KubernetesEnvironmentVariable(name, null, resourceName, null, key)
                : new KubernetesEnvironmentVariable(name, null, null, resourceName, key);
    }
}
