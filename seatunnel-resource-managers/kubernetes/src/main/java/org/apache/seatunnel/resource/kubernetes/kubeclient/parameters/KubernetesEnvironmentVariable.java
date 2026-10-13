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

package org.apache.seatunnel.resource.kubernetes.kubeclient.parameters;

import lombok.Getter;

import java.util.Objects;

/**
 * One environment variable declared with {@code kubernetes.env}.
 *
 * <p>Supported forms are a literal {@code NAME=value}, a Secret reference {@code
 * NAME=secret:<name>/<key>} and a ConfigMap reference {@code NAME=configmap:<name>/<key>}. The
 * referenced values are injected by Kubernetes and can therefore resolve job configuration
 * placeholders inside the Pod.
 */
@Getter
public final class KubernetesEnvironmentVariable {
    private static final String SECRET_PREFIX = "secret:";
    private static final String CONFIG_MAP_PREFIX = "configmap:";
    private static final String ENV_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]*";

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
     * Parses one {@code kubernetes.env} entry.
     *
     * @param entry raw comma-separated entry
     * @return parsed environment variable
     * @throws IllegalArgumentException if the entry is malformed
     */
    public static KubernetesEnvironmentVariable parse(String entry) {
        int separator = entry.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException(
                    "kubernetes.env must contain comma-separated NAME=value entries");
        }
        String name = entry.substring(0, separator).trim();
        String value = entry.substring(separator + 1).trim();
        if (!name.matches(ENV_NAME_PATTERN)) {
            throw new IllegalArgumentException(
                    "kubernetes.env contains an invalid environment variable name: " + name);
        }
        if (value.startsWith(SECRET_PREFIX)) {
            return reference(name, value.substring(SECRET_PREFIX.length()), true);
        }
        if (value.startsWith(CONFIG_MAP_PREFIX)) {
            return reference(name, value.substring(CONFIG_MAP_PREFIX.length()), false);
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

    private static KubernetesEnvironmentVariable reference(
            String name, String reference, boolean secret) {
        int separator = reference.indexOf('/');
        if (separator <= 0
                || separator == reference.length() - 1
                || reference.indexOf('/', separator + 1) >= 0) {
            throw new IllegalArgumentException(
                    "kubernetes.env references must use the form name/key: " + reference);
        }
        String resourceName = reference.substring(0, separator).trim();
        String key = reference.substring(separator + 1).trim();
        if (resourceName.isEmpty() || key.isEmpty()) {
            throw new IllegalArgumentException(
                    "kubernetes.env references must use the form name/key: " + reference);
        }
        return secret
                ? new KubernetesEnvironmentVariable(name, null, resourceName, null, key)
                : new KubernetesEnvironmentVariable(name, null, null, resourceName, key);
    }
}
