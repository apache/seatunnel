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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Validated, strongly typed Kubernetes parameters for one application.
 *
 * <p>{@link KubernetesOptions} declares the public configuration contract. This class parses
 * composite values and enforces relationships between options. Resource factories consume the
 * resolved values and contain no configuration parsing logic.
 */
@Getter
public final class KubernetesApplicationParameters {
    private static final String APPLICATION_LABEL = "seatunnel.apache.org/application-id";
    private static final String ROLE_LABEL = "seatunnel.apache.org/role";

    private final ApplicationSpecification specification;
    private final String image;
    private final String imagePullPolicy;
    private final List<String> imagePullSecrets;
    private final String serviceAccount;
    private final String seatunnelHome;
    private final String configMap;
    private final int retentionSeconds;
    private final String checkpointPvc;
    private final Map<String, String> masterLabels;
    private final Map<String, String> workerLabels;
    private final Map<String, String> masterAnnotations;
    private final Map<String, String> workerAnnotations;
    private final Map<String, String> masterNodeSelector;
    private final Map<String, String> workerNodeSelector;

    private KubernetesApplicationParameters(ApplicationSpecification specification) {
        this.specification = specification;
        this.image = requireNonBlank(specification, KubernetesOptions.IMAGE);
        this.imagePullPolicy = specification.getOption(KubernetesOptions.IMAGE_PULL_POLICY);
        if (!"Always".equals(imagePullPolicy)
                && !"IfNotPresent".equals(imagePullPolicy)
                && !"Never".equals(imagePullPolicy)) {
            throw new IllegalArgumentException("Invalid kubernetes.image-pull-policy");
        }
        this.imagePullSecrets =
                parseList(specification.getOption(KubernetesOptions.IMAGE_PULL_SECRETS));
        this.serviceAccount = requireNonBlank(specification, KubernetesOptions.SERVICE_ACCOUNT);
        this.seatunnelHome = requireNonBlank(specification, KubernetesOptions.SEATUNNEL_HOME);
        if (!seatunnelHome.startsWith("/") || seatunnelHome.contains(":")) {
            throw new IllegalArgumentException(
                    "kubernetes.seatunnel-home must be an absolute path without ':'");
        }
        this.configMap = optionalNonBlank(specification, KubernetesOptions.CONFIG_MAP);
        this.retentionSeconds = specification.getOption(KubernetesOptions.RETENTION_SECONDS);
        if (retentionSeconds < 1) {
            throw new IllegalArgumentException(
                    "kubernetes.finished-job-retention-seconds must be positive");
        }
        this.checkpointPvc = specification.getOption(KubernetesOptions.CHECKPOINT_PVC);
        if (checkpointPvc != null && checkpointPvc.trim().isEmpty()) {
            throw new IllegalArgumentException("kubernetes.checkpoint-pvc must not be empty");
        }
        this.masterLabels = labels(specification, KubernetesOptions.MASTER_LABELS);
        this.workerLabels = labels(specification, KubernetesOptions.WORKER_LABELS);
        this.masterAnnotations = pairs(specification, KubernetesOptions.MASTER_ANNOTATIONS);
        this.workerAnnotations = pairs(specification, KubernetesOptions.WORKER_ANNOTATIONS);
        this.masterNodeSelector = pairs(specification, KubernetesOptions.MASTER_NODE_SELECTOR);
        this.workerNodeSelector = pairs(specification, KubernetesOptions.WORKER_NODE_SELECTOR);
    }

    /**
     * Resolves platform settings before any Kubernetes resource is created.
     *
     * @param specification immutable application launch specification
     * @return parsed Kubernetes application settings
     * @throws IllegalArgumentException if an option is invalid or overrides an ownership label
     */
    public static KubernetesApplicationParameters from(ApplicationSpecification specification) {
        return new KubernetesApplicationParameters(specification);
    }

    private static String requireNonBlank(
            ApplicationSpecification specification, Option<String> option) {
        String value = specification.getOption(option);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option.key() + " is required");
        }
        return value;
    }

    private static String optionalNonBlank(
            ApplicationSpecification specification, Option<String> option) {
        String value = specification.getOption(option);
        if (value == null) {
            return null;
        }
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(option.key() + " must not be empty");
        }
        return value;
    }

    private static List<String> parseList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String item : value.split(",")) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(
                        "kubernetes.image-pull-secrets must not contain empty entries");
            }
            result.add(trimmed);
        }
        return Collections.unmodifiableList(result);
    }

    private static Map<String, String> labels(
            ApplicationSpecification specification, Option<String> option) {
        Map<String, String> labels = pairs(specification, option);
        if (labels.containsKey(APPLICATION_LABEL) || labels.containsKey(ROLE_LABEL)) {
            throw new IllegalArgumentException(
                    option.key() + " must not override SeaTunnel ownership labels");
        }
        return labels;
    }

    private static Map<String, String> pairs(
            ApplicationSpecification specification, Option<String> option) {
        String value = specification.getOption(option);
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : value.split(",")) {
            int separator = pair.indexOf(':');
            if (separator <= 0 || separator == pair.length() - 1) {
                throw new IllegalArgumentException(
                        option.key() + " must contain comma-separated key:value pairs");
            }
            String key = pair.substring(0, separator).trim();
            String configuredValue = pair.substring(separator + 1).trim();
            if (key.isEmpty()
                    || configuredValue.isEmpty()
                    || result.put(key, configuredValue) != null) {
                throw new IllegalArgumentException(
                        option.key() + " contains an empty or duplicate key:value pair");
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
