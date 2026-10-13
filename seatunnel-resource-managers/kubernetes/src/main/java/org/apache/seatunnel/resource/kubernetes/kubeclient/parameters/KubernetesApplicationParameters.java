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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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
    private static final String SECRET_DATA_PREFIX = "kubernetes.secret.data.";

    private final ApplicationSpecification specification;
    private final String namespace;
    private final String image;
    private final ImagePullPolicy imagePullPolicy;
    private final List<String> imagePullSecrets;
    private final String serviceAccount;
    private final String seatunnelHome;
    private final String configMap;
    private final String applicationSecretRef;
    private final String applicationSecretName;
    private final Map<String, String> applicationSecretData;
    private final int retentionSeconds;
    private final String checkpointPvc;
    private final Map<String, String> masterLabels;
    private final Map<String, String> workerLabels;
    private final Map<String, String> masterAnnotations;
    private final Map<String, String> workerAnnotations;
    private final Map<String, String> masterNodeSelector;
    private final Map<String, String> workerNodeSelector;

    private KubernetesApplicationParameters(
            ApplicationSpecification specification, ReadonlyConfig options) {
        this.specification = specification;
        this.namespace = requireNonBlank(options, KubernetesOptions.NAMESPACE);
        this.image = requireNonBlank(options, KubernetesOptions.IMAGE);
        this.imagePullPolicy =
                ImagePullPolicy.fromValue(options.get(KubernetesOptions.IMAGE_PULL_POLICY));
        this.imagePullSecrets = parseList(options.get(KubernetesOptions.IMAGE_PULL_SECRETS));
        this.serviceAccount = requireNonBlank(options, KubernetesOptions.SERVICE_ACCOUNT);
        this.seatunnelHome = requireNonBlank(options, KubernetesOptions.SEATUNNEL_HOME);
        if (!seatunnelHome.startsWith("/") || seatunnelHome.contains(":")) {
            throw new IllegalArgumentException(
                    "kubernetes.seatunnel-home must be an absolute path without ':'");
        }
        this.configMap = optionalNonBlank(options, KubernetesOptions.CONFIG_MAP);
        this.applicationSecretRef = optionalNonBlank(options, KubernetesOptions.SECRET_REF);
        this.applicationSecretName = optionalNonBlank(options, KubernetesOptions.SECRET_NAME);
        this.applicationSecretData = secretData(rawOptions(options));
        validateApplicationSecret();
        this.retentionSeconds = options.get(KubernetesOptions.RETENTION_SECONDS);
        if (retentionSeconds < 1) {
            throw new IllegalArgumentException(
                    "kubernetes.finished-job-retention-seconds must be positive");
        }
        this.checkpointPvc = optionalNonBlank(options, KubernetesOptions.CHECKPOINT_PVC);
        this.masterLabels = labels(options, KubernetesOptions.MASTER_LABELS);
        this.workerLabels = labels(options, KubernetesOptions.WORKER_LABELS);
        this.masterAnnotations = pairs(options, KubernetesOptions.MASTER_ANNOTATIONS);
        this.workerAnnotations = pairs(options, KubernetesOptions.WORKER_ANNOTATIONS);
        this.masterNodeSelector = pairs(options, KubernetesOptions.MASTER_NODE_SELECTOR);
        this.workerNodeSelector = pairs(options, KubernetesOptions.WORKER_NODE_SELECTOR);
    }

    /**
     * Resolves platform settings before any Kubernetes resource is created.
     *
     * @param specification immutable application launch specification
     * @param options platform deployment settings, including dynamic kubernetes.secret.data keys
     * @return parsed Kubernetes application settings
     * @throws IllegalArgumentException if an option is invalid or overrides an ownership label
     */
    public static KubernetesApplicationParameters from(
            ApplicationSpecification specification, ReadonlyConfig options) {
        return new KubernetesApplicationParameters(specification, options);
    }

    /**
     * Reads the master's localized application specification from a local file.
     *
     * <p>Common application fields are decoded by SeatunnelApplicationConfig; this class restores
     * the Kubernetes fields needed to create workers. Credentials come from the master's service
     * account, not the submitter-local kubeconfig.
     *
     * @param path localized application properties
     * @return parsed Kubernetes application settings
     * @throws IOException if the file cannot be read or decoded
     */
    public static KubernetesApplicationParameters read(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return read(properties, null);
    }

    /**
     * Reads the master's Base64 encoded localized specification, optionally replacing the inline
     * job configuration with a file mounted from a user-managed Secret.
     *
     * @param encodedSpecification value of {@link
     *     org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants#SPECIFICATION_ENV}
     * @param jobConfigOverride mounted job configuration file, or {@code null} to use the inline
     *     one
     * @return parsed Kubernetes application settings
     * @throws IOException if the specification is missing, malformed or cannot be read
     */
    public static KubernetesApplicationParameters read(
            String encodedSpecification, Path jobConfigOverride) throws IOException {
        if (encodedSpecification == null || encodedSpecification.trim().isEmpty()) {
            throw new IOException("Missing localized application specification");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(encodedSpecification);
        } catch (IllegalArgumentException failure) {
            throw new IOException("Invalid localized application specification encoding", failure);
        }
        Properties properties = new Properties();
        try (Reader reader =
                new InputStreamReader(new ByteArrayInputStream(decoded), StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return read(properties, jobConfigOverride);
    }

    private static KubernetesApplicationParameters read(
            Properties properties, Path jobConfigOverride) throws IOException {
        if (jobConfigOverride != null) {
            properties.setProperty(
                    SeatunnelApplicationConfig.JOB_CONFIG_KEY,
                    new String(Files.readAllBytes(jobConfigOverride), StandardCharsets.UTF_8));
        }
        Map<String, Object> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("kubernetes.")) {
                options.put(key, properties.getProperty(key));
            }
        }
        return from(
                SeatunnelApplicationConfig.fromProperties(properties),
                ReadonlyConfig.fromMap(options));
    }

    /**
     * Writes only resolved runtime settings, never the submitter's kubeconfig or arbitrary
     * overrides.
     *
     * <p>The resource factory stores this content in the application-owned Secret and mounts it
     * read-only on the master. Common fields and the generated job ID come from the specification;
     * the explicit fields below are the Kubernetes settings needed after submission.
     */
    public void write(Writer writer) throws IOException {
        Properties properties = SeatunnelApplicationConfig.toProperties(specification);
        properties.setProperty(KubernetesOptions.NAMESPACE.key(), namespace);
        properties.setProperty(KubernetesOptions.IMAGE.key(), image);
        properties.setProperty(KubernetesOptions.IMAGE_PULL_POLICY.key(), getImagePullPolicy());
        properties.setProperty(
                KubernetesOptions.IMAGE_PULL_SECRETS.key(), String.join(",", imagePullSecrets));
        properties.setProperty(KubernetesOptions.SERVICE_ACCOUNT.key(), serviceAccount);
        properties.setProperty(KubernetesOptions.SEATUNNEL_HOME.key(), seatunnelHome);
        properties.setProperty(
                KubernetesOptions.RETENTION_SECONDS.key(), Integer.toString(retentionSeconds));
        if (configMap != null) {
            properties.setProperty(KubernetesOptions.CONFIG_MAP.key(), configMap);
        }
        if (applicationSecretRef != null) {
            properties.setProperty(KubernetesOptions.SECRET_REF.key(), applicationSecretRef);
        }
        if (applicationSecretName != null) {
            properties.setProperty(KubernetesOptions.SECRET_NAME.key(), applicationSecretName);
        }
        for (Map.Entry<String, String> entry : applicationSecretData.entrySet()) {
            properties.setProperty(SECRET_DATA_PREFIX + entry.getKey(), entry.getValue());
        }
        if (checkpointPvc != null) {
            properties.setProperty(KubernetesOptions.CHECKPOINT_PVC.key(), checkpointPvc);
        }
        properties.setProperty(KubernetesOptions.MASTER_LABELS.key(), serializePairs(masterLabels));
        properties.setProperty(KubernetesOptions.WORKER_LABELS.key(), serializePairs(workerLabels));
        properties.setProperty(
                KubernetesOptions.MASTER_ANNOTATIONS.key(), serializePairs(masterAnnotations));
        properties.setProperty(
                KubernetesOptions.WORKER_ANNOTATIONS.key(), serializePairs(workerAnnotations));
        properties.setProperty(
                KubernetesOptions.MASTER_NODE_SELECTOR.key(), serializePairs(masterNodeSelector));
        properties.setProperty(
                KubernetesOptions.WORKER_NODE_SELECTOR.key(), serializePairs(workerNodeSelector));
        properties.store(writer, "SeaTunnel Kubernetes application");
    }

    /** Returns the Kubernetes API spelling of the validated pull policy. */
    public String getImagePullPolicy() {
        return imagePullPolicy.value;
    }

    /** @return whether a Secret is mounted into the application Pods */
    public boolean hasApplicationSecret() {
        return applicationSecretRef != null || !applicationSecretData.isEmpty();
    }

    /**
     * Resolves the mounted Secret name.
     *
     * @param applicationId generated application Job name used when no name is configured
     * @return referenced Secret name or the configured/generated name of the created Secret
     */
    public String resolvedApplicationSecretName(String applicationId) {
        if (applicationSecretRef != null) {
            return applicationSecretRef;
        }
        return applicationSecretName == null ? applicationId : applicationSecretName;
    }

    private static Map<String, String> secretData(Map<String, String> options) {
        Map<String, String> data = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!entry.getKey().startsWith(SECRET_DATA_PREFIX)) {
                continue;
            }
            String key = entry.getKey().substring(SECRET_DATA_PREFIX.length());
            if (key.isEmpty() || !key.matches("[-._a-zA-Z0-9]+")) {
                throw new IllegalArgumentException(
                        entry.getKey() + " must use a valid Kubernetes Secret data key");
            }
            if (data.put(key, entry.getValue()) != null) {
                throw new IllegalArgumentException(
                        "kubernetes.secret.data contains duplicate key " + key);
            }
        }
        return Collections.unmodifiableMap(data);
    }

    /**
     * Flattens the resolved options so dynamic {@code kubernetes.secret.data.<KEY>} keys survive.
     */
    private static Map<String, String> rawOptions(ReadonlyConfig options) {
        Map<String, String> raw = new HashMap<>();
        for (Map.Entry<String, ConfigValue> entry : options.toConfig().entrySet()) {
            Object value = entry.getValue().unwrapped();
            if (value != null && !(value instanceof Map) && !(value instanceof List)) {
                raw.put(entry.getKey(), String.valueOf(value));
            }
        }
        return raw;
    }

    private void validateApplicationSecret() {
        if (applicationSecretRef == null) {
            if (applicationSecretName != null && applicationSecretData.isEmpty()) {
                throw new IllegalArgumentException(
                        "kubernetes.secret.name requires kubernetes.secret.data entries");
            }
            return;
        }
        if (applicationSecretName != null || !applicationSecretData.isEmpty()) {
            throw new IllegalArgumentException(
                    "kubernetes.secret-ref cannot be combined with kubernetes.secret");
        }
    }

    private enum ImagePullPolicy {
        ALWAYS("Always"),
        IF_NOT_PRESENT("IfNotPresent"),
        NEVER("Never");

        private final String value;

        ImagePullPolicy(String value) {
            this.value = value;
        }

        private static ImagePullPolicy fromValue(String value) {
            for (ImagePullPolicy policy : values()) {
                if (policy.value.equals(value)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException(
                    "kubernetes.image-pull-policy must be Always, IfNotPresent or Never");
        }
    }

    private static String serializePairs(Map<String, String> pairs) {
        return pairs.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    private static String requireNonBlank(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option.key() + " is required");
        }
        return value;
    }

    private static String optionalNonBlank(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
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

    private static Map<String, String> labels(ReadonlyConfig options, Option<String> option) {
        Map<String, String> labels = pairs(options, option);
        if (labels.containsKey(APPLICATION_LABEL) || labels.containsKey(ROLE_LABEL)) {
            throw new IllegalArgumentException(
                    option.key() + " must not override SeaTunnel ownership labels");
        }
        return labels;
    }

    private static Map<String, String> pairs(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
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
