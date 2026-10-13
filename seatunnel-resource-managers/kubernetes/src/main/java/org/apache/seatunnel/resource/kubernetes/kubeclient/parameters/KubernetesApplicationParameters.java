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
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    private static final String CONFIG_MAP_PREFIX = "kubernetes.configmap.";

    private final ApplicationSpecification specification;
    private final String namespace;
    private final String image;
    private final ImagePullPolicy imagePullPolicy;
    private final List<String> imagePullSecrets;
    private final String serviceAccount;
    private final String seatunnelHome;
    private final String checkpointDirectory;
    private final String secretDirectory;
    private final String masterHazelcastConfig;
    private final String workerHazelcastConfig;
    private final String masterJvmOptions;
    private final String workerJvmOptions;
    private final String jvmOptions;
    private final String seatunnelConfigFile;
    private final String log4jConfigFile;
    private final String applicationSecretRef;
    private final String applicationSecretName;
    private final Map<String, String> applicationSecretData;
    private final String environment;
    private final List<KubernetesEnvironmentVariable> environmentVariables;
    private final List<KubernetesConfigMapEntry> configMaps;
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
        this.seatunnelHome = absolutePath(options, KubernetesOptions.SEATUNNEL_HOME);
        this.checkpointDirectory = absolutePath(options, KubernetesOptions.CHECKPOINT_DIRECTORY);
        this.secretDirectory = absolutePath(options, KubernetesOptions.SECRET_DIRECTORY);
        this.masterHazelcastConfig =
                hazelcastConfigPath(
                        options, KubernetesOptions.MASTER_HAZELCAST_CONFIG, seatunnelHome);
        this.workerHazelcastConfig =
                hazelcastConfigPath(
                        options, KubernetesOptions.WORKER_HAZELCAST_CONFIG, seatunnelHome);
        this.masterJvmOptions =
                jvmOptionsFile(options, KubernetesOptions.MASTER_JVM_OPTIONS, seatunnelHome);
        this.workerJvmOptions =
                jvmOptionsFile(options, KubernetesOptions.WORKER_JVM_OPTIONS, seatunnelHome);
        this.jvmOptions = jvmOptionsFile(options, KubernetesOptions.JVM_OPTIONS, seatunnelHome);
        this.seatunnelConfigFile = seatunnelConfigFile(options, seatunnelHome);
        this.log4jConfigFile = log4jConfigFile(options, seatunnelHome);
        this.applicationSecretRef = optionalNonBlank(options, KubernetesOptions.SECRET_REF);
        this.applicationSecretName = optionalNonBlank(options, KubernetesOptions.SECRET_NAME);
        this.applicationSecretData = secretData(rawOptions(options));
        validateApplicationSecret();
        this.environment = options.get(KubernetesOptions.ENV);
        this.environmentVariables = environmentVariables(environment);
        this.configMaps = configMaps(rawOptions(options));
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
        return read(properties);
    }

    /**
     * Reads the master's Base64 encoded localized specification.
     *
     * <p>When the configured application Secret contains {@code job.conf} in {@code
     * kubernetes.secret-directory}, it replaces the inline job configuration.
     *
     * @param encodedSpecification value of {@link
     *     org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants#SPECIFICATION_ENV}
     * @return parsed Kubernetes application settings
     * @throws IOException if the specification is missing, malformed or cannot be read
     */
    public static KubernetesApplicationParameters read(String encodedSpecification)
            throws IOException {
        Properties properties = decode(encodedSpecification);
        ReadonlyConfig options = ReadonlyConfig.fromMap(kubernetesOptions(properties));
        Path mountedJobConfig =
                Paths.get(
                        options.get(KubernetesOptions.SECRET_DIRECTORY),
                        KubernetesConstants.JOB_CONFIG_FILE);
        if (Files.isRegularFile(mountedJobConfig)) {
            properties.setProperty(
                    SeatunnelApplicationConfig.JOB_CONFIG_KEY,
                    new String(Files.readAllBytes(mountedJobConfig), StandardCharsets.UTF_8));
        }
        return from(SeatunnelApplicationConfig.fromProperties(properties), options);
    }

    private static Properties decode(String encodedSpecification) throws IOException {
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
        return properties;
    }

    private static KubernetesApplicationParameters read(Properties properties) throws IOException {
        return from(
                SeatunnelApplicationConfig.fromProperties(properties),
                ReadonlyConfig.fromMap(kubernetesOptions(properties)));
    }

    private static Map<String, Object> kubernetesOptions(Properties properties) {
        Map<String, Object> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("kubernetes.")) {
                options.put(key, properties.getProperty(key));
            }
        }
        return options;
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
        properties.setProperty(KubernetesOptions.CHECKPOINT_DIRECTORY.key(), checkpointDirectory);
        properties.setProperty(KubernetesOptions.SECRET_DIRECTORY.key(), secretDirectory);
        if (masterHazelcastConfig != null) {
            properties.setProperty(
                    KubernetesOptions.MASTER_HAZELCAST_CONFIG.key(), masterHazelcastConfig);
        }
        if (workerHazelcastConfig != null) {
            properties.setProperty(
                    KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(), workerHazelcastConfig);
        }
        properties.setProperty(
                KubernetesOptions.JVM_OPTIONS.key(), jvmOptions == null ? "none" : jvmOptions);
        properties.setProperty(
                KubernetesOptions.MASTER_JVM_OPTIONS.key(),
                masterJvmOptions == null ? "none" : masterJvmOptions);
        properties.setProperty(
                KubernetesOptions.WORKER_JVM_OPTIONS.key(),
                workerJvmOptions == null ? "none" : workerJvmOptions);
        properties.setProperty(
                KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(),
                seatunnelConfigFile == null ? "none" : seatunnelConfigFile);
        properties.setProperty(
                KubernetesOptions.LOG4J_CONFIG_FILE.key(),
                log4jConfigFile == null ? "none" : log4jConfigFile);
        properties.setProperty(
                KubernetesOptions.RETENTION_SECONDS.key(), Integer.toString(retentionSeconds));
        if (applicationSecretRef != null) {
            properties.setProperty(KubernetesOptions.SECRET_REF.key(), applicationSecretRef);
        }
        if (applicationSecretName != null) {
            properties.setProperty(KubernetesOptions.SECRET_NAME.key(), applicationSecretName);
        }
        for (Map.Entry<String, String> entry : applicationSecretData.entrySet()) {
            properties.setProperty(SECRET_DATA_PREFIX + entry.getKey(), entry.getValue());
        }
        properties.setProperty(KubernetesOptions.ENV.key(), environment);
        writeConfigMaps(properties);
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

    private static List<KubernetesEnvironmentVariable> environmentVariables(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<KubernetesEnvironmentVariable> variables = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (String entry : value.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("kubernetes.env must not contain empty entries");
            }
            KubernetesEnvironmentVariable variable = KubernetesEnvironmentVariable.parse(trimmed);
            if (KubernetesConstants.SPECIFICATION_ENV.equals(variable.getName())) {
                throw new IllegalArgumentException(
                        "kubernetes.env must not override "
                                + KubernetesConstants.SPECIFICATION_ENV);
            }
            if (!names.add(variable.getName())) {
                throw new IllegalArgumentException(
                        "kubernetes.env contains duplicate name " + variable.getName());
            }
            variables.add(variable);
        }
        return Collections.unmodifiableList(variables);
    }

    private static List<KubernetesConfigMapEntry> configMaps(Map<String, String> options) {
        Map<String, String> references = new LinkedHashMap<>();
        Map<String, String> createdNames = new LinkedHashMap<>();
        Map<String, Map<String, String>> data = new LinkedHashMap<>();
        Map<String, Map<String, String>> mounts = new LinkedHashMap<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            String key = option.getKey();
            if (!key.startsWith(CONFIG_MAP_PREFIX)) {
                continue;
            }
            String remainder = key.substring(CONFIG_MAP_PREFIX.length());
            int entrySeparator = remainder.indexOf('.');
            if (entrySeparator <= 0 || entrySeparator == remainder.length() - 1) {
                throw invalidConfigMapEntry(key);
            }
            String entry = remainder.substring(0, entrySeparator);
            if (!entry.matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?")) {
                throw new IllegalArgumentException(
                        key + ": ConfigMap entry names must be lowercase DNS labels, got " + entry);
            }
            String path = remainder.substring(entrySeparator + 1);
            if ("ref".equals(path)) {
                references.put(entry, option.getValue());
                continue;
            }
            if ("name".equals(path)) {
                createdNames.put(entry, option.getValue());
                continue;
            }
            int sectionSeparator = path.indexOf('.');
            if (sectionSeparator <= 0 || sectionSeparator == path.length() - 1) {
                throw invalidConfigMapEntry(key);
            }
            String section = path.substring(0, sectionSeparator);
            String item = path.substring(sectionSeparator + 1);
            if (!item.matches("[-._a-zA-Z0-9]+")) {
                throw new IllegalArgumentException(
                        key + " must use a valid Kubernetes ConfigMap data key");
            }
            if ("data".equals(section)) {
                putConfigMapValue(data, entry, item, option.getValue());
            } else if ("mount".equals(section)) {
                if (!option.getValue().startsWith("/") || option.getValue().contains(":")) {
                    throw new IllegalArgumentException(
                            key + " must be an absolute container path without ':'");
                }
                putConfigMapValue(mounts, entry, item, option.getValue());
            } else {
                throw invalidConfigMapEntry(key);
            }
        }
        LinkedHashSet<String> entries = new LinkedHashSet<>(references.keySet());
        entries.addAll(createdNames.keySet());
        entries.addAll(data.keySet());
        entries.addAll(mounts.keySet());
        List<KubernetesConfigMapEntry> configMaps = new ArrayList<>(entries.size());
        for (String entry : entries) {
            String reference = references.get(entry);
            String createdName = createdNames.get(entry);
            Map<String, String> dataEntries = data.getOrDefault(entry, Collections.emptyMap());
            Map<String, String> mountPaths = mounts.getOrDefault(entry, Collections.emptyMap());
            if (reference != null && !dataEntries.isEmpty()) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX + entry + " cannot combine ref with data entries");
            }
            if (reference == null && dataEntries.isEmpty()) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX
                                + entry
                                + (createdName == null
                                        ? " requires ref or data entries"
                                        : ".name requires data entries"));
            }
            if (reference != null && mountPaths.isEmpty()) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX + entry + " references require mount entries");
            }
            if (reference == null && !mountPaths.keySet().equals(dataEntries.keySet())) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX
                                + entry
                                + " must declare a full mount path for every data entry");
            }
            if (reference != null && createdName != null) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX + entry + " cannot combine name with ref");
            }
            if (createdName != null && !createdName.matches("[a-z0-9]([-a-z0-9.]*[a-z0-9])?")) {
                throw new IllegalArgumentException(
                        CONFIG_MAP_PREFIX
                                + entry
                                + ".name must be a lowercase DNS subdomain, got "
                                + createdName);
            }
            configMaps.add(
                    reference == null
                            ? KubernetesConfigMapEntry.create(
                                    entry, createdName, dataEntries, mountPaths)
                            : KubernetesConfigMapEntry.reference(entry, reference, mountPaths));
        }
        return Collections.unmodifiableList(configMaps);
    }

    private static IllegalArgumentException invalidConfigMapEntry(String key) {
        return new IllegalArgumentException(
                key + " must use kubernetes.configmap.<entry>.ref, .data.<key> or .mount.<key>");
    }

    private static void putConfigMapValue(
            Map<String, Map<String, String>> values, String entry, String key, String value) {
        Map<String, String> entryValues =
                values.computeIfAbsent(entry, ignored -> new LinkedHashMap<>());
        if (entryValues.put(key, value) != null) {
            throw new IllegalArgumentException(
                    CONFIG_MAP_PREFIX + entry + " contains duplicate key " + key);
        }
    }

    private void writeConfigMaps(Properties properties) {
        for (KubernetesConfigMapEntry configMap : configMaps) {
            String prefix = CONFIG_MAP_PREFIX + configMap.getEntryName();
            if (configMap.isReference()) {
                properties.setProperty(prefix + ".ref", configMap.getReference());
            } else {
                if (configMap.getName() != null) {
                    properties.setProperty(prefix + ".name", configMap.getName());
                }
                for (Map.Entry<String, String> entry : configMap.getData().entrySet()) {
                    properties.setProperty(prefix + ".data." + entry.getKey(), entry.getValue());
                }
            }
            for (Map.Entry<String, String> mount : configMap.getMountPaths().entrySet()) {
                properties.setProperty(prefix + ".mount." + mount.getKey(), mount.getValue());
            }
        }
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

    private static String absolutePath(ReadonlyConfig options, Option<String> option) {
        String value = requireNonBlank(options, option);
        if (!value.startsWith("/") || value.contains(":")) {
            throw new IllegalArgumentException(
                    option.key() + " must be an absolute path without ':'");
        }
        return value;
    }

    /**
     * Resolves one Hazelcast member configuration file.
     *
     * @param options resolved deployment settings
     * @param option role-specific option
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute container path, or {@code null} to keep the built-in configuration
     */
    private static String hazelcastConfigPath(
            ReadonlyConfig options, Option<String> option, String seatunnelHome) {
        String value = optionalNonBlank(options, option);
        if (value == null) {
            return null;
        }
        String path;
        if (value.startsWith("/")) {
            if (value.contains(":")) {
                throw new IllegalArgumentException(option.key() + " must not contain ':'");
            }
            path = value;
        } else {
            if (!value.matches("[A-Za-z0-9._-]+")) {
                throw new IllegalArgumentException(
                        option.key()
                                + " must be a file name under <seatunnel-home>/config or an absolute path");
            }
            path = seatunnelHome + (seatunnelHome.endsWith("/") ? "" : "/") + "config/" + value;
        }
        String lowerCase = path.toLowerCase(Locale.ROOT);
        if (!(lowerCase.endsWith(".yaml")
                || lowerCase.endsWith(".yml")
                || lowerCase.endsWith(".xml"))) {
            throw new IllegalArgumentException(
                    option.key() + " must reference a .yaml, .yml or .xml file");
        }
        return path;
    }

    /**
     * Resolves one JVM options file applied through the Java {@code @argfile} syntax.
     *
     * @param options resolved deployment settings
     * @param option role-specific option
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute container path, or {@code null} when disabled
     */
    private static String jvmOptionsFile(
            ReadonlyConfig options, Option<String> option, String seatunnelHome) {
        String value = requireNonBlank(options, option);
        if ("none".equalsIgnoreCase(value)) {
            return null;
        }
        if (value.startsWith("/")) {
            if (value.contains(":")) {
                throw new IllegalArgumentException(option.key() + " must not contain ':'");
            }
            return value;
        }
        if (!value.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException(
                    option.key()
                            + " must be a file name under <seatunnel-home>/config or an absolute path");
        }
        return seatunnelHome + (seatunnelHome.endsWith("/") ? "" : "/") + "config/" + value;
    }

    /**
     * Resolves the SeaTunnel engine configuration file passed as {@code -Dseatunnel.config}.
     *
     * @param options resolved deployment settings
     * @param seatunnelHome distribution home used for relative paths
     * @return absolute container path, or {@code null} when the flag is disabled
     */
    private static String seatunnelConfigFile(ReadonlyConfig options, String seatunnelHome) {
        String value = requireNonBlank(options, KubernetesOptions.SEATUNNEL_CONFIG_FILE);
        if ("none".equalsIgnoreCase(value)) {
            return null;
        }
        String path;
        if (value.startsWith("/")) {
            if (value.contains(":")) {
                throw new IllegalArgumentException(
                        KubernetesOptions.SEATUNNEL_CONFIG_FILE.key() + " must not contain ':'");
            }
            path = value;
        } else {
            for (String segment : value.split("/")) {
                if ("..".equals(segment)) {
                    throw new IllegalArgumentException(
                            KubernetesOptions.SEATUNNEL_CONFIG_FILE.key()
                                    + " must not contain '..'");
                }
            }
            if (value.contains(":") || !value.matches("[A-Za-z0-9._/-]+")) {
                throw new IllegalArgumentException(
                        KubernetesOptions.SEATUNNEL_CONFIG_FILE.key()
                                + " must be a path relative to <seatunnel-home> or an absolute path");
            }
            path = seatunnelHome + (seatunnelHome.endsWith("/") ? "" : "/") + value;
        }
        String lowerCase = path.toLowerCase(Locale.ROOT);
        if (!(lowerCase.endsWith(".yaml") || lowerCase.endsWith(".yml"))) {
            throw new IllegalArgumentException(
                    KubernetesOptions.SEATUNNEL_CONFIG_FILE.key()
                            + " must reference a .yaml or .yml file");
        }
        return path;
    }

    /**
     * Resolves the Log4j2 configuration file passed as {@code -Dlog4j2.configurationFile}.
     *
     * @param options resolved deployment settings
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute container path, or {@code null} when the flag is disabled
     */
    private static String log4jConfigFile(ReadonlyConfig options, String seatunnelHome) {
        String value = jvmOptionsFile(options, KubernetesOptions.LOG4J_CONFIG_FILE, seatunnelHome);
        if (value == null) {
            return null;
        }
        String lowerCase = value.toLowerCase(Locale.ROOT);
        if (!(lowerCase.endsWith(".properties")
                || lowerCase.endsWith(".xml")
                || lowerCase.endsWith(".json")
                || lowerCase.endsWith(".yaml")
                || lowerCase.endsWith(".yml"))) {
            throw new IllegalArgumentException(
                    KubernetesOptions.LOG4J_CONFIG_FILE.key()
                            + " must reference a Log4j2 configuration file");
        }
        return value;
    }

    /** @return ordered JVM options files: the common file first, then the role-specific file */
    public List<String> getMasterJvmOptionsFiles() {
        return jvmOptionsFiles(jvmOptions, masterJvmOptions);
    }

    /** @return ordered JVM options files: the common file first, then the role-specific file */
    public List<String> getWorkerJvmOptionsFiles() {
        return jvmOptionsFiles(jvmOptions, workerJvmOptions);
    }

    private static List<String> jvmOptionsFiles(String common, String role) {
        List<String> files = new ArrayList<>(2);
        if (common != null) {
            files.add(common);
        }
        if (role != null) {
            files.add(role);
        }
        return Collections.unmodifiableList(files);
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
