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

import org.apache.seatunnel.engine.common.config.spec.SessionSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import lombok.Getter;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parsed {@code session.yaml} used by the Kubernetes session master.
 *
 * <p>The file is a deployment document, not a job configuration. It contains the homogeneous worker
 * template, the session worker bounds and the Kubernetes resource settings. Job configurations stay
 * HOCON and are submitted through the existing REST API.
 */
@Getter
public final class KubernetesSessionConfig
        implements KubernetesEnvironmentConfig, KubernetesSecretConfig, KubernetesWorkloadConfig {

    private static final Pattern DNS_LABEL = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?");
    private static final int MAX_NAME_LENGTH = 40;
    private static final int DEFAULT_MIN_WORKERS = 2;
    private static final int DEFAULT_MAX_WORKERS = 32;
    private static final int DEFAULT_MEMORY_MB = 1024;
    private static final int DEFAULT_CPU_CORES = 1;
    private static final int DEFAULT_SLOTS = 1;
    private static final int DEFAULT_MASTER_PORT = 5801;
    private static final long DEFAULT_STARTUP_TIMEOUT_MILLIS = 120_000L;
    private static final long DEFAULT_RETRY_INTERVAL_MILLIS = 3_000L;

    private final SessionSpecification sessionSpecification;
    private final String namespace;
    private final String image;
    private final String imagePullPolicy;
    private final String serviceAccount;
    private final String rootConfigMap;
    private final String workerConfigMap;
    private final String seatunnelHome;
    private final int masterPort;
    private final int heapMb;
    private final String jvmOptions;
    private final Map<String, String> workerLabels;
    private final Map<String, String> workerAnnotations;
    private final List<KubernetesEnvironmentVariable> environmentVariables;
    private final List<KubernetesEnvFromReference> envFromReferences;
    private final String secretRef;
    private final String secretDirectory;

    private KubernetesSessionConfig(
            SessionSpecification sessionSpecification,
            String namespace,
            String image,
            String imagePullPolicy,
            String serviceAccount,
            String rootConfigMap,
            String workerConfigMap,
            String seatunnelHome,
            int masterPort,
            int heapMb,
            String jvmOptions,
            Map<String, String> workerLabels,
            Map<String, String> workerAnnotations,
            List<KubernetesEnvironmentVariable> environmentVariables,
            List<KubernetesEnvFromReference> envFromReferences,
            String secretRef,
            String secretDirectory) {
        this.sessionSpecification = sessionSpecification;
        this.namespace = namespace;
        this.image = image;
        this.imagePullPolicy = imagePullPolicy;
        this.serviceAccount = serviceAccount;
        this.rootConfigMap = rootConfigMap;
        this.workerConfigMap = workerConfigMap;
        this.seatunnelHome = seatunnelHome;
        this.masterPort = masterPort;
        this.heapMb = heapMb;
        this.jvmOptions = jvmOptions;
        this.workerLabels = workerLabels;
        this.workerAnnotations = workerAnnotations;
        this.environmentVariables = environmentVariables;
        this.envFromReferences = envFromReferences;
        this.secretRef = secretRef;
        this.secretDirectory = secretDirectory;
    }

    /**
     * Reads one session deployment file.
     *
     * @param path mounted {@code session.yaml}
     * @return validated session settings
     * @throws IOException if the file cannot be read or parsed
     */
    public static KubernetesSessionConfig read(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return read(reader);
        }
    }

    /**
     * Parses one session deployment document.
     *
     * @param yaml session YAML content
     * @return validated session settings
     * @throws IOException if the document cannot be parsed
     */
    public static KubernetesSessionConfig read(String yaml) throws IOException {
        return read(new StringReader(yaml));
    }

    private static KubernetesSessionConfig read(Reader reader) throws IOException {
        Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(reader);
        Map<String, Object> root = asMap(loaded, "session document");
        Map<String, Object> session = childMap(root, "session");
        Map<String, Object> worker = childMap(session, "worker");
        Map<String, Object> dynamicWorker = childMap(session, "dynamic-worker");
        Map<String, Object> kubernetes = childMap(root, "kubernetes");

        String name = requiredString(session, "name", "session.name");
        validateName(name);
        String clusterName = optionalString(session, "cluster-name", "seatunnel-session-" + name);
        String rootConfigMap =
                optionalString(session, "root-config-map", "seatunnel-session-" + name + "-meta");
        String image = requiredString(worker, "image", "session.worker.image");
        String namespace = optionalString(kubernetes, "namespace", "default");
        String serviceAccount = optionalString(worker, "service-account", "default");
        String imagePullPolicy = optionalString(worker, "image-pull-policy", "IfNotPresent");
        String workerConfigMap =
                optionalString(worker, "config-map", "seatunnel-session-" + name + "-config");
        String seatunnelHome =
                optionalString(
                        worker, "seatunnel-home", KubernetesConstants.DEFAULT_SEATUNNEL_HOME);

        int minWorkers = optionalInt(worker, "min-workers", DEFAULT_MIN_WORKERS);
        int maxWorkers = optionalInt(worker, "max-workers", DEFAULT_MAX_WORKERS);
        int memoryMb = optionalInt(worker, "memory-mb", DEFAULT_MEMORY_MB);
        int cpuCores = optionalInt(worker, "cpu-cores", DEFAULT_CPU_CORES);
        int slots = optionalInt(worker, "slots", DEFAULT_SLOTS);
        int heapMb = optionalInt(worker, "heap-mb", Math.max(1, memoryMb * 3 / 4));
        int masterPort = optionalInt(childMap(session, "master"), "port", DEFAULT_MASTER_PORT);
        boolean dynamicEnabled = optionalBoolean(dynamicWorker, "enabled", false);
        long startupTimeout =
                optionalLong(
                        dynamicWorker, "startup-timeout-millis", DEFAULT_STARTUP_TIMEOUT_MILLIS);
        long retryInterval =
                optionalLong(dynamicWorker, "retry-interval-millis", DEFAULT_RETRY_INTERVAL_MILLIS);
        String jvmOptions =
                optionalString(worker, "jvm-options", "-Xms" + heapMb + "m -Xmx" + heapMb + "m");
        Map<String, String> labels = stringMap(worker, "labels");
        Map<String, String> annotations = stringMap(worker, "annotations");
        List<KubernetesEnvironmentVariable> environmentVariables = environmentVariables(worker);
        List<KubernetesEnvFromReference> envFromReferences = envFromReferences(worker);
        String secretRef = optionalString(worker, "secret-ref", null);
        String secretDirectory =
                optionalString(worker, "secret-directory", "/etc/seatunnel-secrets/session");

        WorkerSpecification workerSpecification =
                new WorkerSpecification(memoryMb, cpuCores, slots);
        SessionSpecification specification =
                new SessionSpecification(
                        name,
                        clusterName,
                        minWorkers,
                        maxWorkers,
                        workerSpecification,
                        dynamicEnabled,
                        startupTimeout,
                        retryInterval);

        if (heapMb <= 0 || heapMb >= memoryMb) {
            throw new IllegalArgumentException(
                    "session.worker.heap-mb must be > 0 and less than session.worker.memory-mb");
        }
        validateDnsLabel(namespace, "kubernetes.namespace");
        validateDnsLabel(rootConfigMap, "session.root-config-map");
        validateDnsLabel(workerConfigMap, "session.worker.config-map");
        if (masterPort <= 0 || masterPort > 65535) {
            throw new IllegalArgumentException("session.master.port must be between 1 and 65535");
        }

        return new KubernetesSessionConfig(
                specification,
                namespace,
                image,
                imagePullPolicy,
                serviceAccount,
                rootConfigMap,
                workerConfigMap,
                seatunnelHome,
                masterPort,
                heapMb,
                jvmOptions,
                labels,
                annotations,
                environmentVariables,
                envFromReferences,
                secretRef,
                secretDirectory);
    }

    /** @return worker creation timeout used by the session resource manager */
    public long getStartupTimeoutMillis() {
        return sessionSpecification.getStartupTimeoutMillis();
    }

    /** @return delay between worker registration checks */
    public long getRetryIntervalMillis() {
        return sessionSpecification.getRetryIntervalMillis();
    }

    @Override
    public boolean hasApplicationSecret() {
        return secretRef != null && !secretRef.trim().isEmpty();
    }

    @Override
    public String resolvedApplicationSecretName(String applicationId) {
        return secretRef;
    }

    @Override
    public Map<String, String> getApplicationSecretData() {
        return Collections.emptyMap();
    }

    @Override
    public List<String> getImagePullSecrets() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> getMasterNodeSelector() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getWorkerNodeSelector() {
        return Collections.emptyMap();
    }

    @Override
    public List<KubernetesToleration> getMasterTolerations() {
        return Collections.emptyList();
    }

    @Override
    public List<KubernetesToleration> getWorkerTolerations() {
        return Collections.emptyList();
    }

    @Override
    public String getSeatunnelConfigFile() {
        return seatunnelHome + "/config/seatunnel.yaml";
    }

    @Override
    public String getLog4jConfigFile() {
        return seatunnelHome + "/config/log4j2.properties";
    }

    private static List<KubernetesEnvironmentVariable> environmentVariables(
            Map<String, Object> worker) {
        Map<String, Object> env = childMap(worker, "env");
        List<KubernetesEnvironmentVariable> variables = new ArrayList<>();
        env.forEach(
                (name, value) ->
                        variables.add(
                                KubernetesEnvironmentVariable.fromValue(
                                        name, String.valueOf(value))));
        return Collections.unmodifiableList(variables);
    }

    private static List<KubernetesEnvFromReference> envFromReferences(Map<String, Object> worker) {
        Map<String, Object> envFrom = childMap(worker, "env-from");
        List<KubernetesEnvFromReference> references = new ArrayList<>();
        addEnvFrom(references, envFrom, "secret", true);
        addEnvFrom(references, envFrom, "configmap", false);
        return Collections.unmodifiableList(references);
    }

    private static void addEnvFrom(
            List<KubernetesEnvFromReference> references,
            Map<String, Object> envFrom,
            String key,
            boolean secret) {
        Object value = envFrom.get(key);
        if (value == null) {
            return;
        }
        for (String name : String.valueOf(value).split(",")) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                references.add(
                        secret
                                ? KubernetesEnvFromReference.secret(trimmed)
                                : KubernetesEnvFromReference.configMap(trimmed));
            }
        }
    }

    /**
     * Builds the decorator-chain entries for the worker runtime files.
     *
     * <p>The worker ConfigMap is user-owned and is mounted file-by-file through the same {@code
     * ConfigMapDecorator} used by application mode. Keeping the list here makes the set of required
     * files explicit instead of hiding the whole image config directory behind one mount.
     *
     * @return ConfigMap entries in decorator order
     */
    public List<KubernetesConfigMapEntry> getWorkerConfigMapEntries() {
        if (workerConfigMap == null || workerConfigMap.trim().isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> mounts = new LinkedHashMap<>();
        for (String key :
                Arrays.asList(
                        "seatunnel.yaml",
                        "hazelcast-worker.yaml",
                        "log4j2.properties",
                        "jvm_worker_options")) {
            mounts.put(key, seatunnelHome + "/config/" + key);
        }
        return Collections.singletonList(
                KubernetesConfigMapEntry.reference("worker", workerConfigMap, mounts));
    }

    private static void validateName(String name) {
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    "session.name must be at most " + MAX_NAME_LENGTH + " characters");
        }
        validateDnsLabel(name, "session.name");
    }

    private static void validateDnsLabel(String value, String option) {
        if (value == null || !DNS_LABEL.matcher(value).matches()) {
            throw new IllegalArgumentException(option + " must be a DNS-1123 label");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value, String path) {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException(path + " must be a YAML object");
        }
        return (Map<String, Object>) value;
    }

    private static Map<String, Object> childMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value == null) {
            return Collections.emptyMap();
        }
        return asMap(value, key);
    }

    private static String requiredString(Map<String, Object> parent, String key, String option) {
        String value = optionalString(parent, key, null);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option + " must not be empty");
        }
        return value.trim();
    }

    private static String optionalString(
            Map<String, Object> parent, String key, String defaultValue) {
        Object value = parent.get(key);
        return value == null ? defaultValue : String.valueOf(value).trim();
    }

    private static int optionalInt(Map<String, Object> parent, String key, int defaultValue) {
        Object value = parent.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(key + " must be an integer, was: " + value, failure);
        }
    }

    private static long optionalLong(Map<String, Object> parent, String key, long defaultValue) {
        Object value = parent.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(key + " must be an integer, was: " + value, failure);
        }
    }

    private static boolean optionalBoolean(
            Map<String, Object> parent, String key, boolean defaultValue) {
        Object value = parent.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(String.valueOf(value));
    }

    private static Map<String, String> stringMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> raw = asMap(value, key);
        Map<String, String> result = new LinkedHashMap<>();
        raw.forEach((name, item) -> result.put(name, String.valueOf(item)));
        return Collections.unmodifiableMap(result);
    }
}
