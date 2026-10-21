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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionUtil;
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
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Parsed Session policy and homogeneous worker template.
 *
 * <p>The YAML documents are flattened into option keys and read through {@link ReadonlyConfig}, the
 * same mechanism used by {@link KubernetesApplicationConfig}. {@link KubernetesSessionOptions}
 * declares every supported key once; the parser and the unknown-key check both use that class. The
 * fixed slot count is read from the shared engine configuration.
 */
@Getter
public final class KubernetesSessionConfig
        implements KubernetesEnvironmentConfig, KubernetesSecretConfig, KubernetesWorkloadConfig {

    private static final Pattern DNS_LABEL = Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?");
    private static final int MAX_NAME_LENGTH = 40;
    private static final int MAX_VOLUME_NAME_LENGTH = 63;
    private static final String SESSION_NAME_PREFIX = "seatunnel-session-";
    private static final String ROOT_CONFIG_SUFFIX = "-meta";
    private static final String WORKER_CONFIG_SUFFIX = "-config";
    private static final List<Option<?>> SESSION_KUBERNETES_OPTIONS =
            Arrays.asList(
                    KubernetesOptions.NAMESPACE,
                    KubernetesOptions.IMAGE,
                    KubernetesOptions.IMAGE_PULL_POLICY,
                    KubernetesOptions.IMAGE_PULL_SECRETS,
                    KubernetesOptions.SERVICE_ACCOUNT,
                    KubernetesOptions.SEATUNNEL_HOME,
                    KubernetesOptions.SECRET_REF,
                    KubernetesOptions.SECRET_DIRECTORY,
                    KubernetesOptions.WORKER_LABELS,
                    KubernetesOptions.WORKER_ANNOTATIONS,
                    KubernetesOptions.WORKER_NODE_SELECTOR,
                    KubernetesOptions.WORKER_TOLERATIONS);

    private final SessionSpecification sessionSpecification;
    private final String namespace;
    private final String image;
    private final String imagePullPolicy;
    private final List<String> imagePullSecrets;
    private final String serviceAccount;
    private final Map<String, String> workerNodeSelector;
    private final List<KubernetesToleration> workerTolerations;
    private final String rootConfigMap;
    private final String dataClaim;
    private final String dataMount;
    private final String workerConfigMap;
    private final String seatunnelHome;
    private final int heapMb;
    private final String jvmOptions;
    private final Map<String, String> workerLabels;
    private final Map<String, String> workerAnnotations;
    private final List<KubernetesEnvironmentVariable> environmentVariables;
    private final List<KubernetesEnvFromReference> envFromReferences;
    private final String secretRef;
    private final String secretDirectory;

    private KubernetesSessionConfig(int slots, Map<String, String> rawOptions) {
        validateDeclaredOptions(rawOptions);
        ReadonlyConfig options = ReadonlyConfig.fromMap(new LinkedHashMap<>(rawOptions));

        String name = requireNonBlank(options, KubernetesSessionOptions.NAME);
        validateName(name);
        String clusterName =
                defaultIfNull(
                        optionalNonBlank(options, KubernetesSessionOptions.CLUSTER_NAME),
                        defaultClusterName(name));
        String rootConfigMap =
                defaultIfNull(
                        optionalNonBlank(options, KubernetesSessionOptions.ROOT_CONFIG_MAP),
                        defaultRootConfigMap(name));
        String dataClaim = optionalNonBlank(options, KubernetesSessionOptions.DATA_CLAIM);
        String workerConfigMap =
                defaultIfNull(
                        optionalNonBlank(options, KubernetesSessionOptions.WORKER_CONFIG_MAP),
                        defaultWorkerConfigMap(name));

        int memoryMb = requirePositiveInt(options, KubernetesSessionOptions.WORKER_MEMORY_MB);
        int cpuCores = requirePositiveInt(options, KubernetesSessionOptions.WORKER_CPU_CORES);
        Integer configuredHeap = options.get(KubernetesSessionOptions.WORKER_HEAP_MB);
        int heapMb =
                configuredHeap == null
                        ? defaultHeapMb(memoryMb)
                        : requirePositiveInt(
                                KubernetesSessionOptions.WORKER_HEAP_MB, configuredHeap);
        String jvmOptions =
                defaultIfNull(
                        optionalNonBlank(options, KubernetesSessionOptions.WORKER_JVM_OPTIONS),
                        defaultJvmOptions(heapMb));

        int minWorkers = requirePositiveInt(options, KubernetesSessionOptions.MIN_WORKERS);
        int maxWorkers = requirePositiveInt(options, KubernetesSessionOptions.MAX_WORKERS);
        boolean dynamicEnabled = options.get(KubernetesSessionOptions.DYNAMIC_WORKER_ENABLED);
        long startupTimeout =
                requirePositiveLong(options, KubernetesSessionOptions.STARTUP_TIMEOUT_MILLIS);
        long retryInterval =
                requirePositiveLong(options, KubernetesSessionOptions.RETRY_INTERVAL_MILLIS);

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

        if (heapMb >= memoryMb) {
            throw new IllegalArgumentException(
                    KubernetesSessionOptions.WORKER_HEAP_MB.key()
                            + " must be less than "
                            + KubernetesSessionOptions.WORKER_MEMORY_MB.key());
        }
        String namespace = dnsLabel(options, KubernetesOptions.NAMESPACE);
        validateDnsLabel(rootConfigMap, KubernetesSessionOptions.ROOT_CONFIG_MAP.key());
        validateDnsLabel(workerConfigMap, KubernetesSessionOptions.WORKER_CONFIG_MAP.key());
        if (dataClaim != null) {
            validateDataClaim(dataClaim);
        }
        String dataMount = absolutePath(options, KubernetesSessionOptions.DATA_MOUNT);
        String seatunnelHome = absolutePath(options, KubernetesOptions.SEATUNNEL_HOME);
        String secretDirectory = absolutePath(options, KubernetesOptions.SECRET_DIRECTORY);
        String secretRef = optionalNonBlank(options, KubernetesOptions.SECRET_REF);
        if (dataClaim != null) {
            validateDataMount(dataMount, seatunnelHome, secretRef == null ? null : secretDirectory);
        }

        List<String> imagePullSecrets =
                KubernetesOptionValues.parseList(
                        options.get(KubernetesOptions.IMAGE_PULL_SECRETS),
                        KubernetesOptions.IMAGE_PULL_SECRETS.key());
        Map<String, String> workerNodeSelector =
                KubernetesOptionValues.parsePairs(
                        options.get(KubernetesOptions.WORKER_NODE_SELECTOR),
                        KubernetesOptions.WORKER_NODE_SELECTOR.key());
        List<KubernetesToleration> workerTolerations =
                KubernetesToleration.parse(
                        KubernetesOptions.WORKER_TOLERATIONS.key(),
                        options.get(KubernetesOptions.WORKER_TOLERATIONS));
        Map<String, String> workerLabels = workerLabels(options);
        Map<String, String> workerAnnotations =
                KubernetesOptionValues.parsePairs(
                        options.get(KubernetesOptions.WORKER_ANNOTATIONS),
                        KubernetesOptions.WORKER_ANNOTATIONS.key());

        this.sessionSpecification = specification;
        this.namespace = namespace;
        this.image = requireNonBlank(options, KubernetesOptions.IMAGE);
        this.imagePullPolicy = imagePullPolicy(options);
        this.imagePullSecrets = imagePullSecrets;
        this.serviceAccount = requireNonBlank(options, KubernetesOptions.SERVICE_ACCOUNT);
        this.workerNodeSelector = workerNodeSelector;
        this.workerTolerations = workerTolerations;
        this.rootConfigMap = rootConfigMap;
        this.dataClaim = dataClaim;
        this.dataMount = dataMount;
        this.workerConfigMap = workerConfigMap;
        this.seatunnelHome = seatunnelHome;
        this.heapMb = heapMb;
        this.jvmOptions = jvmOptions;
        this.workerLabels = workerLabels;
        this.workerAnnotations = workerAnnotations;
        this.environmentVariables = environmentVariables(rawOptions);
        this.envFromReferences = envFromReferences(rawOptions);
        this.secretRef = secretRef;
        this.secretDirectory = secretDirectory;
    }

    /**
     * Reads the Session policy and worker template.
     *
     * @param sessionPath mounted {@code session-cluster.yaml}
     * @param slots fixed slot count from the shared engine configuration
     * @return validated session settings
     * @throws IOException if the file cannot be read or parsed
     */
    public static KubernetesSessionConfig read(Path sessionPath, int slots) throws IOException {
        Map<String, String> options = new LinkedHashMap<>();
        flatten(readYaml(sessionPath, "session document"), "", "session document", options);
        return read(options, slots);
    }

    /**
     * Parses the Session policy and worker template document.
     *
     * @param sessionYaml Session YAML content
     * @param slots fixed slot count from the shared engine configuration
     * @return validated session settings
     * @throws IOException if the document cannot be parsed
     */
    public static KubernetesSessionConfig read(String sessionYaml, int slots) throws IOException {
        Map<String, String> options = new LinkedHashMap<>();
        flatten(loadYaml(sessionYaml), "", "session document", options);
        return read(options, slots);
    }

    private static KubernetesSessionConfig read(Map<String, String> options, int slots) {
        if (slots <= 0) {
            throw new IllegalArgumentException("seatunnel.yaml slot-num must be > 0");
        }
        return new KubernetesSessionConfig(slots, options);
    }

    private static Object readYaml(Path path, String document) throws IOException {
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return loadYaml(reader, document);
        }
    }

    private static Object loadYaml(String yaml) throws IOException {
        return loadYaml(new StringReader(yaml), "document");
    }

    private static Object loadYaml(Reader reader, String document) throws IOException {
        return new Yaml(new SafeConstructor(new LoaderOptions())).load(reader);
    }

    private static void flatten(
            Object loaded, String prefix, String path, Map<String, String> result) {
        Map<String, Object> values = asMap(loaded, path);
        flattenMap(values, prefix, path, result);
    }

    private static void flattenMap(
            Map<String, Object> values, String prefix, String path, Map<String, String> result) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            String childPath = path + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenMap(asMap(value, childPath), key, childPath, result);
            } else if (value instanceof List) {
                throw new IllegalArgumentException(
                        childPath + " must be a scalar or an object, not a YAML list");
            } else if (value != null) {
                result.put(key, String.valueOf(value));
            }
        }
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
        return imagePullSecrets;
    }

    @Override
    public Map<String, String> getWorkerNodeSelector() {
        return workerNodeSelector;
    }

    @Override
    public List<KubernetesToleration> getWorkerTolerations() {
        return workerTolerations;
    }

    /**
     * Builds the worker runtime-file mounts from the user-owned ConfigMap.
     *
     * <p>The worker ConfigMap is part of the Session configuration, so the parsed configuration
     * owns the required file list and mount paths. The factory only consumes the entries when it
     * assembles the decorator chain.
     *
     * @return ConfigMap entries in decorator order, or an empty list when none is configured
     */
    public List<KubernetesConfigMapEntry> getWorkerConfigMapEntries() {
        if (workerConfigMap == null || workerConfigMap.trim().isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, String> mounts = new LinkedHashMap<>();
        for (String key : KubernetesConstants.SESSION_WORKER_CONFIG_FILES) {
            mounts.put(key, seatunnelHome + "/" + KubernetesConstants.CONFIG_DIRECTORY + "/" + key);
        }
        return Collections.singletonList(
                KubernetesConfigMapEntry.reference(
                        KubernetesConstants.WORKER_CONFIG_ENTRY, workerConfigMap, mounts));
    }

    private static List<KubernetesEnvironmentVariable> environmentVariables(
            Map<String, String> rawOptions) {
        List<KubernetesEnvironmentVariable> variables = new ArrayList<>();
        for (Map.Entry<String, String> entry : rawOptions.entrySet()) {
            if (entry.getKey().startsWith(KubernetesConstants.ENV_PREFIX)) {
                variables.add(
                        KubernetesEnvironmentVariable.fromValue(
                                entry.getKey().substring(KubernetesConstants.ENV_PREFIX.length()),
                                entry.getValue()));
            }
        }
        return Collections.unmodifiableList(variables);
    }

    private static List<KubernetesEnvFromReference> envFromReferences(
            Map<String, String> rawOptions) {
        List<KubernetesEnvFromReference> references = new ArrayList<>();
        addEnvFrom(
                references,
                KubernetesOptionValues.parseList(
                        rawOptions.get(KubernetesConstants.ENV_FROM_SECRET),
                        KubernetesConstants.ENV_FROM_SECRET),
                true);
        addEnvFrom(
                references,
                KubernetesOptionValues.parseList(
                        rawOptions.get(KubernetesConstants.ENV_FROM_CONFIGMAP),
                        KubernetesConstants.ENV_FROM_CONFIGMAP),
                false);
        return Collections.unmodifiableList(references);
    }

    private static void addEnvFrom(
            List<KubernetesEnvFromReference> references, List<String> names, boolean secret) {
        for (String name : names) {
            references.add(
                    secret
                            ? KubernetesEnvFromReference.secret(name)
                            : KubernetesEnvFromReference.configMap(name));
        }
    }

    /**
     * Rejects unknown options so a misspelled key fails at startup instead of being ignored.
     *
     * <p>Session keys are declared by {@link KubernetesSessionOptions}. Shared Kubernetes keys are
     * the ones this class actually consumes from {@link KubernetesOptions}; {@code
     * kubernetes.env.*} is dynamic, while the two {@code kubernetes.env-from.*} keys are fixed.
     *
     * @param options flattened Session and Kubernetes options
     * @throws IllegalArgumentException if a key is neither declared nor dynamic
     */
    private static void validateDeclaredOptions(Map<String, String> options) {
        Set<String> sessionKeys = OptionUtil.getDeclaredOptionKeys(KubernetesSessionOptions.class);
        for (String key : options.keySet()) {
            if (sessionKeys.contains(key)
                    || isSharedKubernetesKey(key)
                    || key.startsWith(KubernetesConstants.ENV_PREFIX)
                    || KubernetesConstants.ENV_FROM_SECRET.equals(key)
                    || KubernetesConstants.ENV_FROM_CONFIGMAP.equals(key)) {
                continue;
            }
            throw new IllegalArgumentException(
                    "Unknown Session option " + key + "; check the option name");
        }
    }

    private static boolean isSharedKubernetesKey(String key) {
        for (Option<?> option : SESSION_KUBERNETES_OPTIONS) {
            if (option.key().equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, String> workerLabels(ReadonlyConfig options) {
        Map<String, String> labels =
                KubernetesOptionValues.parsePairs(
                        options.get(KubernetesOptions.WORKER_LABELS),
                        KubernetesOptions.WORKER_LABELS.key());
        if (labels.containsKey(KubernetesConstants.APPLICATION_LABEL)
                || labels.containsKey(KubernetesConstants.ROLE_LABEL)
                || labels.containsKey(KubernetesConstants.SESSION_LABEL)) {
            throw new IllegalArgumentException(
                    KubernetesOptions.WORKER_LABELS.key()
                            + " must not override SeaTunnel ownership labels");
        }
        return labels;
    }

    private static void validateName(String name) {
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    KubernetesSessionOptions.NAME.key()
                            + " must be at most "
                            + MAX_NAME_LENGTH
                            + " characters");
        }
        validateDnsLabel(name, KubernetesSessionOptions.NAME.key());
    }

    private static void validateDataClaim(String claim) {
        String option = KubernetesSessionOptions.DATA_CLAIM.key();
        validateDnsLabel(claim, option);
        if (claim.length() > MAX_VOLUME_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    option
                            + " must be at most "
                            + MAX_VOLUME_NAME_LENGTH
                            + " characters because it is used as a volume name");
        }
        if (KubernetesConstants.APPLICATION_VOLUME.equals(claim)
                || claim.startsWith(KubernetesConstants.CONFIG_VOLUME + "-")) {
            throw new IllegalArgumentException(option + " must not use a reserved volume name");
        }
    }

    private static void validateDataMount(
            String dataMount, String seatunnelHome, String secretDirectory) {
        String configDirectory = seatunnelHome + "/" + KubernetesConstants.CONFIG_DIRECTORY;
        if (overlaps(dataMount, configDirectory)
                || (secretDirectory != null && overlaps(dataMount, secretDirectory))) {
            throw new IllegalArgumentException(
                    KubernetesSessionOptions.DATA_MOUNT.key()
                            + " must not overlap the worker config or Secret mount");
        }
    }

    private static boolean overlaps(String first, String second) {
        return first.equals(second)
                || first.startsWith(second + "/")
                || second.startsWith(first + "/");
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

    private static String requireNonBlank(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(option.key() + " is required");
        }
        return value.trim();
    }

    private static String optionalNonBlank(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null) {
            return null;
        }
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException(option.key() + " must not be empty");
        }
        return value.trim();
    }

    private static String absolutePath(ReadonlyConfig options, Option<String> option) {
        String value = requireNonBlank(options, option);
        if (!value.startsWith("/") || value.contains(":")) {
            throw new IllegalArgumentException(
                    option.key() + " must be an absolute path without ':'");
        }
        return value;
    }

    private static String dnsLabel(ReadonlyConfig options, Option<String> option) {
        String value = requireNonBlank(options, option);
        validateDnsLabel(value, option.key());
        return value;
    }

    private static String imagePullPolicy(ReadonlyConfig options) {
        String value = requireNonBlank(options, KubernetesOptions.IMAGE_PULL_POLICY);
        if (!"Always".equals(value) && !"IfNotPresent".equals(value) && !"Never".equals(value)) {
            throw new IllegalArgumentException(
                    KubernetesOptions.IMAGE_PULL_POLICY.key()
                            + " must be Always, IfNotPresent or Never");
        }
        return value;
    }

    private static int requirePositiveInt(ReadonlyConfig options, Option<Integer> option) {
        return requirePositiveInt(option, options.get(option));
    }

    private static int requirePositiveInt(Option<Integer> option, Integer value) {
        if (value == null || value < 1) {
            throw new IllegalArgumentException(option.key() + " must be positive");
        }
        return value;
    }

    private static long requirePositiveLong(ReadonlyConfig options, Option<Long> option) {
        Long value = options.get(option);
        if (value == null || value < 1) {
            throw new IllegalArgumentException(option.key() + " must be positive");
        }
        return value;
    }

    private static String defaultIfNull(String value, String defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static String defaultClusterName(String name) {
        return SESSION_NAME_PREFIX + name;
    }

    private static String defaultRootConfigMap(String name) {
        return defaultClusterName(name) + ROOT_CONFIG_SUFFIX;
    }

    private static String defaultWorkerConfigMap(String name) {
        return defaultClusterName(name) + WORKER_CONFIG_SUFFIX;
    }

    private static int defaultHeapMb(int memoryMb) {
        return Math.max(
                KubernetesConstants.MINIMUM_JVM_HEAP_MB,
                memoryMb
                        * KubernetesConstants.JVM_HEAP_NUMERATOR
                        / KubernetesConstants.JVM_HEAP_DENOMINATOR);
    }

    private static String defaultJvmOptions(int heapMb) {
        return KubernetesConstants.JVM_MIN_HEAP_PREFIX
                + heapMb
                + "m "
                + KubernetesConstants.JVM_MAX_HEAP_PREFIX
                + heapMb
                + "m";
    }
}
