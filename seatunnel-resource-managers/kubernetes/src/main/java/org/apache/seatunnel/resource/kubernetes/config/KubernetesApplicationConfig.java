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
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
import java.util.Arrays;
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
 * Validated, strongly typed Kubernetes configuration for one application.
 *
 * <p>{@link KubernetesOptions} declares the public configuration contract. This class parses
 * composite values and enforces relationships between options. The decorator steps consume the
 * resolved values and contain no configuration parsing logic.
 */
@Getter
@Slf4j
public final class KubernetesApplicationConfig {

    /** Prefixes of options whose keys are dynamic, for example {@code kubernetes.env.<NAME>}. */
    private static final List<String> DYNAMIC_OPTION_PREFIXES =
            Arrays.asList(
                    KubernetesConstants.ENV_PREFIX,
                    KubernetesConstants.ENV_FROM_PREFIX,
                    KubernetesConstants.SECRET_DATA_PREFIX,
                    KubernetesConstants.CONFIG_MAP_PREFIX);

    /** Sections of one {@code kubernetes.configmap.<entry>} option. */
    private static final String SECTION_REF = "ref";

    private static final String SECTION_NAME = "name";
    private static final String SECTION_DATA = "data";
    private static final String SECTION_MOUNT = "mount";

    /** Validation patterns for Kubernetes names, data keys, environment names and paths. */
    private static final String DNS_LABEL_PATTERN = "[a-z0-9]([-a-z0-9]*[a-z0-9])?";

    private static final String DNS_SUBDOMAIN_PATTERN = "[a-z0-9]([-a-z0-9.]*[a-z0-9])?";
    private static final String DATA_KEY_PATTERN = "[-._a-zA-Z0-9]+";
    private static final String FILE_NAME_PATTERN = "[A-Za-z0-9._-]+";
    private static final String RELATIVE_PATH_PATTERN = "[A-Za-z0-9._/-]+";
    private static final String ENV_NAME_PATTERN = "[A-Za-z_][A-Za-z0-9_]*";

    /** Accepted suffixes of the runtime files the engine and Log4j2 read. */
    private static final List<String> HAZELCAST_SUFFIXES = Arrays.asList(".yaml", ".yml", ".xml");

    private static final List<String> ENGINE_SUFFIXES = Arrays.asList(".yaml", ".yml");

    private static final List<String> LOG4J_SUFFIXES =
            Arrays.asList(".properties", ".xml", ".json", ".yaml", ".yml");

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
    private final String jobConfigFile;
    private final String applicationSecretRef;
    private final String applicationSecretName;
    private final Map<String, String> applicationSecretData;
    private final List<KubernetesEnvironmentVariable> environmentVariables;
    private final List<KubernetesEnvFromReference> envFromReferences;
    private final List<KubernetesConfigMapEntry> configMaps;
    private final int retentionSeconds;
    private final Long activeDeadlineSeconds;
    private final String checkpointPvc;
    private final Map<String, String> masterLabels;
    private final Map<String, String> workerLabels;
    private final Map<String, String> masterAnnotations;
    private final Map<String, String> workerAnnotations;
    private final Map<String, String> masterNodeSelector;
    private final Map<String, String> workerNodeSelector;
    private final List<KubernetesToleration> masterTolerations;
    private final List<KubernetesToleration> workerTolerations;

    private KubernetesApplicationConfig(
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
        this.jobConfigFile = resolveJobConfigFile(options, seatunnelHome);
        requireJobConfiguration(specification.getJobConfig(), jobConfigFile);
        Map<String, String> rawOptions = rawOptions(options);
        validateDeclaredOptions(rawOptions);
        this.applicationSecretRef = optionalNonBlank(options, KubernetesOptions.SECRET_REF);
        this.applicationSecretName = optionalNonBlank(options, KubernetesOptions.SECRET_NAME);
        this.applicationSecretData = secretData(rawOptions);
        validateApplicationSecret();
        this.environmentVariables = environmentVariables(rawOptions);
        this.envFromReferences = envFromReferences(rawOptions);
        this.configMaps = configMaps(rawOptions);
        validateConfigMapMounts();
        this.retentionSeconds = requirePositive(options, KubernetesOptions.RETENTION_SECONDS);
        // Unset keeps the Job active until it finishes; setting it bounds startup plus the run.
        this.activeDeadlineSeconds =
                optionalPositive(options, KubernetesOptions.ACTIVE_DEADLINE_SECONDS);
        this.checkpointPvc = optionalNonBlank(options, KubernetesOptions.CHECKPOINT_PVC);
        this.masterLabels = labels(options, KubernetesOptions.MASTER_LABELS);
        this.workerLabels = labels(options, KubernetesOptions.WORKER_LABELS);
        this.masterAnnotations = pairs(options, KubernetesOptions.MASTER_ANNOTATIONS);
        this.workerAnnotations = pairs(options, KubernetesOptions.WORKER_ANNOTATIONS);
        this.masterNodeSelector = pairs(options, KubernetesOptions.MASTER_NODE_SELECTOR);
        this.workerNodeSelector = pairs(options, KubernetesOptions.WORKER_NODE_SELECTOR);
        this.masterTolerations =
                KubernetesToleration.parse(
                        KubernetesOptions.MASTER_TOLERATIONS.key(),
                        options.get(KubernetesOptions.MASTER_TOLERATIONS));
        this.workerTolerations =
                KubernetesToleration.parse(
                        KubernetesOptions.WORKER_TOLERATIONS.key(),
                        options.get(KubernetesOptions.WORKER_TOLERATIONS));
    }

    /**
     * Resolves platform settings before any Kubernetes resource is created.
     *
     * @param specification immutable application launch specification
     * @param options platform deployment settings, including dynamic kubernetes.secret.data keys
     * @return parsed Kubernetes application settings
     * @throws IllegalArgumentException if an option is invalid or overrides an ownership label
     */
    public static KubernetesApplicationConfig from(
            ApplicationSpecification specification, ReadonlyConfig options) {
        return new KubernetesApplicationConfig(specification, options);
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
    public static KubernetesApplicationConfig read(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return read(properties);
    }

    /**
     * Reads the master's Base64 encoded localized specification.
     *
     * <p>When the file resolved from {@code kubernetes.job-config-file} exists in the Pod, it
     * replaces the inline job configuration. By default that is {@code job.conf} inside the mounted
     * application Secret; a ConfigMap key mounted at an absolute path works the same way.
     *
     * <p>Submitters omit the inline job text whenever {@code kubernetes.job-config-file} is set, so
     * a missing file fails here instead of starting without the job the file was meant to provide.
     * A specification written by an older submitter still carries the text and falls back to it
     * with a warning.
     *
     * @param encodedSpecification value of {@link
     *     org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants#SPECIFICATION_ENV}
     * @return parsed Kubernetes application settings
     * @throws IOException if the specification is missing, malformed or cannot be read
     */
    public static KubernetesApplicationConfig read(String encodedSpecification) throws IOException {
        Properties properties = decode(encodedSpecification);
        ReadonlyConfig options = ReadonlyConfig.fromMap(kubernetesOptions(properties));
        String inlineJobConfiguration = inlineJobConfiguration(properties);
        String jobConfigFile =
                resolveJobConfigFile(options, options.get(KubernetesOptions.SEATUNNEL_HOME));
        if (jobConfigFile != null) {
            Path mountedJobConfig = Paths.get(jobConfigFile);
            if (Files.isRegularFile(mountedJobConfig)) {
                properties.setProperty(
                        SeatunnelApplicationConfig.JOB_CONFIG_KEY,
                        new String(Files.readAllBytes(mountedJobConfig), StandardCharsets.UTF_8));
            } else if (inlineJobConfiguration.isEmpty()) {
                throw new IOException(
                        KubernetesOptions.JOB_CONFIG_FILE.key()
                                + " "
                                + jobConfigFile
                                + " does not exist in the Pod and the localized specification carries no"
                                + " inline job configuration");
            } else {
                // Only reachable for specifications written before the inline text was omitted.
                log.warn(
                        "Configured {} {} does not exist; using the inline job configuration",
                        KubernetesOptions.JOB_CONFIG_FILE.key(),
                        jobConfigFile);
            }
        }
        return from(SeatunnelApplicationConfig.fromProperties(properties), options);
    }

    private static String inlineJobConfiguration(Properties properties) {
        String inline = properties.getProperty(SeatunnelApplicationConfig.JOB_CONFIG_KEY);
        if (inline == null) {
            return "";
        }
        return inline.trim();
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

    private static KubernetesApplicationConfig read(Properties properties) throws IOException {
        return from(
                SeatunnelApplicationConfig.fromProperties(properties),
                ReadonlyConfig.fromMap(kubernetesOptions(properties)));
    }

    private static Map<String, Object> kubernetesOptions(Properties properties) {
        Map<String, Object> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith(KubernetesConstants.OPTION_PREFIX)) {
                options.put(key, properties.getProperty(key));
            }
        }
        return options;
    }

    /**
     * Writes only resolved runtime settings, never the submitter's kubeconfig or arbitrary
     * overrides.
     *
     * <p>The master reads this content from its container environment to recreate the settings it
     * needs when it creates workers. Common fields and the generated job ID come from the
     * specification; the explicit fields below are the Kubernetes settings needed after submission.
     *
     * <p>When {@code kubernetes.job-config-file} is configured the inline job text is dropped: the
     * master reads the mounted file instead, so credentials inside {@code -c} never reach the Job
     * object.
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
        if (jobConfigFile != null) {
            // The master replaces the inline job with the mounted file. Keeping the submitted text
            // as well would expose it through the Job object for the retention period.
            properties.remove(SeatunnelApplicationConfig.JOB_CONFIG_KEY);
            properties.setProperty(KubernetesOptions.JOB_CONFIG_FILE.key(), jobConfigFile);
        }
        if (masterHazelcastConfig != null) {
            properties.setProperty(
                    KubernetesOptions.MASTER_HAZELCAST_CONFIG.key(), masterHazelcastConfig);
        }
        if (workerHazelcastConfig != null) {
            properties.setProperty(
                    KubernetesOptions.WORKER_HAZELCAST_CONFIG.key(), workerHazelcastConfig);
        }
        properties.setProperty(
                KubernetesOptions.JVM_OPTIONS.key(),
                jvmOptions == null ? KubernetesConstants.DISABLED : jvmOptions);
        properties.setProperty(
                KubernetesOptions.MASTER_JVM_OPTIONS.key(),
                masterJvmOptions == null ? KubernetesConstants.DISABLED : masterJvmOptions);
        properties.setProperty(
                KubernetesOptions.WORKER_JVM_OPTIONS.key(),
                workerJvmOptions == null ? KubernetesConstants.DISABLED : workerJvmOptions);
        properties.setProperty(KubernetesOptions.SEATUNNEL_CONFIG_FILE.key(), seatunnelConfigFile);
        properties.setProperty(KubernetesOptions.LOG4J_CONFIG_FILE.key(), log4jConfigFile);
        properties.setProperty(
                KubernetesOptions.RETENTION_SECONDS.key(), Integer.toString(retentionSeconds));
        if (activeDeadlineSeconds != null) {
            properties.setProperty(
                    KubernetesOptions.ACTIVE_DEADLINE_SECONDS.key(),
                    Long.toString(activeDeadlineSeconds));
        }
        if (applicationSecretRef != null) {
            properties.setProperty(KubernetesOptions.SECRET_REF.key(), applicationSecretRef);
        }
        if (applicationSecretName != null) {
            properties.setProperty(KubernetesOptions.SECRET_NAME.key(), applicationSecretName);
        }
        for (Map.Entry<String, String> entry : applicationSecretData.entrySet()) {
            // The master only needs the key set; values must stay on the submitting client.
            properties.setProperty(KubernetesConstants.SECRET_DATA_PREFIX + entry.getKey(), "");
        }
        for (KubernetesEnvironmentVariable variable : environmentVariables) {
            properties.setProperty(
                    KubernetesConstants.ENV_PREFIX + variable.getName(), variable.serializeValue());
        }
        for (KubernetesEnvFromReference reference : envFromReferences) {
            properties.setProperty(
                    reference.getSource() == KubernetesEnvFromReference.Source.SECRET
                            ? KubernetesConstants.ENV_FROM_SECRET
                            : KubernetesConstants.ENV_FROM_CONFIGMAP,
                    reference.getName());
        }
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
        properties.setProperty(
                KubernetesOptions.MASTER_TOLERATIONS.key(),
                KubernetesToleration.serialize(masterTolerations));
        properties.setProperty(
                KubernetesOptions.WORKER_TOLERATIONS.key(),
                KubernetesToleration.serialize(workerTolerations));
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
            if (!entry.getKey().startsWith(KubernetesConstants.SECRET_DATA_PREFIX)) {
                continue;
            }
            String key = entry.getKey().substring(KubernetesConstants.SECRET_DATA_PREFIX.length());
            if (key.isEmpty() || !key.matches(DATA_KEY_PATTERN)) {
                throw new IllegalArgumentException(
                        entry.getKey() + " must use a valid Kubernetes Secret data key");
            }
            if (data.put(key, entry.getValue()) != null) {
                throw new IllegalArgumentException(
                        KubernetesConstants.SECRET_DATA_OPTION + " contains duplicate key " + key);
            }
        }
        return Collections.unmodifiableMap(data);
    }

    /**
     * Flattens the resolved options so dynamic {@code kubernetes.secret.data.<KEY>} keys survive.
     *
     * <p>The source map is read directly: round-tripping through HOCON would quote keys that
     * contain characters outside the HOCON path alphabet (for example {@code BAD_NAME!}), which
     * would both hide them from the dynamic prefixes and skip their validation.
     */
    private static Map<String, String> rawOptions(ReadonlyConfig options) {
        Map<String, String> raw = new HashMap<>();
        for (Map.Entry<String, Object> entry : options.getSourceMap().entrySet()) {
            Object value = entry.getValue();
            if (value != null && !(value instanceof Map) && !(value instanceof List)) {
                raw.put(entry.getKey(), String.valueOf(value));
            }
        }
        return raw;
    }

    private static List<KubernetesEnvironmentVariable> environmentVariables(
            Map<String, String> options) {
        List<KubernetesEnvironmentVariable> variables = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            if (!option.getKey().startsWith(KubernetesConstants.ENV_PREFIX)) {
                continue;
            }
            String name = option.getKey().substring(KubernetesConstants.ENV_PREFIX.length());
            if (!name.matches(ENV_NAME_PATTERN)) {
                throw new IllegalArgumentException(
                        option.getKey() + " must use a valid environment variable name");
            }
            if (KubernetesConstants.SPECIFICATION_ENV.equals(name)) {
                throw new IllegalArgumentException(
                        KubernetesConstants.ENV_PREFIX
                                + name
                                + " must not override "
                                + KubernetesConstants.SPECIFICATION_ENV);
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException(
                        "kubernetes.env contains duplicate name " + name);
            }
            variables.add(KubernetesEnvironmentVariable.fromValue(name, option.getValue()));
        }
        return Collections.unmodifiableList(variables);
    }

    private static List<KubernetesEnvFromReference> envFromReferences(Map<String, String> options) {
        List<KubernetesEnvFromReference> references = new ArrayList<>(2);
        String secret = options.get(KubernetesConstants.ENV_FROM_SECRET);
        String configMap = options.get(KubernetesConstants.ENV_FROM_CONFIGMAP);
        if (secret != null) {
            references.add(KubernetesEnvFromReference.secret(requireReferenceName(secret)));
        }
        if (configMap != null) {
            references.add(KubernetesEnvFromReference.configMap(requireReferenceName(configMap)));
        }
        for (String key : options.keySet()) {
            if (key.startsWith(KubernetesConstants.ENV_FROM_PREFIX)
                    && !key.equals(KubernetesConstants.ENV_FROM_SECRET)
                    && !key.equals(KubernetesConstants.ENV_FROM_CONFIGMAP)) {
                throw new IllegalArgumentException(
                        key
                                + " must use "
                                + KubernetesConstants.ENV_FROM_SECRET
                                + " or "
                                + KubernetesConstants.ENV_FROM_CONFIGMAP);
            }
        }
        return Collections.unmodifiableList(references);
    }

    private static String requireReferenceName(String value) {
        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException("kubernetes.env-from values must not be empty");
        }
        return value.trim();
    }

    private static List<KubernetesConfigMapEntry> configMaps(Map<String, String> options) {
        Map<String, String> references = new LinkedHashMap<>();
        Map<String, String> createdNames = new LinkedHashMap<>();
        Map<String, Map<String, String>> data = new LinkedHashMap<>();
        Map<String, Map<String, String>> mounts = new LinkedHashMap<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            String key = option.getKey();
            if (!key.startsWith(KubernetesConstants.CONFIG_MAP_PREFIX)) {
                continue;
            }
            String remainder = key.substring(KubernetesConstants.CONFIG_MAP_PREFIX.length());
            int entrySeparator = remainder.indexOf('.');
            if (entrySeparator <= 0 || entrySeparator == remainder.length() - 1) {
                throw invalidConfigMapEntry(key);
            }
            String entry = remainder.substring(0, entrySeparator);
            if (!entry.matches(DNS_LABEL_PATTERN)) {
                throw new IllegalArgumentException(
                        key + ": ConfigMap entry names must be lowercase DNS labels, got " + entry);
            }
            String path = remainder.substring(entrySeparator + 1);
            if (SECTION_REF.equals(path)) {
                references.put(entry, option.getValue());
                continue;
            }
            if (SECTION_NAME.equals(path)) {
                createdNames.put(entry, option.getValue());
                continue;
            }
            int sectionSeparator = path.indexOf('.');
            if (sectionSeparator <= 0 || sectionSeparator == path.length() - 1) {
                throw invalidConfigMapEntry(key);
            }
            String section = path.substring(0, sectionSeparator);
            String item = path.substring(sectionSeparator + 1);
            if (!item.matches(DATA_KEY_PATTERN)) {
                throw new IllegalArgumentException(
                        key + " must use a valid Kubernetes ConfigMap data key");
            }
            if (SECTION_DATA.equals(section)) {
                putConfigMapValue(data, entry, item, option.getValue());
            } else if (SECTION_MOUNT.equals(section)) {
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
                        KubernetesConstants.CONFIG_MAP_PREFIX
                                + entry
                                + " cannot combine ref with data entries");
            }
            if (reference == null && dataEntries.isEmpty()) {
                throw new IllegalArgumentException(
                        KubernetesConstants.CONFIG_MAP_PREFIX
                                + entry
                                + (createdName == null
                                        ? " requires ref or data entries"
                                        : ".name requires data entries"));
            }
            if (reference != null && mountPaths.isEmpty()) {
                throw new IllegalArgumentException(
                        KubernetesConstants.CONFIG_MAP_PREFIX
                                + entry
                                + " references require mount entries");
            }
            if (reference == null && !mountPaths.keySet().equals(dataEntries.keySet())) {
                throw new IllegalArgumentException(
                        KubernetesConstants.CONFIG_MAP_PREFIX
                                + entry
                                + " must declare a full mount path for every data entry");
            }
            if (reference != null && createdName != null) {
                throw new IllegalArgumentException(
                        KubernetesConstants.CONFIG_MAP_PREFIX
                                + entry
                                + " cannot combine name with ref");
            }
            if (createdName != null && !createdName.matches(DNS_SUBDOMAIN_PATTERN)) {
                throw new IllegalArgumentException(
                        KubernetesConstants.CONFIG_MAP_PREFIX
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
                key
                        + " must use "
                        + KubernetesConstants.CONFIG_MAP_PREFIX
                        + "<entry>.ref, .data.<key> or .mount.<key>");
    }

    private static void putConfigMapValue(
            Map<String, Map<String, String>> values, String entry, String key, String value) {
        Map<String, String> entryValues =
                values.computeIfAbsent(entry, ignored -> new LinkedHashMap<>());
        if (entryValues.put(key, value) != null) {
            throw new IllegalArgumentException(
                    KubernetesConstants.CONFIG_MAP_PREFIX
                            + entry
                            + " contains duplicate key "
                            + key);
        }
    }

    private void writeConfigMaps(Properties properties) {
        for (KubernetesConfigMapEntry configMap : configMaps) {
            String prefix = KubernetesConstants.CONFIG_MAP_PREFIX + configMap.getEntryName();
            if (configMap.isReference()) {
                properties.setProperty(prefix + "." + SECTION_REF, configMap.getReference());
            } else {
                if (configMap.getName() != null) {
                    properties.setProperty(prefix + "." + SECTION_NAME, configMap.getName());
                }
                for (Map.Entry<String, String> entry : configMap.getData().entrySet()) {
                    // The master only needs the key set; values are used client-side only.
                    properties.setProperty(prefix + "." + SECTION_DATA + "." + entry.getKey(), "");
                }
            }
            for (Map.Entry<String, String> mount : configMap.getMountPaths().entrySet()) {
                properties.setProperty(
                        prefix + "." + SECTION_MOUNT + "." + mount.getKey(), mount.getValue());
            }
        }
    }

    private void validateApplicationSecret() {
        if (applicationSecretRef == null) {
            if (applicationSecretName != null && applicationSecretData.isEmpty()) {
                throw new IllegalArgumentException(
                        KubernetesConstants.SECRET_OPTION
                                + ".name requires "
                                + KubernetesConstants.SECRET_DATA_OPTION
                                + " entries");
            }
            return;
        }
        if (applicationSecretName != null || !applicationSecretData.isEmpty()) {
            throw new IllegalArgumentException(
                    KubernetesConstants.OPTION_PREFIX
                            + "secret-ref cannot be combined with "
                            + KubernetesConstants.SECRET_OPTION);
        }
    }

    private void validateConfigMapMounts() {
        Set<String> mountPaths = new HashSet<>();
        for (KubernetesConfigMapEntry entry : configMaps) {
            for (String path : entry.getMountPaths().values()) {
                if (!mountPaths.add(path)) {
                    throw new IllegalArgumentException(
                            KubernetesConstants.CONFIG_MAP_OPTION
                                    + " entries mount multiple keys to "
                                    + path);
                }
                for (String directory : Arrays.asList(secretDirectory, checkpointDirectory)) {
                    if (path.equals(directory)
                            || path.startsWith(directory + "/")
                            || directory.startsWith(path + "/")) {
                        throw new IllegalArgumentException(
                                KubernetesConstants.CONFIG_MAP_OPTION
                                        + " mount path "
                                        + path
                                        + " overlaps the reserved directory "
                                        + directory);
                    }
                }
            }
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
                .map(entry -> entry.getKey() + ":" + entry.getValue().replace(",", "\\,"))
                .collect(Collectors.joining(","));
    }

    /**
     * Rejects unknown {@code kubernetes.*} options so a misspelled key fails fast instead of being
     * dropped silently.
     *
     * <p>Keys with dynamic names, such as {@code kubernetes.env.<NAME>} or {@code
     * kubernetes.configmap.<entry>.data.<key>}, are validated by their own parsers.
     *
     * @param options raw deployment options
     * @throws IllegalArgumentException if an option is neither declared nor dynamic
     */
    private static void validateDeclaredOptions(Map<String, String> options) {
        Set<String> declared = OptionUtil.getDeclaredOptionKeys(KubernetesOptions.class);
        for (String key : options.keySet()) {
            if (!key.startsWith(KubernetesConstants.OPTION_PREFIX)
                    || declared.contains(key)
                    || startsWithAny(key, DYNAMIC_OPTION_PREFIXES)) {
                continue;
            }
            throw new IllegalArgumentException(
                    "Unknown Kubernetes option " + key + "; check the option name");
        }
    }

    private static boolean startsWithAny(String value, List<String> prefixes) {
        for (String prefix : prefixes) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
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
            if (!value.matches(FILE_NAME_PATTERN)) {
                throw new IllegalArgumentException(
                        option.key()
                                + " must be a file name under <seatunnel-home>/config or an absolute path");
            }
            path = configDirectory(seatunnelHome) + value;
        }
        String lowerCase = path.toLowerCase(Locale.ROOT);
        if (!hasSuffix(lowerCase, HAZELCAST_SUFFIXES)) {
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
        if (KubernetesConstants.DISABLED.equalsIgnoreCase(value)) {
            return null;
        }
        return configFilePath(option, value, seatunnelHome);
    }

    /**
     * Resolves a runtime configuration file: a bare file name under {@code <seatunnel-home>/config}
     * or an absolute container path.
     *
     * @param option option being resolved, used for error messages
     * @param value configured value
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute container path
     */
    private static String configFilePath(
            Option<String> option, String value, String seatunnelHome) {
        if (value.startsWith("/")) {
            if (value.contains(":")) {
                throw new IllegalArgumentException(option.key() + " must not contain ':'");
            }
            return value;
        }
        if (!value.matches(FILE_NAME_PATTERN)) {
            throw new IllegalArgumentException(
                    option.key()
                            + " must be a file name under <seatunnel-home>/config or an absolute path");
        }
        return configDirectory(seatunnelHome) + value;
    }

    /**
     * Resolves the SeaTunnel engine configuration file passed as {@code -Dseatunnel.config}.
     *
     * @param options resolved deployment settings
     * @param seatunnelHome distribution home used for relative paths
     * @return absolute container path
     */
    private static String seatunnelConfigFile(ReadonlyConfig options, String seatunnelHome) {
        String value = requireNonBlank(options, KubernetesOptions.SEATUNNEL_CONFIG_FILE);
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
            if (value.contains(":") || !value.matches(RELATIVE_PATH_PATTERN)) {
                throw new IllegalArgumentException(
                        KubernetesOptions.SEATUNNEL_CONFIG_FILE.key()
                                + " must be a path relative to <seatunnel-home> or an absolute path");
            }
            path = seatunnelHome + (seatunnelHome.endsWith("/") ? "" : "/") + value;
        }
        String lowerCase = path.toLowerCase(Locale.ROOT);
        if (!hasSuffix(lowerCase, ENGINE_SUFFIXES)) {
            throw new IllegalArgumentException(
                    KubernetesOptions.SEATUNNEL_CONFIG_FILE.key()
                            + " must reference a .yaml or .yml file");
        }
        return path;
    }

    /**
     * Resolves the container path whose file replaces the inline job configuration. A file name
     * resolves under {@code <seatunnel-home>/config} (the ConfigMap-friendly form, no Secret
     * permission needed); an absolute path supports any mounted file, for example {@code job.conf}
     * inside the application Secret directory.
     *
     * @param options resolved deployment settings
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute job configuration path, or {@code null} when the submitter's content is used
     *     as-is
     */
    private static String resolveJobConfigFile(ReadonlyConfig options, String seatunnelHome) {
        String value = options.get(KubernetesOptions.JOB_CONFIG_FILE);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        if (value.startsWith("/")) {
            if (value.contains(":")) {
                throw new IllegalArgumentException(
                        KubernetesOptions.JOB_CONFIG_FILE.key() + " must not contain ':'");
            }
            return value;
        }
        if (!value.matches(FILE_NAME_PATTERN)) {
            throw new IllegalArgumentException(
                    KubernetesOptions.JOB_CONFIG_FILE.key()
                            + " must be a file name under <seatunnel-home>/config or an absolute path");
        }
        return configDirectory(seatunnelHome) + value;
    }

    /**
     * Requires a job configuration source: the content the submitter carried, or the file mounted
     * through {@code kubernetes.job-config-file}. Providers validate this before creating the first
     * resource so a submission without any job fails immediately.
     *
     * @param inlineJobConfiguration job content the submitter carried, possibly empty
     * @param jobConfigFile resolved mounted file, or {@code null} when no replacement is configured
     */
    private static void requireJobConfiguration(
            String inlineJobConfiguration, String jobConfigFile) {
        boolean mountedFileConfigured = jobConfigFile != null;
        boolean inlineContentSubmitted =
                inlineJobConfiguration != null && !inlineJobConfiguration.trim().isEmpty();
        if (!mountedFileConfigured && !inlineContentSubmitted) {
            throw new IllegalArgumentException(
                    "The submission carries no job configuration; pass -c or configure "
                            + KubernetesOptions.JOB_CONFIG_FILE.key());
        }
    }

    private static String configDirectory(String seatunnelHome) {
        return seatunnelHome + (seatunnelHome.endsWith("/") ? "" : "/") + "config/";
    }

    /** @return whether the lower-cased path ends with one of the accepted suffixes */
    private static boolean hasSuffix(String path, List<String> suffixes) {
        for (String suffix : suffixes) {
            if (path.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves the Log4j2 configuration file passed as {@code -Dlog4j2.configurationFile}.
     *
     * @param options resolved deployment settings
     * @param seatunnelHome distribution home used for relative file names
     * @return absolute container path
     */
    private static String log4jConfigFile(ReadonlyConfig options, String seatunnelHome) {
        String value = requireNonBlank(options, KubernetesOptions.LOG4J_CONFIG_FILE);
        String path = configFilePath(KubernetesOptions.LOG4J_CONFIG_FILE, value, seatunnelHome);
        String lowerCase = path.toLowerCase(Locale.ROOT);
        if (!hasSuffix(lowerCase, LOG4J_SUFFIXES)) {
            throw new IllegalArgumentException(
                    KubernetesOptions.LOG4J_CONFIG_FILE.key()
                            + " must reference a Log4j2 configuration file");
        }
        return path;
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

    private static int requirePositive(ReadonlyConfig options, Option<Integer> option) {
        int value = options.get(option);
        if (value < 1) {
            throw new IllegalArgumentException(option.key() + " must be positive");
        }
        return value;
    }

    private static Long optionalPositive(ReadonlyConfig options, Option<Long> option) {
        Long value = options.get(option);
        if (value != null && value < 1) {
            throw new IllegalArgumentException(option.key() + " must be positive");
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
        if (labels.containsKey(KubernetesConstants.APPLICATION_LABEL)
                || labels.containsKey(KubernetesConstants.ROLE_LABEL)) {
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
        for (String pair : splitPairs(value)) {
            int separator = pair.indexOf(':');
            if (separator <= 0 || separator == pair.length() - 1) {
                throw new IllegalArgumentException(
                        option.key() + " must contain comma-separated key:value pairs");
            }
            String key = pair.substring(0, separator).trim();
            String configuredValue = pair.substring(separator + 1).trim().replace("\\,", ",");
            if (key.isEmpty()
                    || configuredValue.isEmpty()
                    || result.put(key, configuredValue) != null) {
                throw new IllegalArgumentException(
                        option.key() + " contains an empty or duplicate key:value pair");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Splits comma-separated pairs while keeping an escaped comma ({@code \,}) inside one pair.
     *
     * @param value configured pairs
     * @return the raw pairs, with escapes preserved for the value parser
     */
    private static List<String> splitPairs(String value) {
        List<String> pairs = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (escaped) {
                current.append('\\').append(character);
                escaped = false;
            } else if (character == '\\') {
                escaped = true;
            } else if (character == ',') {
                pairs.add(current.toString());
                current.setLength(0);
            } else {
                current.append(character);
            }
        }
        if (escaped) {
            current.append('\\');
        }
        pairs.add(current.toString());
        return pairs;
    }
}
