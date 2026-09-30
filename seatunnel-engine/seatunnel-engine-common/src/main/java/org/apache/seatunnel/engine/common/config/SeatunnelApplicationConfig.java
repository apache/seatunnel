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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;

import com.hazelcast.config.JoinConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Shared application configuration loading, localization and Engine member preparation.
 *
 * <p>On the submitter, {@link #load(Path, Map)} merges the application file and overrides; {@link
 * #parse(Path, Map)} reads the separate job file and resolves common application fields. Platform
 * descriptors consume platform options separately, without storing them in the specification.
 *
 * <p>For remote startup, {@link #toProperties(ApplicationSpecification)} and {@link
 * #fromProperties(Properties)} encode/decode the resolved application fields. The platform owns the
 * runtime file or Secret, its permissions and its platform-specific fields. This utility neither
 * owns resources nor changes the existing seatunnel.yaml/Hazelcast configuration loading.
 *
 * <p>After that existing configuration has been loaded, {@link #configure(SeaTunnelConfig, String,
 * String, int)} and {@link #configureCheckpointRetention(SeaTunnelConfig)} apply application-mode
 * settings before member creation. They mutate only caller-owned configuration and never start
 * members, allocate workers or close runtime resources.
 */
public final class SeatunnelApplicationConfig {
    private static final String FORMAT_VERSION = "V1";
    private static final String VERSION_KEY = "format.version";
    private static final String JOB_CONFIG_KEY = "job.config";

    private SeatunnelApplicationConfig() {}

    /**
     * Loads the submitter's optional HOCON application file and applies deployment overrides.
     *
     * <p>Overrides win over file values and are merged before resolving substitutions. Defaults are
     * applied later by the common/platform options, not inserted into this map. Both nested HOCON
     * and literal dotted keys are returned as the dotted keys declared by those options. An
     * explicit path must exist; a null path means no file, not automatic configuration discovery.
     *
     * @param applicationConfig submitter-local application file, or null for overrides only
     * @param overrides highest-priority values; the caller's map is not modified
     * @return resolved common and platform options, without a job configuration
     * @throws org.apache.seatunnel.shade.com.typesafe.config.ConfigException if the file cannot be
     *     read, parsed or resolved
     */
    public static Map<String, String> load(Path applicationConfig, Map<String, String> overrides) {
        Config fileOptions =
                applicationConfig == null
                        ? ConfigFactory.empty()
                        : ConfigFactory.parseFile(applicationConfig.toFile(), hoconParseOptions());
        Config normalizedOptions = ConfigFactory.empty();
        for (Map.Entry<String, ConfigValue> entry : fileOptions.entrySet()) {
            // SeaTunnel HOCON uses "->" for nested paths, while Option keys use dots. Keep
            // ConfigValue intact here: unwrapping it before merging would break substitutions.
            normalizedOptions =
                    normalizedOptions.withValue(
                            String.join(".", ConfigUtil.splitPath(entry.getKey())),
                            entry.getValue());
        }
        return TypesafeConfigUtils.configToMap(
                ConfigFactory.parseMap(overrides).withFallback(normalizedOptions).resolve());
    }

    /**
     * Reads a submitter-local job file and builds its application specification.
     *
     * <p>The job is resolved independently of deployment options: worker counts, queues and other
     * deployment overrides must not alter source/transform/sink configuration. Resolved JSON is
     * carried to the master, so the original job path is not needed there.
     *
     * @param jobConfig submitter-local HOCON job file, regardless of filename suffix
     * @param options merged deployment options from {@link #load(Path, Map)} or an embedding caller
     * @return immutable, platform-independent application specification
     */
    public static ApplicationSpecification parse(Path jobConfig, Map<String, String> options) {
        String resolvedJob =
                ConfigFactory.parseFile(jobConfig.toFile(), hoconParseOptions())
                        .resolve()
                        .root()
                        .render(ConfigRenderOptions.concise());
        return parse(resolvedJob, options);
    }

    private static ConfigParseOptions hoconParseOptions() {
        return ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF).setAllowMissing(false);
    }

    /**
     * Builds a specification from job content already resolved by the caller.
     *
     * <p>Only common application options are read. Platform options are neither interpreted nor
     * retained. Defaults and an absent job ID are resolved once on submission; the resulting ID
     * travels with the specification and must not be regenerated by the remote master.
     *
     * @param jobConfig resolved job content, normally JSON; may contain credentials
     * @param options common and platform deployment options; not modified
     * @return immutable application fields, without the original options map
     */
    public static ApplicationSpecification parse(String jobConfig, Map<String, String> options) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>(options));
        Long jobId = config.get(ApplicationOptions.JOB_ID);
        return new ApplicationSpecification(
                config.get(ApplicationOptions.NAME),
                jobConfig,
                jobId != null
                        ? jobId
                        : (UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE) | 1L,
                config.get(ApplicationOptions.RESTORE_JOB_ID),
                config.get(ApplicationOptions.WORKER_COUNT),
                new WorkerSpecification(
                        config.get(ApplicationOptions.WORKER_MEMORY_MB),
                        config.get(ApplicationOptions.WORKER_CPU_CORES),
                        config.get(ApplicationOptions.WORKER_SLOTS)),
                config.get(ApplicationOptions.MASTER_MEMORY_MB),
                config.get(ApplicationOptions.MASTER_CPU_CORES),
                config.get(ApplicationOptions.MASTER_PORT),
                config.get(ApplicationOptions.STARTUP_TIMEOUT_MILLIS));
    }

    /**
     * Encodes only application fields; the platform owns writing and protecting the resulting
     * content.
     *
     * <p>This is the generated runtime representation, not a copy of the user's application.config.
     * The platform may add its required runtime fields before writing application.properties. Never
     * log these properties: job.config can contain connector credentials.
     */
    public static Properties toProperties(ApplicationSpecification specification) {
        Properties properties = new Properties();
        properties.setProperty(VERSION_KEY, FORMAT_VERSION);
        properties.setProperty(JOB_CONFIG_KEY, specification.getJobConfig());
        properties.setProperty(ApplicationOptions.NAME.key(), specification.getName());
        properties.setProperty(
                ApplicationOptions.JOB_ID.key(), Long.toString(specification.getJobId()));
        if (specification.getRestoreJobId() != null) {
            properties.setProperty(
                    ApplicationOptions.RESTORE_JOB_ID.key(),
                    specification.getRestoreJobId().toString());
        }
        properties.setProperty(
                ApplicationOptions.WORKER_COUNT.key(),
                Integer.toString(specification.getWorkerCount()));
        WorkerSpecification worker = specification.getWorkerSpecification();
        properties.setProperty(
                ApplicationOptions.WORKER_MEMORY_MB.key(), Integer.toString(worker.getMemoryMb()));
        properties.setProperty(
                ApplicationOptions.WORKER_CPU_CORES.key(), Integer.toString(worker.getCpuCores()));
        properties.setProperty(
                ApplicationOptions.WORKER_SLOTS.key(), Integer.toString(worker.getSlots()));
        properties.setProperty(
                ApplicationOptions.MASTER_MEMORY_MB.key(),
                Integer.toString(specification.getMasterMemoryMb()));
        properties.setProperty(
                ApplicationOptions.MASTER_CPU_CORES.key(),
                Integer.toString(specification.getMasterCpuCores()));
        properties.setProperty(
                ApplicationOptions.MASTER_PORT.key(),
                Integer.toString(specification.getMasterPort()));
        properties.setProperty(
                ApplicationOptions.STARTUP_TIMEOUT_MILLIS.key(),
                Long.toString(specification.getStartupTimeoutMillis()));
        return properties;
    }

    /**
     * Restores application fields from a localized runtime file without reading submitter files.
     *
     * <p>The platform reads the file/Secret and handles its own properties. A missing job ID is an
     * invalid runtime file, not a request to generate a new job identity on the master.
     */
    public static ApplicationSpecification fromProperties(Properties properties)
            throws IOException {
        if (!FORMAT_VERSION.equals(properties.getProperty(VERSION_KEY))) {
            throw new IOException("Unsupported localized application configuration version");
        }
        if (!properties.containsKey(ApplicationOptions.JOB_ID.key())) {
            throw new IOException(
                    "Localized application configuration is missing application.job-id");
        }
        Map<String, String> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("application.")) {
                options.put(key, properties.getProperty(key));
            }
        }
        try {
            return parse(properties.getProperty(JOB_CONFIG_KEY), options);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IOException("Invalid localized application configuration", e);
        }
    }

    /** Uses the platform identity so workers and later clients can locate the same cluster. */
    public static String clusterName(String applicationId) {
        return "seatunnel-application-" + applicationId;
    }

    /**
     * Selects application membership, node role, fixed slots and distribution-local connector jars.
     *
     * <p>A null master address configures the sole master. Workers require the specified master and
     * cannot start an independent cluster. Both roles use TCP discovery without inherited session
     * peers, zero backup replicas and lifecycle hooks owned by the application runtime. This method
     * mutates only the supplied configuration and does not transfer its ownership.
     *
     * @param config exclusively owned configuration to prepare before member startup
     * @param clusterName nonempty identity shared only by this application's members
     * @param masterAddress master host and port for workers, or null for the master
     * @param slots fixed slot count already validated by the deployment specification
     * @throws IllegalArgumentException if the cluster name or a supplied master address is empty
     * @throws NullPointerException if config is null
     */
    public static void configure(
            SeaTunnelConfig config, String clusterName, String masterAddress, int slots) {
        if (clusterName == null || clusterName.trim().isEmpty()) {
            throw new IllegalArgumentException("An application cluster name is required");
        }
        boolean worker = masterAddress != null;
        JoinConfig join = new JoinConfig();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        // Hazelcast requires every member to use the same joiner. The master starts alone with
        // no discovery peers; workers join its explicit address in this unique application cluster.
        join.getTcpIpConfig().setEnabled(true);
        if (worker) {
            if (masterAddress.trim().isEmpty()) {
                throw new IllegalArgumentException("An application master address is required");
            }
            // requiredMember prevents a worker from forming its own cluster if the master is down.
            join.getTcpIpConfig()
                    .addMember(masterAddress)
                    .setRequiredMember(masterAddress)
                    .setConnectionTimeoutSeconds(10);
        }
        config.getHazelcastConfig().setClusterName(clusterName).setLiteMember(worker);
        config.getHazelcastConfig().getNetworkConfig().setJoin(join).setPublicAddress(null);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getHazelcastConfig().setProperty("hazelcast.discovery.enabled", "false");
        config.getHazelcastConfig().setProperty("hazelcast.graceful.shutdown.max.wait", "10");
        config.getHazelcastConfig().setProperty("hazelcast.max.join.seconds", "60");
        config.getEngineConfig()
                .setClusterRole(
                        worker ? EngineConfig.ClusterRole.WORKER : EngineConfig.ClusterRole.MASTER);
        config.getEngineConfig().setBackupCount(0);
        config.getEngineConfig().setMode(ExecutionMode.CLUSTER);
        // Every application container localizes the same distribution. Upload identifiers contain
        // absolute master paths and are unsuitable for YARN's per-container localization roots.
        ConnectorJarStorageConfig jars = new ConnectorJarStorageConfig();
        jars.setEnable(false);
        config.getEngineConfig().setConnectorJarStorageConfig(jars);
        HttpConfig http = new HttpConfig();
        http.setEnabled(false);
        http.setEnableHttps(false);
        config.getEngineConfig().setHttpConfig(http);
        SlotServiceConfig slotConfig = new SlotServiceConfig();
        slotConfig.setDynamicSlot(false);
        slotConfig.setSlotNum(slots);
        config.getEngineConfig().setSlotServiceConfig(slotConfig);
    }

    /**
     * Configures checkpoint retention for the application lifecycle.
     *
     * <p>Application cleanup cancels an unfinished native job, including after worker failure, so
     * cancellation retains checkpoints by default. An explicit job-level retention option still
     * overrides this default. Existing timing, retention limits, and plugin options are copied so
     * distribution defaults are not mutated. The complete native storage backend is preserved
     * without interpreting plugin options, including credentials and endpoint settings.
     *
     * <p>Call before master startup with exclusive access to the configuration. The replacement
     * checkpoint configuration and its plugin map are owned by the caller; prior configuration
     * objects remain unchanged. This method neither opens storage nor validates backend options.
     *
     * @param config exclusively owned application master configuration to update
     * @throws NullPointerException if config or its native checkpoint settings are null
     */
    public static void configureCheckpointRetention(SeaTunnelConfig config) {
        CheckpointConfig previous = config.getEngineConfig().getCheckpointConfig();
        CheckpointConfig checkpoint = new CheckpointConfig();
        checkpoint.setCheckpointInterval(previous.getCheckpointInterval());
        checkpoint.setCheckpointTimeout(previous.getCheckpointTimeout());
        checkpoint.setCheckpointMinPause(previous.getCheckpointMinPause());
        checkpoint.setSchemaChangeCheckpointTimeout(previous.getSchemaChangeCheckpointTimeout());
        checkpoint.setRetainAfterJobCancelled(true);
        checkpoint.setCheckpointEnable(previous.isCheckpointEnable());
        CheckpointStorageConfig storage = new CheckpointStorageConfig();
        storage.setStorage(previous.getStorage().getStorage());
        storage.setMaxRetainedCheckpoints(previous.getStorage().getMaxRetainedCheckpoints());
        Map<String, String> options = new HashMap<>(previous.getStorage().getStoragePluginConfig());
        storage.setStoragePluginConfig(options);
        checkpoint.setStorage(storage);
        config.getEngineConfig().setCheckpointConfig(checkpoint);
    }
}
