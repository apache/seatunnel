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

package org.apache.seatunnel.resource.core.application;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * Immutable launch specification for exactly one job in an isolated Zeta application.
 *
 * <p>The job is resolved HOCON content, not a client-local path. Worker capacity is fixed for the
 * application lifetime. Platform options are defensively copied, and all readers may safely share
 * an instance. This object is a launch artifact, not checkpoint state or a public job-config
 * format. It intentionally has no content-bearing {@code toString()} because options may contain
 * secrets.
 */
@Getter
public final class ApplicationSpecification {

    /** Current serialization version for specifications localized by platform providers. */
    private static final String FORMAT_VERSION = "1";

    /** Stable property names shared by specification writers and application entrypoints. */
    private static final String FORMAT_VERSION_PROPERTY = "format.version";

    private static final String DEPLOY_TYPE_PROPERTY = "deploy.type";
    private static final String NAME_PROPERTY = "name";
    private static final String JOB_CONFIG_PROPERTY = "job.config";
    private static final String WORKER_COUNT_PROPERTY = "worker.count";
    private static final String WORKER_MEMORY_PROPERTY = "worker.memory";
    private static final String WORKER_CPU_PROPERTY = "worker.cpu";
    private static final String WORKER_SLOTS_PROPERTY = "worker.slots";

    /** Prefix separating deployment options from the specification's structural properties. */
    private static final String OPTION_PREFIX = "option.";
    /** External resource platform responsible for this application. */
    private final DeployType deployType;
    /** Human-readable platform display name, not a unique cluster identity. */
    private final String name;
    /** Fully resolved job configuration; may contain credentials and must not be logged. */
    private final String jobConfig;
    /** Number of workers required before submitting the job. */
    private final int workerCount;
    /** Per-worker resource envelope and fixed execution-slot capacity. */
    private final WorkerSpecification workerSpecification;
    /** Immutable scalar deployment options, including platform-specific settings. */
    private final Map<String, String> options;
    /** Typed view over a private immutable snapshot; reused by every option lookup. */
    @Getter(AccessLevel.NONE)
    private final ReadonlyConfig configuration;

    /**
     * Validates and freezes a specification before any external resources are created.
     *
     * @param deployType YARN or KUBERNETES; standalone does not use this lifecycle
     * @param name nonempty display name
     * @param jobConfig nonempty, resolved job configuration content
     * @param workerCount positive number of workers
     * @param workerSpecification non-null resources for each worker
     * @param options non-null scalar platform options, copied by this constructor
     * @throws IllegalArgumentException if the target, capacities, timeout or master port are
     *     invalid
     */
    public ApplicationSpecification(
            DeployType deployType,
            String name,
            String jobConfig,
            int workerCount,
            WorkerSpecification workerSpecification,
            Map<String, String> options) {
        this.deployType = Objects.requireNonNull(deployType, "deployType");
        if (deployType == DeployType.STANDALONE) {
            throw new IllegalArgumentException("Application mode requires YARN or KUBERNETES");
        }
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Application name must not be empty");
        }
        if (jobConfig == null || jobConfig.trim().isEmpty()) {
            throw new IllegalArgumentException("Application job configuration must not be empty");
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("application.worker-count must be positive");
        }
        this.name = name;
        this.jobConfig = jobConfig;
        this.workerCount = workerCount;
        this.workerSpecification =
                Objects.requireNonNull(workerSpecification, "workerSpecification");
        Map<String, String> resolvedOptions = new LinkedHashMap<>(options);
        resolvedOptions.computeIfAbsent(
                ApplicationOptions.JOB_ID.key(),
                ignored ->
                        Long.toString(
                                (UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE)
                                        | 1L));
        this.options = Collections.unmodifiableMap(resolvedOptions);
        this.configuration =
                ReadonlyConfig.fromMap(
                        Collections.unmodifiableMap(new HashMap<String, Object>(resolvedOptions)));
        Long restoreJobId = getOption(ApplicationOptions.RESTORE_JOB_ID);
        if (getJobId() <= 0 || (restoreJobId != null && restoreJobId <= 0)) {
            throw new IllegalArgumentException("Application job IDs must be positive");
        }
        if (restoreJobId != null && restoreJobId == getJobId()) {
            throw new IllegalArgumentException(
                    "application.job-id must differ from application.restore-job-id to preserve the source checkpoint");
        }
        if (getStartupTimeoutMillis() <= 0
                || getOption(ApplicationOptions.MASTER_MEMORY_MB) <= 0
                || getOption(ApplicationOptions.MASTER_CPU_CORES) <= 0) {
            throw new IllegalArgumentException(
                    "Application startup timeout and master resources must be positive");
        }
        int port = getOption(ApplicationOptions.MASTER_PORT);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                    "application.master.port must be between 1 and 65535");
        }
    }

    /**
     * Reads a typed deployment option using its declared conversion and default value.
     *
     * @param option option definition owned by the runtime or platform provider
     * @param <T> option value type
     * @return configured value, or the option default when absent
     * @throws IllegalArgumentException if the configured value cannot be converted
     */
    public <T> T getOption(Option<T> option) {
        return configuration.get(option);
    }

    /**
     * @return positive deadline duration for runtime startup, allocation and worker registration
     */
    public long getStartupTimeoutMillis() {
        return getOption(ApplicationOptions.STARTUP_TIMEOUT_MILLIS);
    }

    /**
     * Returns the native job identity assigned before deployment and preserved during localization.
     *
     * <p>This ID differs from the platform application ID and identifies the job's checkpoint
     * directory. A recovery submission gets its own new ID and refers to the historical ID through
     * {@link ApplicationOptions#RESTORE_JOB_ID}.
     *
     * @return positive native Zeta job ID
     */
    public long getJobId() {
        return getOption(ApplicationOptions.JOB_ID);
    }

    /**
     * Builds shared worker and master settings from the same options passed to platform providers.
     *
     * @param type external deployment platform
     * @param jobConfig resolved job content
     * @param options scalar deployment options including common and platform-specific keys
     * @return validated immutable specification
     * @throws IllegalArgumentException if required capacities or common options are invalid
     */
    public static ApplicationSpecification fromOptions(
            DeployType type, String jobConfig, Map<String, String> options) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<String, Object>(options));
        return new ApplicationSpecification(
                type,
                config.get(ApplicationOptions.NAME),
                jobConfig,
                config.get(ApplicationOptions.WORKER_COUNT),
                new WorkerSpecification(
                        config.get(ApplicationOptions.WORKER_MEMORY_MB),
                        config.get(ApplicationOptions.WORKER_CPU_CORES),
                        config.get(ApplicationOptions.WORKER_SLOTS)),
                options);
    }

    /**
     * Serializes a versioned UTF-8 properties file for platform localization.
     *
     * <p>The caller must create the containing directory/file with restricted access and remove it
     * according to the platform's staging policy. The method does not log specification contents.
     *
     * @param path destination file, replaced if it already exists
     * @throws IOException if the specification cannot be written
     */
    public void write(Path path) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            write(writer);
        }
    }

    /**
     * Serializes this specification to a caller-owned character stream.
     *
     * <p>This method does not close or log the writer. Callers can therefore write directly to a
     * platform staging stream or an in-memory Secret payload without creating a temporary file. The
     * serialized content may contain credentials and must be handled as sensitive data.
     *
     * @param writer destination owned and closed by the caller
     * @throws IOException if the specification cannot be written
     */
    public void write(Writer writer) throws IOException {
        Properties properties = new Properties();
        properties.setProperty(FORMAT_VERSION_PROPERTY, FORMAT_VERSION);
        properties.setProperty(DEPLOY_TYPE_PROPERTY, deployType.name());
        properties.setProperty(NAME_PROPERTY, name);
        properties.setProperty(JOB_CONFIG_PROPERTY, jobConfig);
        properties.setProperty(WORKER_COUNT_PROPERTY, Integer.toString(workerCount));
        properties.setProperty(
                WORKER_MEMORY_PROPERTY, Integer.toString(workerSpecification.getMemoryMb()));
        properties.setProperty(
                WORKER_CPU_PROPERTY, Integer.toString(workerSpecification.getCpuCores()));
        properties.setProperty(
                WORKER_SLOTS_PROPERTY, Integer.toString(workerSpecification.getSlots()));
        options.forEach((key, value) -> properties.setProperty(OPTION_PREFIX + key, value));
        properties.store(writer, "SeaTunnel application specification");
    }

    /**
     * Reads and validates a localized specification, rejecting unknown serialization versions.
     *
     * @param path file previously written by {@link #write(Path)}
     * @return validated immutable launch specification
     * @throws IOException if reading fails, the version is unsupported or required fields are
     *     invalid
     */
    public static ApplicationSpecification read(Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        if (!FORMAT_VERSION.equals(properties.getProperty(FORMAT_VERSION_PROPERTY))) {
            throw new IOException("Unsupported application specification format: " + path);
        }
        Map<String, String> options = new LinkedHashMap<>();
        properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith(OPTION_PREFIX))
                .forEach(
                        key ->
                                options.put(
                                        key.substring(OPTION_PREFIX.length()),
                                        properties.getProperty(key)));
        try {
            return new ApplicationSpecification(
                    DeployType.valueOf(properties.getProperty(DEPLOY_TYPE_PROPERTY)),
                    properties.getProperty(NAME_PROPERTY),
                    properties.getProperty(JOB_CONFIG_PROPERTY),
                    Integer.parseInt(properties.getProperty(WORKER_COUNT_PROPERTY)),
                    new WorkerSpecification(
                            Integer.parseInt(properties.getProperty(WORKER_MEMORY_PROPERTY)),
                            Integer.parseInt(properties.getProperty(WORKER_CPU_PROPERTY)),
                            Integer.parseInt(properties.getProperty(WORKER_SLOTS_PROPERTY))),
                    options);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IOException("Invalid application specification: " + path, e);
        }
    }
}
