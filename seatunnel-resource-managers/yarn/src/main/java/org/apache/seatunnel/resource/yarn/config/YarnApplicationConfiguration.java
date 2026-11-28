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

package org.apache.seatunnel.resource.yarn.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnFileSource;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResourceGroup;

import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Validated YARN deployment settings kept separate from the application specification.
 *
 * <p>{@link YarnOptions} contains only public option declarations. This object resolves defaults,
 * validates local inputs and derives master/worker scheduling values before the deployer or
 * resource-manager driver performs external operations.
 */
@Getter
public final class YarnApplicationConfiguration {
    private final ApplicationSpecification specification;
    private final YarnDeploymentTarget deploymentTarget;
    private final URI distributionUri;
    private final YarnFileSource jobFile;
    private final YarnContainerSettings containerSettings;
    private final Map<String, YarnFileSource> shipFiles;
    private final Long activeDeadlineSeconds;
    private final Path stagingRoot;
    private final String queue;
    private final int priority;
    private final Set<String> tags;
    private final String masterNodeLabel;
    private final Integer masterHttpPort;
    private final String workerNodeLabel;
    private final String hadoopUserName;

    private YarnApplicationConfiguration(
            ApplicationSpecification specification,
            ReadonlyConfig options,
            boolean requireDistribution) {
        this.specification = specification;
        this.deploymentTarget = options.get(YarnOptions.DEPLOYMENT_TARGET);
        String distributionPath = options.get(YarnOptions.DISTRIBUTION);
        if (distributionPath == null || distributionPath.trim().isEmpty()) {
            if (requireDistribution) {
                throw new IllegalArgumentException("Required option yarn.distribution is missing");
            }
            this.distributionUri = null;
        } else {
            this.distributionUri =
                    YarnFileSource.sourceUri(distributionPath, YarnOptions.DISTRIBUTION.key());
            File local = getDistribution();
            if (local != null
                    && (!Files.isRegularFile(local.toPath())
                            || !Files.isReadable(local.toPath()))) {
                throw new IllegalArgumentException(
                        "yarn.distribution must be a readable local distribution archive: "
                                + local);
            }
        }
        String job = emptyToNull(options.get(YarnOptions.JOB_CONFIG_FILE));
        Map<String, YarnFileSource> sources = YarnLocalResourceGroup.resolve(options);
        YarnFileSource selected = sources.remove(job);
        this.jobFile =
                selected != null || job == null
                        ? selected
                        : YarnFileSource.fromUri(
                                YarnFileSource.sourceUri(job, YarnOptions.JOB_CONFIG_FILE.key()));
        if (requireDistribution
                && jobFile == null
                && specification.getJobConfig().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Pass --config or configure " + YarnOptions.JOB_CONFIG_FILE.key());
        }
        this.shipFiles = Collections.unmodifiableMap(sources);
        this.containerSettings = new YarnContainerSettings(options, sources.keySet());
        this.activeDeadlineSeconds = options.get(YarnOptions.ACTIVE_DEADLINE_SECONDS);
        if (activeDeadlineSeconds != null && activeDeadlineSeconds <= 0) {
            throw new IllegalArgumentException(
                    YarnOptions.ACTIVE_DEADLINE_SECONDS.key() + " must be positive");
        }
        this.stagingRoot = new Path(options.get(YarnOptions.STAGING_DIRECTORY));
        this.queue = options.get(YarnOptions.QUEUE).trim();
        if (queue.isEmpty()) {
            throw new IllegalArgumentException("yarn.queue must not be empty");
        }
        this.priority = options.get(YarnOptions.PRIORITY);
        if (priority < -1) {
            throw new IllegalArgumentException("yarn.priority must be -1 or greater");
        }
        this.tags = parseTags(options.get(YarnOptions.TAGS));
        this.masterNodeLabel = emptyToNull(options.get(YarnOptions.MASTER_NODE_LABEL));
        this.masterHttpPort = options.get(YarnOptions.MASTER_HTTP_PORT);
        if (masterHttpPort != null && (masterHttpPort < 1 || masterHttpPort > 65535)) {
            throw new IllegalArgumentException(
                    YarnOptions.MASTER_HTTP_PORT.key() + " must be between 1 and 65535");
        }
        String worker = emptyToNull(options.get(YarnOptions.WORKER_NODE_LABEL));
        this.workerNodeLabel = worker == null ? masterNodeLabel : worker;
        this.hadoopUserName =
                YarnSecurityContext.simpleUser(options.get(YarnOptions.HADOOP_USER_NAME))
                        .getShortUserName();
    }

    /**
     * Resolves the archive location. Paths without a scheme are relative to the submitter's working
     * directory even when Hadoop uses HDFS by default.
     *
     * @param specification immutable application launch specification
     * @param options platform deployment settings
     * @return YARN configuration for client-side deployment
     */
    public static YarnApplicationConfiguration forSubmission(
            ApplicationSpecification specification, ReadonlyConfig options) {
        return new YarnApplicationConfiguration(specification, options, true);
    }

    /** Returns the local archive for a file source, or null for an HDFS source or a container. */
    public File getDistribution() {
        return distributionUri != null && "file".equals(distributionUri.getScheme())
                ? new File(distributionUri)
                : null;
    }

    /**
     * Resolves parameters needed by the localized ApplicationMaster and worker allocator.
     *
     * <p>The AM reads the generated application properties from its working directory. When the
     * submitter selected a separate job file, the AM loads its sibling {@code job.conf}. The
     * properties also carry the worker node label and Hadoop user needed for worker allocation.
     *
     * @param path localized application configuration file
     * @return the AM's application specification and resolved worker settings
     * @throws IOException if the properties or the selected job file cannot be read, or the job
     *     file is empty
     */
    public static YarnApplicationConfiguration read(java.nio.file.Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        if (properties.containsKey(YarnOptions.JOB_CONFIG_FILE.key())) {
            java.nio.file.Path jobFile =
                    path.resolveSibling(YarnConstants.LOCALIZED_JOB_CONFIG_NAME);
            String job = Files.readString(jobFile, StandardCharsets.UTF_8);
            if (job.trim().isEmpty()) {
                throw new IOException(YarnOptions.JOB_CONFIG_FILE.key() + " contains an empty job");
            }
            properties.setProperty(SeatunnelApplicationConfig.JOB_CONFIG_KEY, job);
        }
        Map<String, Object> options = new HashMap<>();
        if (properties.containsKey(YarnOptions.MASTER_HTTP_PORT.key())) {
            options.put(
                    YarnOptions.MASTER_HTTP_PORT.key(),
                    properties.getProperty(YarnOptions.MASTER_HTTP_PORT.key()));
        }
        options.put(
                YarnOptions.WORKER_NODE_LABEL.key(),
                properties.getProperty(YarnOptions.WORKER_NODE_LABEL.key(), ""));
        options.put(
                YarnOptions.HADOOP_USER_NAME.key(),
                properties.getProperty(YarnOptions.HADOOP_USER_NAME.key()));
        return new YarnApplicationConfiguration(
                SeatunnelApplicationConfig.fromProperties(properties),
                ReadonlyConfig.fromMap(options),
                false);
    }

    /**
     * Writes common application fields, resolved worker placement and Hadoop user for the remote
     * master.
     *
     * <p>When a separate job file is staged, the properties record its container-local name. The
     * uploader owns the writer and the private directory holding the job content.
     *
     * @param writer destination for the AM's application properties, closed by the caller
     * @throws IOException if the properties cannot be written
     */
    public void write(Writer writer) throws IOException {
        Properties properties = SeatunnelApplicationConfig.toProperties(specification);
        if (jobFile != null) {
            properties.remove(SeatunnelApplicationConfig.JOB_CONFIG_KEY);
            properties.setProperty(
                    YarnOptions.JOB_CONFIG_FILE.key(), YarnConstants.LOCALIZED_JOB_CONFIG_NAME);
        }
        if (workerNodeLabel != null) {
            properties.setProperty(YarnOptions.WORKER_NODE_LABEL.key(), workerNodeLabel);
        }
        if (masterHttpPort != null) {
            properties.setProperty(YarnOptions.MASTER_HTTP_PORT.key(), masterHttpPort.toString());
        }
        properties.setProperty(YarnOptions.HADOOP_USER_NAME.key(), hadoopUserName);
        properties.store(writer, "SeaTunnel YARN application");
    }

    private static Set<String> parseTags(String configuredTags) {
        if (configuredTags == null || configuredTags.trim().isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> tags = new LinkedHashSet<>();
        for (String value : configuredTags.split(",")) {
            String tag = value.trim();
            if (tag.isEmpty()) {
                throw new IllegalArgumentException("yarn.tags must not contain empty entries");
            }
            tags.add(tag);
        }
        return Collections.unmodifiableSet(tags);
    }

    private static String emptyToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }
}
