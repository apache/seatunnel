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

import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
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
    private final File distribution;
    private final Path stagingRoot;
    private final String queue;
    private final int priority;
    private final Set<String> tags;
    private final String masterNodeLabel;
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
            this.distribution = null;
        } else {
            this.distribution = new File(distributionPath).getAbsoluteFile();
            if (requireDistribution && !Files.isRegularFile(distribution.toPath())) {
                throw new IllegalArgumentException(
                        "yarn.distribution must be a readable local distribution archive: "
                                + distribution);
            }
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
        String worker = emptyToNull(options.get(YarnOptions.WORKER_NODE_LABEL));
        this.workerNodeLabel = worker == null ? masterNodeLabel : worker;
        this.hadoopUserName = options.get(YarnOptions.HADOOP_USER_NAME).trim();
        if (hadoopUserName.isEmpty()) {
            throw new IllegalArgumentException("yarn.hadoop-user-name must not be empty");
        }
    }

    /**
     * Resolves submission parameters and validates the local distribution archive path.
     *
     * @param specification immutable application launch specification
     * @param options platform deployment settings
     * @return YARN configuration for client-side deployment
     */
    public static YarnApplicationConfiguration forSubmission(
            ApplicationSpecification specification, ReadonlyConfig options) {
        return new YarnApplicationConfiguration(specification, options, true);
    }

    /**
     * Resolves parameters needed by the localized ApplicationMaster and worker allocator.
     *
     * <p>YARN localizes application.properties into the container working directory. This is a
     * generated runtime file, not the submitter's original HOCON file. Only the resolved worker
     * node label and Hadoop user name are needed from YARN deployment options; Hadoop settings and
     * the staging path arrive through the localized Hadoop XML and container environment
     * respectively.
     *
     * @param path localized application configuration file
     * @return YARN configuration that does not require the submitter-local distribution path
     */
    public static YarnApplicationConfiguration read(java.nio.file.Path path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        Map<String, Object> options = new HashMap<>();
        options.put(
                YarnOptions.WORKER_NODE_LABEL.key(),
                properties.getProperty(YarnOptions.WORKER_NODE_LABEL.key(), ""));
        options.put(
                YarnOptions.HADOOP_USER_NAME.key(),
                properties.getProperty(
                        YarnOptions.HADOOP_USER_NAME.key(),
                        YarnOptions.HADOOP_USER_NAME.defaultValue()));
        return new YarnApplicationConfiguration(
                SeatunnelApplicationConfig.fromProperties(properties),
                ReadonlyConfig.fromMap(options),
                false);
    }

    /**
     * Writes common application fields, resolved worker placement and Hadoop user for the remote
     * master.
     *
     * <p>Queue, priority, tags and local distribution paths are submission inputs, not driver
     * settings. Do not serialize the original options map here. The uploader owns this writer and
     * the private staging directory because the job content may contain credentials.
     */
    public void write(Writer writer) throws IOException {
        Properties properties = SeatunnelApplicationConfig.toProperties(specification);
        if (workerNodeLabel != null) {
            properties.setProperty(YarnOptions.WORKER_NODE_LABEL.key(), workerNodeLabel);
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
