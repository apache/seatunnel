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

import org.apache.seatunnel.resource.core.application.ApplicationSpecification;

import org.apache.hadoop.fs.Path;

import lombok.Getter;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Validated YARN deployment configuration derived from one application specification.
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

    private YarnApplicationConfiguration(
            ApplicationSpecification specification, boolean requireDistribution) {
        this.specification = specification;
        this.deploymentTarget = specification.getOption(YarnOptions.DEPLOYMENT_TARGET);
        String distributionPath = specification.getOption(YarnOptions.DISTRIBUTION);
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
        this.stagingRoot = new Path(specification.getOption(YarnOptions.STAGING_DIRECTORY));
        this.queue = specification.getOption(YarnOptions.QUEUE).trim();
        if (queue.isEmpty()) {
            throw new IllegalArgumentException("yarn.queue must not be empty");
        }
        this.priority = specification.getOption(YarnOptions.PRIORITY);
        if (priority < -1) {
            throw new IllegalArgumentException("yarn.priority must be -1 or greater");
        }
        this.tags = parseTags(specification.getOption(YarnOptions.TAGS));
        this.masterNodeLabel = emptyToNull(specification.getOption(YarnOptions.MASTER_NODE_LABEL));
        String worker = emptyToNull(specification.getOption(YarnOptions.WORKER_NODE_LABEL));
        this.workerNodeLabel = worker == null ? masterNodeLabel : worker;
    }

    /**
     * Resolves submission parameters and validates the local distribution archive path.
     *
     * @param specification immutable application launch specification
     * @return YARN configuration for client-side deployment
     */
    public static YarnApplicationConfiguration forSubmission(
            ApplicationSpecification specification) {
        return new YarnApplicationConfiguration(specification, true);
    }

    /**
     * Resolves parameters needed by the localized ApplicationMaster and worker allocator.
     *
     * @param specification immutable localized application specification
     * @return YARN configuration that does not require the submitter-local distribution path
     */
    public static YarnApplicationConfiguration forApplicationMaster(
            ApplicationSpecification specification) {
        return new YarnApplicationConfiguration(specification, false);
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
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
