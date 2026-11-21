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

package org.apache.seatunnel.resource.yarn.launch.factory;

import org.apache.seatunnel.engine.common.config.EngineConfig.ClusterRole;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnApplicationCli;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnContainerSettings;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.decorators.ApplicationSpecificationDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.CredentialsDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.DistributionDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.JvmOptionsDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.LoggingDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.RuntimeConfigDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.YarnStepDecorator;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResourceRequirements;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Collections;
import java.util.List;

/** Builds the AM launch context and the application submission sent to the ResourceManager. */
public final class ApplicationMasterContainerFactory {
    private static final String APPLICATION_TYPE = "SeaTunnel";
    private static final int SINGLE_APPLICATION_ATTEMPT = 1;

    private ApplicationMasterContainerFactory() {}

    /**
     * Defines the AM's staging and launch order. The application specification and selected job
     * file belong to this role.
     *
     * @param settings environment and runtime files for this application
     * @param hasJobFile whether the job is staged separately from the application specification
     * @return steps in the order used to declare files and decorate the AM
     */
    public static List<YarnStepDecorator<YarnLocalResource>> decorators(
            YarnContainerSettings settings, boolean hasJobFile) {
        return List.of(
                new DistributionDecorator(),
                new ApplicationSpecificationDecorator(hasJobFile),
                new EnvironmentDecorator(),
                new LoggingDecorator(),
                new RuntimeConfigDecorator(settings),
                new JvmOptionsDecorator(),
                new CredentialsDecorator());
    }

    /**
     * Builds the AM launch context after the submitter has staged the application files.
     *
     * @param configuration submitter's Hadoop configuration for acquiring staging credentials
     * @param staging qualified directory owned by this YARN application
     * @param requirements memory and virtual cores requested for the AM
     * @param localized registered files and runtime settings read from the staged manifest
     * @param user Hadoop user exported to the container in simple authentication mode
     * @return AM spec with its command, environment and local resources ready for submission
     */
    public static YarnContainerSpec create(
            Configuration configuration,
            Path staging,
            YarnResourceRequirements requirements,
            YarnLocalResourceDescriptor localized,
            String user) {
        YarnLaunchParameters parameters =
                new YarnLaunchParameters(configuration, staging, user, localized);
        YarnContainerSpec master =
                new YarnContainerSpec(
                        ClusterRole.MASTER,
                        requirements,
                        parameters,
                        SeatunnelYarnApplicationCli.class.getName(),
                        Collections.emptyList());

        List<YarnStepDecorator<YarnLocalResource>> decorators =
                decorators(
                        localized.getSettings(),
                        localized
                                .getResources()
                                .containsKey(YarnConstants.LOCALIZED_JOB_CONFIG_NAME));

        return master.applyDecorators(decorators);
    }

    /**
     * Checks each role's request against the maximum reported when the RM allocates an application
     * ID.
     *
     * @throws IllegalArgumentException if either role exceeds the memory or virtual-core limit
     */
    public static void validateResources(ApplicationSpecification specification, Resource maximum) {
        YarnResourceRequirements master = YarnResourceRequirements.forMaster(specification);
        YarnResourceRequirements worker =
                YarnResourceRequirements.forWorker(specification.getWorkerSpecification());
        if (master.exceeds(maximum) || worker.exceeds(maximum)) {
            throw new IllegalArgumentException(
                    "Requested master/worker resources exceed YARN maximum container capability");
        }
    }

    /**
     * Populates the submission context allocated by the RM, including the single AM attempt.
     * Optional scheduling settings are applied when supplied by the submitter.
     *
     * @param submission RM-allocated context with its application ID
     * @param deployment validated queue, placement and lifetime settings
     * @param master AM spec with its local resources and credentials attached
     * @return the supplied submission context, ready for the YARN client
     */
    public static ApplicationSubmissionContext createSubmission(
            ApplicationSubmissionContext submission,
            YarnApplicationConfiguration deployment,
            YarnContainerSpec master) {
        ApplicationSpecification specification = deployment.getSpecification();
        submission.setApplicationName(specification.getName());
        submission.setApplicationType(APPLICATION_TYPE);
        submission.setQueue(deployment.getQueue());
        submission.setMaxAppAttempts(SINGLE_APPLICATION_ATTEMPT);
        submission.setResource(master.getRequirements().toResource());
        submission.setAMContainerSpec(master.getInternalResource());
        if (deployment.getActiveDeadlineSeconds() != null) {
            submission.setApplicationTimeouts(
                    Collections.singletonMap(
                            ApplicationTimeoutType.LIFETIME,
                            deployment.getActiveDeadlineSeconds()));
        }
        if (deployment.getPriority() >= 0) {
            submission.setPriority(Priority.newInstance(deployment.getPriority()));
        }
        if (!deployment.getTags().isEmpty()) {
            submission.setApplicationTags(deployment.getTags());
        }
        if (deployment.getMasterNodeLabel() != null) {
            submission.setNodeLabelExpression(deployment.getMasterNodeLabel());
        }
        return submission;
    }
}
