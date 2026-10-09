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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.common.constants.ApplicationOperation;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ApplicationCommandArgs;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDescriptorFactory;
import org.apache.seatunnel.engine.client.deployment.ClusterClientServiceLoader;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Executes application submit, status, and cancellation operations against an external platform.
 */
public class ApplicationExecuteCommand implements Command<ApplicationCommandArgs> {

    private final ApplicationCommandArgs applicationCommandArgs;

    public ApplicationExecuteCommand(ApplicationCommandArgs applicationCommandArgs) {
        this.applicationCommandArgs = applicationCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        try {
            Map<String, String> options = resolveOptions();
            ClusterClientServiceLoader clientServiceLoader = new ClusterClientServiceLoader();
            String applicationId = applicationCommandArgs.getId();

            if (applicationCommandArgs.getOperation() == ApplicationOperation.SUBMIT) {
                applicationId = submit(options);
                if (!applicationCommandArgs.isWait()) {
                    return;
                }
            } else {
                System.out.println("Application ID: " + applicationId);
            }

            executePlatformOperation(clientServiceLoader, options, applicationId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandExecuteException("Application command was interrupted", e);
        } catch (Exception e) {
            throw new CommandExecuteException("Application command failed", e);
        }
    }

    private <ID> void executePlatformOperation(
            ClusterClientServiceLoader clientServiceLoader, Map<String, String> options, String id)
            throws Exception {
        ApplicationClusterDescriptorFactory<ID> factory =
                clientServiceLoader.getClusterClientFactory(applicationCommandArgs.getTarget());
        ID applicationId = factory.parseApplicationId(id);
        try (ClusterDescriptor<ID> descriptor = factory.create(options)) {
            if (applicationCommandArgs.getOperation() == ApplicationOperation.CANCEL) {
                descriptor.cancelApplication(applicationId);
                System.out.println("Cancellation requested");
                return;
            }
            if (printResult(descriptor, applicationId, applicationCommandArgs.isWait()) != 0) {
                throw new CommandExecuteException(
                        "Application finished with an unsuccessful status");
            }
        }
    }

    private Map<String, String> resolveOptions() {
        // Named job-ID flags take precedence over -i. Shared loading applies these values over
        // the application file before resolving substitutions; it has no dependency on CLI args.
        Map<String, String> overrides = new LinkedHashMap<>(applicationCommandArgs.getOptions());
        if (applicationCommandArgs.getJobId() != null) {
            overrides.put(
                    ApplicationOptions.JOB_ID.key(), applicationCommandArgs.getJobId().toString());
        }
        if (applicationCommandArgs.getRestoreJobId() != null) {
            overrides.put(
                    ApplicationOptions.RESTORE_JOB_ID.key(),
                    applicationCommandArgs.getRestoreJobId().toString());
        }
        String applicationConfig = applicationCommandArgs.getApplicationConfig();
        return SeatunnelApplicationConfig.load(
                applicationConfig == null ? null : Paths.get(applicationConfig), overrides);
    }

    private String submit(Map<String, String> options) throws Exception {
        // Only submit reads a job file. Status/cancel need platform connection settings only.
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse(
                        Paths.get(applicationCommandArgs.getConfig()), options);
        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(
                        applicationCommandArgs.getTarget(), specification, options);
        String applicationId = deployer.run().toString();
        System.out.println("Application ID: " + applicationId);
        System.out.println("Job ID: " + specification.getJobId());
        return applicationId;
    }

    static <ID> int printResult(ClusterDescriptor<ID> descriptor, ID applicationId, boolean wait)
            throws Exception {
        ApplicationStatus status = descriptor.getApplicationStatus(applicationId);
        while (wait && !status.isTerminal() && status != ApplicationStatus.UNKNOWN) {
            Thread.sleep(500L);
            status = descriptor.getApplicationStatus(applicationId);
        }
        System.out.println("Status: " + status);
        return status.isFailure() ? 1 : 0;
    }
}
