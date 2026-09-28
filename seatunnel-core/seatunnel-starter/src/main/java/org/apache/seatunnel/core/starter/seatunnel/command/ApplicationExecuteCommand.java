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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.ApplicationOperation;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ApplicationCommandArgs;
import org.apache.seatunnel.resource.core.ApplicationClusterDescriptor;
import org.apache.seatunnel.resource.core.ApplicationClusterDescriptors;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import java.nio.file.Paths;
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
            executeApplication();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandExecuteException("Application command was interrupted", e);
        } catch (Exception e) {
            throw new CommandExecuteException("Application command failed", e);
        }
    }

    private void executeApplication() throws Exception {
        ApplicationOperation operation = applicationCommandArgs.getOperation();
        Map<String, String> options =
                TypesafeConfigUtils.configToMap(
                        ConfigFactory.parseFile(
                                        Paths.get(applicationCommandArgs.getDeploymentConfig())
                                                .toFile())
                                .resolve());
        options.putAll(applicationCommandArgs.getOptions());
        if (applicationCommandArgs.getJobId() != null) {
            options.put(
                    ApplicationOptions.JOB_ID.key(), applicationCommandArgs.getJobId().toString());
        }
        if (applicationCommandArgs.getRestoreJobId() != null) {
            options.put(
                    ApplicationOptions.RESTORE_JOB_ID.key(),
                    applicationCommandArgs.getRestoreJobId().toString());
        }

        ApplicationSpecification specification = null;
        if (operation == ApplicationOperation.SUBMIT) {
            String jobConfig =
                    ConfigFactory.parseFile(Paths.get(applicationCommandArgs.getConfig()).toFile())
                            .resolve()
                            .root()
                            .render(ConfigRenderOptions.concise());
            specification =
                    ApplicationSpecification.fromOptions(
                            applicationCommandArgs.getDeployType(), jobConfig, options);
        }

        try (ApplicationClusterDescriptor descriptor =
                        ApplicationClusterDescriptors.create(
                                applicationCommandArgs.getDeployType(), options);
                ApplicationClient client =
                        operation == ApplicationOperation.SUBMIT
                                ? descriptor.deploy(specification)
                                : descriptor.retrieve(
                                        new ApplicationId(
                                                applicationCommandArgs.getDeployType(),
                                                applicationCommandArgs.getId()),
                                        options)) {
            System.out.println("Application ID: " + client.getApplicationId().getId());
            if (specification != null) {
                System.out.println("Job ID: " + specification.getJobId());
            }
            if (operation == ApplicationOperation.CANCEL) {
                client.cancel();
                System.out.println("Cancellation requested");
                return;
            }
            int result = printResult(client, applicationCommandArgs.isWait());
            if (result != 0) {
                throw new CommandExecuteException(
                        "Application finished with an unsuccessful status");
            }
        }
    }

    static int printResult(ApplicationClient client, boolean wait) throws Exception {
        ApplicationResult result = client.getResult();
        while (wait
                && !result.getStatus().isTerminal()
                && result.getStatus() != ApplicationStatus.UNKNOWN) {
            Thread.sleep(1000L);
            result = client.getResult();
        }
        System.out.println("Status: " + result.getStatus());
        if (!result.getDiagnostics().isEmpty()) {
            System.out.println(result.getDiagnostics());
        }
        return result.getStatus() == ApplicationStatus.FAILED
                        || result.getStatus() == ApplicationStatus.CANCELED
                        || result.getStatus() == ApplicationStatus.UNKNOWN
                ? 1
                : 0;
    }
}
