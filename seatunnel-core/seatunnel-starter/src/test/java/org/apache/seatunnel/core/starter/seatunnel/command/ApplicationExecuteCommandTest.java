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
import org.apache.seatunnel.core.starter.seatunnel.args.ApplicationCommandArgs;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.beust.jcommander.JCommander;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApplicationExecuteCommandTest {
    @TempDir Path temporary;

    @Test
    void stopsWaitingWhenApplicationNoLongerExists() throws Exception {
        ApplicationClient client = mock(ApplicationClient.class);
        when(client.getResult())
                .thenReturn(
                        new ApplicationResult(
                                new ApplicationId(DeployType.KUBERNETES, "expired-job"),
                                ApplicationStatus.UNKNOWN,
                                "Job no longer exists"));
        assertEquals(1, ApplicationExecuteCommand.printResult(client, true));
        verify(client, times(1)).getResult();
    }

    @Test
    void validatesCommandBeforeLoadingAnyPlatformProvider() throws Exception {
        Path deploymentConfig = temporary.resolve("deployment.conf");
        Files.write(deploymentConfig, "yarn.queue = default".getBytes(StandardCharsets.UTF_8));

        ApplicationCommandArgs cancel = command(ApplicationOperation.CANCEL, deploymentConfig);
        assertThrows(IllegalArgumentException.class, cancel::buildCommand);

        ApplicationCommandArgs submit = command(ApplicationOperation.SUBMIT, deploymentConfig);
        assertThrows(IllegalArgumentException.class, submit::buildCommand);

        ApplicationCommandArgs status = command(ApplicationOperation.STATUS, deploymentConfig);
        status.setId("application_1");
        status.setRestoreJobId(100L);
        assertThrows(IllegalArgumentException.class, status::buildCommand);

        assertThrows(
                IllegalArgumentException.class,
                () -> new ApplicationCommandArgs.DeployTypeConverter().convert("standalone"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ApplicationCommandArgs.ApplicationOperationConverter().convert("delete"));

        StringBuilder usage = new StringBuilder();
        JCommander.newBuilder()
                .programName("seatunnel-application.sh")
                .addObject(new ApplicationCommandArgs())
                .build()
                .getUsageFormatter()
                .usage(usage);
        assertTrue(usage.toString().contains("--deployment-config"));
        assertTrue(usage.toString().contains("--restore-job-id"));
    }

    private ApplicationCommandArgs command(
            ApplicationOperation operation, Path deploymentConfiguration) {
        ApplicationCommandArgs arguments = new ApplicationCommandArgs();
        arguments.setOperation(operation);
        arguments.setDeployType(DeployType.YARN);
        arguments.setDeploymentConfig(deploymentConfiguration.toString());
        return arguments;
    }
}
