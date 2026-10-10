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

import org.apache.seatunnel.common.constants.ApplicationOperation;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.args.ApplicationCommandArgs;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDescriptorFactory;
import org.apache.seatunnel.engine.client.deployment.ClusterClientServiceLoader;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ApplicationExecuteCommandTest {
    @TempDir Path temporary;

    @Test
    void parsesCommandsWithShortLongAndEqualsOptions() throws Exception {
        Path application = temporary.resolve("application.config");
        Files.write(application, "application.worker-count = 2".getBytes(StandardCharsets.UTF_8));
        Path job = temporary.resolve("job config.conf");
        Files.write(job, "env.parallelism = 1".getBytes(StandardCharsets.UTF_8));
        for (String[] arguments :
                new String[][] {
                    {"submit", "-t", "yarn", "-c", job.toString(), "-a", application.toString()},
                    {
                        "submit",
                        "--target=YARN",
                        "--config=" + job,
                        "--application-config=" + application
                    }
                }) {
            ApplicationCommandArgs args = parse(arguments);
            args.buildCommand();
            assertEquals(ApplicationOperation.SUBMIT, args.getOperation());
            assertEquals(DeployType.YARN, args.getTarget());
            assertEquals(application.toString(), args.getApplicationConfig());
            assertEquals(job.toString(), args.getConfig());
        }
        ApplicationCommandArgs status =
                parse("status", "-t=kubernetes", "--id", "example", "-ikubernetes.namespace=test");
        status.buildCommand();
        assertEquals("test", status.getOptions().get("kubernetes.namespace"));
        assertEquals(DeployType.KUBERNETES, status.getTarget());
        assertEquals("a=b", parse("status", "-t", "yarn", "-ikey=a=b").getOptions().get("key"));
    }

    @Test
    void rejectsMissingCommandsTargetsAndRemovedOptionsButAllowsHelp() {
        assertThrows(ParameterException.class, () -> parse("--target", "yarn"));
        assertThrows(ParameterException.class, () -> parse("status", "--id", "app"));
        assertThrows(ParameterException.class, () -> parse("submit", "status", "-t", "yarn"));
        for (String option :
                new String[] {
                    "-p",
                    "--operation",
                    "-d",
                    "--deploy-type",
                    "-dc",
                    "--deployment-config",
                    "--restore-from-checkpoint",
                    "-Dkey=value"
                }) {
            assertThrows(
                    ParameterException.class, () -> parse("submit", "-t", "yarn", option, "value"));
        }
        assertTrue(parse("--help").isHelp());
        assertTrue(parse("submit", "--help").isHelp());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void submitsApplicationConfigAndOverridesBeforeResolving(boolean wait) throws Exception {
        Path application = temporary.resolve("application.config");
        Files.write(
                application,
                ("application { name = demo, worker-count = 2, job-id = 10, restore-job-id = 9 }\n"
                                + "application.worker.slots = ${application.worker-count}\n"
                                + "yarn.queue = ${queue}\n")
                        .getBytes(StandardCharsets.UTF_8));
        Path job = temporary.resolve("job.config");
        Files.write(job, "env.parallelism = 1".getBytes(StandardCharsets.UTF_8));
        ClusterDescriptor<String> descriptor = mock(ClusterDescriptor.class);
        ApplicationClusterDescriptorFactory<String> factory =
                mock(ApplicationClusterDescriptorFactory.class);
        when(factory.create(any())).thenReturn(descriptor);
        when(descriptor.deployApplication(any())).thenReturn("application_1");
        when(factory.parseApplicationId("application_1")).thenReturn("native-id");
        when(descriptor.getApplicationStatus("native-id")).thenReturn(ApplicationStatus.SUCCEEDED);
        try (MockedConstruction<ClusterClientServiceLoader> loaders =
                mockConstruction(
                        ClusterClientServiceLoader.class,
                        (loader, context) ->
                                when(loader.<String>getClusterClientFactory(DeployType.YARN))
                                        .thenReturn(factory))) {
            ApplicationCommandArgs args =
                    parse(
                            "submit",
                            "-t",
                            "yarn",
                            "-c",
                            job.toString(),
                            "-a",
                            application.toString(),
                            "-iapplication.worker-count=3",
                            "-iapplication.job-id=20",
                            "-iqueue=analytics",
                            "--job-id",
                            "30",
                            "--restore-job-id",
                            "8");
            args.setWait(wait);
            args.buildCommand().execute();
        }
        ArgumentCaptor<ApplicationSpecification> specification =
                ArgumentCaptor.forClass(ApplicationSpecification.class);
        verify(descriptor).deployApplication(specification.capture());
        assertEquals("demo", specification.getValue().getName());
        assertEquals(3, specification.getValue().getWorkerCount());
        assertEquals(3, specification.getValue().getWorkerSpecification().getSlots());
        assertEquals(30L, specification.getValue().getJobId());
        assertEquals(Long.valueOf(8), specification.getValue().getRestoreJobId());
        ArgumentCaptor<Map<String, String>> deploymentOptions = ArgumentCaptor.forClass(Map.class);
        verify(factory, times(wait ? 2 : 1)).create(deploymentOptions.capture());
        assertEquals("analytics", deploymentOptions.getValue().get("yarn.queue"));
        assertEquals(
                1,
                ConfigFactory.parseString(specification.getValue().getJobConfig())
                        .getInt("env.parallelism"));
        verify(descriptor, times(wait ? 1 : 0)).getApplicationStatus("native-id");
        verify(descriptor, times(wait ? 2 : 1)).close();
        verify(descriptor, never()).retrieve(any());
    }

    @Test
    void rejectsMissingOrMalformedFilesBeforeOpeningPlatformConnections() throws Exception {
        Path application = temporary.resolve("application.config");
        ApplicationCommandArgs missing =
                parse("status", "-t", "yarn", "--id", "app", "-a", application.toString());
        assertTrue(
                assertThrows(IllegalArgumentException.class, missing::buildCommand)
                        .getMessage()
                        .contains("--application-config"));
        Files.write(application, "application { invalid".getBytes(StandardCharsets.UTF_8));
        try (MockedConstruction<ClusterClientServiceLoader> loaders =
                mockConstruction(ClusterClientServiceLoader.class)) {
            assertThrows(CommandExecuteException.class, () -> missing.buildCommand().execute());
            assertTrue(loaders.constructed().isEmpty());
        }
    }

    @Test
    void serviceLoaderRejectsMissingOrAmbiguousProvidersWithoutOpeningConnections()
            throws Exception {
        ApplicationClusterDescriptorFactory<?> yarn =
                mock(ApplicationClusterDescriptorFactory.class);
        ApplicationClusterDescriptorFactory<?> duplicate =
                mock(ApplicationClusterDescriptorFactory.class);
        when(yarn.getDeployType()).thenReturn(DeployType.YARN);
        when(duplicate.getDeployType()).thenReturn(DeployType.YARN);
        ServiceLoader<ApplicationClusterDescriptorFactory> providers = mock(ServiceLoader.class);
        when(providers.iterator())
                .thenAnswer(ignored -> Collections.singletonList(yarn).iterator());
        try (MockedStatic<ServiceLoader> spi = mockStatic(ServiceLoader.class)) {
            spi.when(() -> ServiceLoader.load(ApplicationClusterDescriptorFactory.class))
                    .thenReturn(providers);
            ClusterClientServiceLoader loader = new ClusterClientServiceLoader();
            assertSame(yarn, loader.getClusterClientFactory(DeployType.YARN));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> loader.getClusterClientFactory(DeployType.KUBERNETES));
            when(providers.iterator())
                    .thenAnswer(ignored -> Arrays.asList(yarn, duplicate).iterator());
            assertThrows(
                    IllegalStateException.class,
                    () -> loader.getClusterClientFactory(DeployType.YARN));
            verify(yarn, never()).create(any());
            verify(duplicate, never()).create(any());
        }
    }

    @Test
    void deployerUsesInjectedLoaderAndClosesDescriptorOnSuccessAndFailure() throws Exception {
        ClusterClientServiceLoader loader = mock(ClusterClientServiceLoader.class);
        ApplicationClusterDescriptorFactory<String> factory =
                mock(ApplicationClusterDescriptorFactory.class);
        ClusterDescriptor<String> descriptor = mock(ClusterDescriptor.class);
        ApplicationSpecification specification = mock(ApplicationSpecification.class);
        when(loader.<String>getClusterClientFactory(DeployType.YARN)).thenReturn(factory);
        when(factory.create(Collections.emptyMap())).thenReturn(descriptor);
        when(descriptor.deployApplication(specification)).thenReturn("application_1");
        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(
                        loader, DeployType.YARN, specification, Collections.emptyMap());
        assertEquals("application_1", deployer.<String>run());
        verify(descriptor).close();

        Exception failure = new Exception("submission rejected");
        Exception closeFailure = new Exception("close failed");
        when(descriptor.deployApplication(specification)).thenThrow(failure);
        doThrow(closeFailure).when(descriptor).close();
        assertSame(failure, assertThrows(Exception.class, deployer::run));
        assertSame(closeFailure, failure.getSuppressed()[0]);
        verify(descriptor, times(2)).close();
        verify(descriptor, never()).cancelApplication(any());
        verify(descriptor, never()).retrieve(any());
    }

    @Test
    void reportsApplicationStateWithoutConnectingToMaster() throws Exception {
        ClusterDescriptor<String> descriptor = mock(ClusterDescriptor.class);
        when(descriptor.getApplicationStatus("app")).thenReturn(ApplicationStatus.UNKNOWN);
        assertEquals(
                ApplicationStatus.UNKNOWN,
                ApplicationExecuteCommand.getStatus(descriptor, "app", false));
        assertEquals(
                ApplicationStatus.UNKNOWN,
                ApplicationExecuteCommand.getStatus(descriptor, "app", true));
        verify(descriptor, never()).retrieve(any());
    }

    @Test
    void waitsForPlatformTerminalState() throws Exception {
        ClusterDescriptor<String> descriptor = mock(ClusterDescriptor.class);
        when(descriptor.getApplicationStatus("app"))
                .thenReturn(ApplicationStatus.RUNNING, ApplicationStatus.SUCCEEDED);
        assertEquals(
                ApplicationStatus.SUCCEEDED,
                ApplicationExecuteCommand.getStatus(descriptor, "app", true));
        verify(descriptor, times(2)).getApplicationStatus("app");
        verify(descriptor, never()).retrieve(any());
    }

    @Test
    void statusAndCancelUseApplicationIdWithoutJobId() throws Exception {
        ClusterDescriptor<String> descriptor = mock(ClusterDescriptor.class);
        ApplicationClusterDescriptorFactory<String> factory =
                mock(ApplicationClusterDescriptorFactory.class);
        when(factory.parseApplicationId("application_1")).thenReturn("native-id");
        org.mockito.Mockito.doReturn(descriptor).when(factory).create(any());
        when(descriptor.getApplicationStatus("native-id")).thenReturn(ApplicationStatus.SUCCEEDED);
        try (MockedConstruction<ClusterClientServiceLoader> loaders =
                mockConstruction(
                        ClusterClientServiceLoader.class,
                        (loader, context) ->
                                when(loader.<String>getClusterClientFactory(DeployType.YARN))
                                        .thenReturn(factory))) {
            for (ApplicationOperation operation :
                    new ApplicationOperation[] {
                        ApplicationOperation.STATUS, ApplicationOperation.CANCEL
                    }) {
                ApplicationCommandArgs args =
                        parse(operation.getOperation(), "-t", "yarn", "--id", "application_1");
                args.buildCommand().execute();
            }
        }
        verify(descriptor).getApplicationStatus("native-id");
        verify(descriptor).cancelApplication("native-id");
        verify(descriptor, never()).retrieve(any());
        verify(descriptor, times(2)).close();
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
        assertTrue(status.buildCommand() instanceof ApplicationExecuteCommand);
        status.setJobId(12L);
        assertThrows(IllegalArgumentException.class, status::buildCommand);
        status.setJobId(null);
        status.setRestoreJobId(100L);
        assertThrows(IllegalArgumentException.class, status::buildCommand);

        assertThrows(
                ParameterException.class,
                () -> new ApplicationCommandArgs.DeployTypeConverter().convert("standalone"));
        assertThrows(
                ParameterException.class,
                () -> new ApplicationCommandArgs.ApplicationOperationConverter().convert("delete"));

        StringBuilder usage = new StringBuilder();
        JCommander.newBuilder()
                .programName("seatunnel-application.sh")
                .addObject(new ApplicationCommandArgs())
                .build()
                .getUsageFormatter()
                .usage(usage);
        assertTrue(usage.toString().contains("--application-config"));
        assertTrue(usage.toString().contains("--restore-job-id"));
        assertTrue(usage.toString().contains("submit | status | cancel"));
        assertFalse(usage.toString().contains("--operation"));
        assertFalse(usage.toString().contains("--deployment-config"));
    }

    private ApplicationCommandArgs parse(String... arguments) {
        ApplicationCommandArgs args = new ApplicationCommandArgs();
        JCommander.newBuilder().addObject(args).build().parse(arguments);
        return args;
    }

    private ApplicationCommandArgs command(
            ApplicationOperation operation, Path deploymentConfiguration) {
        ApplicationCommandArgs arguments = new ApplicationCommandArgs();
        arguments.setOperation(operation);
        arguments.setTarget(DeployType.YARN);
        arguments.setApplicationConfig(deploymentConfiguration.toString());
        return arguments;
    }
}
