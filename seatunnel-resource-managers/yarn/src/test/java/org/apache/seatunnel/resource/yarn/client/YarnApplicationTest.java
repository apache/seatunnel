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

package org.apache.seatunnel.resource.yarn.client;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.yarn.YarnApplicationMaster;
import org.apache.seatunnel.resource.yarn.cluster.YarnContainerLaunch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YarnApplicationTest {
    @TempDir File temporary;

    @Test
    void localStagingIsRejectedForRemoteApplications() throws Exception {
        YarnClient client = client();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationDeployer(
                                                localConfiguration(), () -> client, false)
                                        .deploy(specification()));
        assertTrue(failure.getMessage().contains("shared filesystem"));
        verify(client, never()).submitApplication(any());
        verify(client).stop();
    }

    @Test
    void queueStartupTimeoutKillsApplicationAndCleansArtifacts() throws Exception {
        YarnClient client = client();
        ApplicationReport report = Records.newRecord(ApplicationReport.class);
        report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
        when(client.getApplicationReport(any())).thenReturn(report);
        ApplicationSpecification original = specification();
        Map<String, String> options = new HashMap<>(original.getOptions());
        options.put("application.startup-timeout-millis", "1");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.YARN, "env {}", options);
        assertThrows(
                TimeoutException.class,
                () ->
                        new YarnApplicationDeployer(localConfiguration(), () -> client, true)
                                .deploy(specification));
        verify(client).killApplication(ApplicationId.newInstance(1, 1));
        assertFalse(Files.exists(temporary.toPath().resolve("application_1_0001")));
    }

    @Test
    void missingDistributionHasActionableValidationMessage() {
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(
                        DeployType.YARN, "env {}", Collections.emptyMap());
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationDeployer(localConfiguration())
                                        .deploy(specification));
        assertEquals("Required option yarn.distribution is missing", failure.getMessage());
    }

    @Test
    void terminalStatusUsesTheFinalJobResult() {
        ApplicationReport report = Records.newRecord(ApplicationReport.class);
        report.setYarnApplicationState(YarnApplicationState.FINISHED);
        report.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
        assertEquals(ApplicationStatus.FAILED, YarnApplicationClient.status(report));
        report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        assertEquals(ApplicationStatus.SUCCEEDED, YarnApplicationClient.status(report));
        report.setYarnApplicationState(YarnApplicationState.KILLED);
        assertEquals(ApplicationStatus.CANCELED, YarnApplicationClient.status(report));
        report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
        assertEquals(ApplicationStatus.DEPLOYING, YarnApplicationClient.status(report));
    }

    @Test
    void submitStagesPrivateArtifactsAndDisablesRetries() throws Exception {
        Configuration configuration = localConfiguration();
        YarnClient client = client();
        ApplicationSpecification specification = specification();
        try (ApplicationClient deployed =
                new YarnApplicationDeployer(configuration, () -> client, true)
                        .deploy(specification)) {
            ArgumentCaptor<ApplicationSubmissionContext> context =
                    ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
            verify(client).submitApplication(context.capture());
            assertEquals(1, context.getValue().getMaxAppAttempts());
            assertEquals("test-application", context.getValue().getApplicationName());
            ContainerLaunchContext launch = context.getValue().getAMContainerSpec();
            assertEquals(3, launch.getLocalResources().size());
            assertTrue(launch.getCommands().get(0).contains("-Dseatunnel.home=\"{{PWD}}\"/"));
            assertEquals("{{PWD}}", launch.getEnvironment().get("HADOOP_CONF_DIR"));
            assertTrue(
                    launch.getCommands()
                            .get(0)
                            .contains("seatunnel/apache-seatunnel/starter/seatunnel-starter.jar:"));
            assertFalse(launch.getCommands().get(0).contains("starter/*"));
            assertTrue(launch.getCommands().get(0).contains("starter/logging/*"));
            assertTrue(launch.getCommands().get(0).contains(YarnApplicationMaster.class.getName()));
            Path staging = new Path(launch.getEnvironment().get(YarnContainerLaunch.STAGING_ENV));
            try (FileSystem fileSystem = FileSystem.newInstance(configuration)) {
                assertEquals(
                        (short) 0700, fileSystem.getFileStatus(staging).getPermission().toShort());
                assertTrue(fileSystem.exists(new Path(staging, YarnContainerLaunch.SPECIFICATION)));
            }
            deployed.cancel();
            assertFalse(Files.exists(Paths.get(staging.toUri())));
        }
        verify(client).killApplication(ApplicationId.newInstance(1, 1));
    }

    @Test
    void lostSubmissionResponseKillsPotentiallyAcceptedApplicationAndCleansStaging()
            throws Exception {
        YarnClient client = client();
        when(client.submitApplication(any())).thenThrow(new IOException("lost response"));
        assertThrows(
                IOException.class,
                () ->
                        new YarnApplicationDeployer(localConfiguration(), () -> client, true)
                                .deploy(specification()));
        verify(client).killApplication(ApplicationId.newInstance(1, 1));
        assertFalse(Files.exists(temporary.toPath().resolve("application_1_0001")));
    }

    @Test
    void conflictingStagingDirectoryIsNeverDeleted() throws Exception {
        File existing =
                Files.createDirectories(temporary.toPath().resolve("application_1_0001")).toFile();
        Files.write(existing.toPath().resolve("preserve"), new byte[] {1});
        YarnClient client = client();
        assertThrows(
                IllegalStateException.class,
                () ->
                        new YarnApplicationDeployer(localConfiguration(), () -> client, true)
                                .deploy(specification()));
        assertTrue(Files.exists(existing.toPath().resolve("preserve")));
        verify(client, never()).killApplication(any());
    }

    @Test
    void detachedClientCloseDoesNotCancelOrDeleteArtifacts() throws Exception {
        File staging = Files.createDirectories(temporary.toPath().resolve("detached")).toFile();
        YarnClient client = mock(YarnClient.class);
        new YarnApplicationClient(
                        client,
                        localConfiguration(),
                        ApplicationId.newInstance(1, 1).toString(),
                        new Path(staging.toURI()))
                .close();
        verify(client).stop();
        verify(client, never()).killApplication(any());
        assertTrue(Files.exists(staging.toPath()));
    }

    @Test
    void statusCleansArtifactsAfterApplicationMasterFailsBeforeStarting() throws Exception {
        File staging = Files.createDirectories(temporary.toPath().resolve("failed")).toFile();
        YarnClient client = mock(YarnClient.class);
        ApplicationReport report = Records.newRecord(ApplicationReport.class);
        report.setYarnApplicationState(YarnApplicationState.FAILED);
        when(client.getApplicationReport(any())).thenReturn(report);
        try (ApplicationClient deployed =
                new YarnApplicationClient(
                        client,
                        localConfiguration(),
                        ApplicationId.newInstance(1, 1).toString(),
                        new Path(staging.toURI()))) {
            assertEquals(ApplicationStatus.FAILED, deployed.getStatus());
            assertFalse(Files.exists(staging.toPath()));
        }
    }

    @Test
    void kerberosFailsBeforeAnyApplicationIsSubmitted() {
        Configuration configuration = localConfiguration();
        configuration.set("hadoop.security.authentication", "kerberos");
        assertThrows(
                IllegalArgumentException.class, () -> new YarnApplicationDeployer(configuration));
    }

    private Configuration localConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        return configuration;
    }

    private ApplicationSpecification specification() throws Exception {
        File archive = new File(temporary, "distribution.zip");
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive.toPath()))) {
            zip.putNextEntry(new ZipEntry("apache-seatunnel/starter/seatunnel-starter.jar"));
            zip.write(new byte[] {1, 2, 3});
            zip.closeEntry();
        }
        Map<String, String> options = new HashMap<>();
        options.put("yarn.distribution", archive.toString());
        options.put("yarn.staging-dir", temporary.toURI().toString());
        return new ApplicationSpecification(
                DeployType.YARN,
                "test-application",
                "env {}",
                1,
                new WorkerSpecification(512, 1, 2),
                options);
    }

    private YarnClient client() throws Exception {
        YarnClient client = mock(YarnClient.class);
        ApplicationSubmissionContext context =
                Records.newRecord(ApplicationSubmissionContext.class);
        context.setApplicationId(ApplicationId.newInstance(1, 1));
        GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
        response.setMaximumResourceCapability(Resource.newInstance(4096, 4));
        when(client.createApplication()).thenReturn(new YarnClientApplication(response, context));
        ApplicationReport report = Records.newRecord(ApplicationReport.class);
        report.setYarnApplicationState(YarnApplicationState.RUNNING);
        when(client.getApplicationReport(any())).thenReturn(report);
        return client;
    }
}
