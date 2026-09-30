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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.deployment.SeatunnelClientProvider;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnMasterCli;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnWorkerCli;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationClient;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnApplicationFileUploader;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.hazelcast.client.config.ClientConfig;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YarnApplicationTest {
    @TempDir File temporary;
    private MockedConstruction<SeaTunnelClient> nativeClients;
    private final List<ClientConfig> clientConfigs = new ArrayList<>();

    @BeforeEach
    void mockNativeConnections() {
        nativeClients =
                mockConstruction(
                        SeaTunnelClient.class,
                        (client, context) ->
                                clientConfigs.add((ClientConfig) context.arguments().get(0)));
    }

    @AfterEach
    void closeNativeConnections() {
        nativeClients.close();
    }

    @Test
    void startsLocalizedWorkerWithoutOwningApplicationCleanup() throws Exception {
        SeaTunnelConfig config = new SeaTunnelConfig();
        java.nio.file.Path masterHome = temporary.toPath().resolve("master");
        java.nio.file.Path workerHome =
                Files.createDirectories(temporary.toPath().resolve("worker"));
        java.nio.file.Path localJar =
                Files.write(workerHome.resolve("connector.jar"), new byte[] {1});
        String previousHome = System.getProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY);
        System.setProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY, workerHome.toString());
        try (MockedStatic<ConfigProvider> configurations = mockStatic(ConfigProvider.class);
                MockedStatic<SeaTunnelServerStarter> starter =
                        mockStatic(SeaTunnelServerStarter.class);
                MockedStatic<YarnStagingDirectory> staging =
                        mockStatic(YarnStagingDirectory.class)) {
            configurations.when(ConfigProvider::locateAndGetSeaTunnelConfig).thenReturn(config);
            SeatunnelYarnWorkerCli.main(
                    new String[] {"yarn-app", "master:5801", "3", masterHome.toString()});
            ArgumentCaptor<JarPathResolver> resolver =
                    ArgumentCaptor.forClass(JarPathResolver.class);
            starter.verify(
                    () ->
                            SeaTunnelServerStarter.createHazelcastInstance(
                                    eq(config),
                                    isNull(),
                                    resolver.capture(),
                                    any(ResourceManagerFactory.class)));
            starter.verifyNoMoreInteractions();
            staging.verifyNoInteractions();
            assertEquals("yarn-app", config.getHazelcastConfig().getClusterName());
            assertEquals(
                    EngineConfig.ClusterRole.WORKER, config.getEngineConfig().getClusterRole());
            assertTrue(config.getHazelcastConfig().isLiteMember());
            assertEquals(
                    "master:5801",
                    config.getHazelcastConfig()
                            .getNetworkConfig()
                            .getJoin()
                            .getTcpIpConfig()
                            .getRequiredMember());
            assertTrue(config.getHazelcastConfig().getNetworkConfig().isPortAutoIncrement());
            assertEquals(3, config.getEngineConfig().getSlotServiceConfig().getSlotNum());
            assertFalse(config.getEngineConfig().getSlotServiceConfig().isDynamicSlot());
            assertEquals(
                    "true",
                    config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.enabled"));
            assertEquals(
                    "GRACEFUL",
                    config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.policy"));
            List<URL> original =
                    Collections.singletonList(masterHome.resolve("connector.jar").toUri().toURL());
            assertEquals(
                    Collections.singletonList(localJar.toUri().toURL()),
                    resolver.getValue().resolve(original));
        } finally {
            if (previousHome == null) {
                System.clearProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY);
            } else {
                System.setProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY, previousHome);
            }
        }
    }

    @Test
    void rejectsIncompleteWorkerArgumentsBeforeStartingResources() {
        try (MockedStatic<SeaTunnelServerStarter> starter =
                mockStatic(SeaTunnelServerStarter.class)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> SeatunnelYarnWorkerCli.main(new String[] {"yarn-app"}));
            starter.verifyNoInteractions();
        }
    }

    @Test
    void retrievesLazyProviderWithIndependentlyOwnedClients() throws Exception {
        YarnClient client = client();
        ApplicationId id = ApplicationId.newInstance(1, 1);
        Map<String, String> options =
                Collections.singletonMap(
                        YarnOptions.STAGING_DIRECTORY.key(), temporary.toURI().toString());
        SeatunnelClientProvider provider;
        SeaTunnelClient first;
        try (YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options)) {
            provider = descriptor.retrieve(id);
            assertTrue(nativeClients.constructed().isEmpty());
            first = provider.getClusterClient();
        }
        verify(first, never()).close();
        try (SeaTunnelClient application = first;
                SeaTunnelClient second = provider.getClusterClient()) {
            assertSame(application, nativeClients.constructed().get(0));
            assertSame(second, nativeClients.constructed().get(1));
            assertNotSame(application, second);
            assertEquals("seatunnel-application-" + id, clientConfigs.get(0).getClusterName());
            assertEquals(
                    Collections.singletonList("master:5801"),
                    clientConfigs.get(0).getNetworkConfig().getAddresses());
        }
        for (SeaTunnelClient application : nativeClients.constructed()) {
            verify(application).close();
        }
        verify(client, never()).createApplication();
        verify(client, never()).submitApplication(any());
        verify(client, never()).killApplication(any());
        verify(client).stop();
    }

    @Test
    void rejectsInvalidApplicationIdBeforeOpeningYarnClient() throws Exception {
        YarnClient client = mock(YarnClient.class);
        try (YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(localConfiguration(), () -> client, true)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            new YarnApplicationClusterDescriptorFactory()
                                    .parseApplicationId("not-a-yarn-application"));
        }
        verify(client, never()).init(any());
        verify(client, never()).start();
    }

    @Test
    void applicationOperationsDoNotNeedALiveMaster() throws Exception {
        YarnClient client = client();
        ApplicationId id = ApplicationId.newInstance(1, 1);
        ApplicationReport report = client.getApplicationReport(id);
        report.setYarnApplicationState(YarnApplicationState.FINISHED);
        report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        report.setHost(null);
        report.setRpcPort(-1);
        java.nio.file.Path staging =
                Files.createDirectories(temporary.toPath().resolve(id.toString()));
        Map<String, String> options =
                Collections.singletonMap(
                        YarnOptions.STAGING_DIRECTORY.key(), temporary.toURI().toString());
        try (YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options)) {
            assertEquals(ApplicationStatus.SUCCEEDED, descriptor.getApplicationStatus(id));
            assertFalse(Files.exists(staging));
            assertThrows(IllegalStateException.class, () -> descriptor.retrieve(id));
            descriptor.cancelApplication(id);
            verify(client).killApplication(id);
            assertTrue(nativeClients.constructed().isEmpty());
        }
        verify(client).stop();
    }

    @Test
    void deploymentReturnsIdWhenApplicationFinishesBeforeConnecting() throws Exception {
        YarnClient client = client();
        ApplicationReport report = client.getApplicationReport(ApplicationId.newInstance(1, 1));
        report.setYarnApplicationState(YarnApplicationState.FINISHED);
        report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        report.setHost(null);
        try (YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(localConfiguration(), () -> client, true)) {
            assertEquals(
                    ApplicationId.newInstance(1, 1), descriptor.deployApplication(specification()));
            assertTrue(nativeClients.constructed().isEmpty());
        }
        verify(client, never()).killApplication(any());
    }

    @Test
    void localStagingIsRejectedForRemoteApplications() throws Exception {
        YarnClient client = client();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationClusterDescriptor(
                                                localConfiguration(), () -> client, false)
                                        .deployApplication(specification()));
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
                        new YarnApplicationClusterDescriptor(
                                        localConfiguration(), () -> client, true)
                                .deployApplication(specification));
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
                                new YarnApplicationClusterDescriptor(localConfiguration())
                                        .deployApplication(specification));
        assertEquals("Required option yarn.distribution is missing", failure.getMessage());
    }

    @Test
    void terminalStatusUsesTheFinalJobResult() throws Exception {
        YarnClient client = mock(YarnClient.class);
        ApplicationReport report = Records.newRecord(ApplicationReport.class);
        when(client.getApplicationReport(any())).thenReturn(report);
        YarnApplicationClient application =
                new YarnApplicationClient(
                        client,
                        localConfiguration(),
                        ApplicationId.newInstance(1, 1),
                        new Path(new Path(temporary.toURI()), "terminal"));
        report.setYarnApplicationState(YarnApplicationState.FINISHED);
        report.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
        assertEquals(ApplicationStatus.FAILED, application.getStatus());
        report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        assertEquals(ApplicationStatus.SUCCEEDED, application.getStatus());
        report.setYarnApplicationState(YarnApplicationState.KILLED);
        assertEquals(ApplicationStatus.CANCELED, application.getStatus());
        report.setYarnApplicationState(YarnApplicationState.ACCEPTED);
        assertEquals(ApplicationStatus.DEPLOYING, application.getStatus());
    }

    @Test
    void submitStagesPrivateArtifactsAndDisablesRetries() throws Exception {
        Configuration configuration = localConfiguration();
        configuration.set("seatunnel.test.hadoop-option", "localized-value");
        YarnClient client = client();
        ApplicationSpecification specification = specification();
        try (YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        configuration, () -> client, true, specification.getOptions())) {
            ApplicationId deployed = descriptor.deployApplication(specification);
            assertTrue(nativeClients.constructed().isEmpty());
            ArgumentCaptor<ApplicationSubmissionContext> context =
                    ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
            verify(client).submitApplication(context.capture());
            assertEquals(context.getValue().getApplicationId(), deployed);
            assertEquals(1, context.getValue().getMaxAppAttempts());
            assertEquals("test-application", context.getValue().getApplicationName());
            assertEquals(3, context.getValue().getPriority().getPriority());
            assertEquals(
                    new HashSet<>(Arrays.asList("batch", "finance")),
                    context.getValue().getApplicationTags());
            assertEquals("master-pool", context.getValue().getNodeLabelExpression());
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
            assertTrue(
                    launch.getCommands().get(0).contains(SeatunnelYarnMasterCli.class.getName()));
            Path staging =
                    new Path(launch.getEnvironment().get(YarnConstants.STAGING_DIRECTORY_ENV));
            try (FileSystem fileSystem = FileSystem.newInstance(configuration)) {
                assertEquals(
                        (short) 0700, fileSystem.getFileStatus(staging).getPermission().toShort());
                assertTrue(
                        fileSystem.exists(
                                new Path(staging, YarnConstants.LOCALIZED_SPECIFICATION_NAME)));
            }
            java.nio.file.Path stagedFiles = Paths.get(staging.toUri());
            ApplicationSpecification localized =
                    ApplicationSpecification.read(
                            stagedFiles.resolve(YarnConstants.LOCALIZED_SPECIFICATION_NAME));
            assertEquals(specification.getJobConfig(), localized.getJobConfig());
            assertEquals(specification.getOptions(), localized.getOptions());
            Configuration localizedConfiguration = new Configuration(false);
            localizedConfiguration.addResource(
                    new Path(staging, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME));
            assertEquals(
                    "localized-value", localizedConfiguration.get("seatunnel.test.hadoop-option"));
            assertArrayEquals(
                    Files.readAllBytes(
                            Paths.get(specification.getOption(YarnOptions.DISTRIBUTION))),
                    Files.readAllBytes(stagedFiles.resolve("distribution.zip")));
            assertTrue(Files.exists(Paths.get(staging.toUri())));
        }
        verify(client, never()).killApplication(any());
        verify(client).stop();
    }

    @Test
    void failedUploadRemovesOnlyItsNewStagingDirectory() throws Exception {
        ApplicationSpecification specification = specification();
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(specification);
        Configuration configuration = spy(localConfiguration());
        java.nio.file.Path staging = temporary.toPath().resolve("application_1_0001");
        java.nio.file.Path existing =
                Files.createDirectories(temporary.toPath().resolve("application_1_0002"));
        Files.write(existing.resolve("preserve"), new byte[] {1});
        IOException uploadFailure = new IOException("Failed to write Hadoop configuration");
        doAnswer(
                        invocation -> {
                            assertTrue(Files.exists(staging.resolve("distribution.zip")));
                            assertTrue(
                                    Files.exists(
                                            staging.resolve(
                                                    YarnConstants.LOCALIZED_SPECIFICATION_NAME)));
                            throw uploadFailure;
                        })
                .when(configuration)
                .writeXml(any(OutputStream.class));
        try (YarnApplicationFileUploader uploader =
                new YarnApplicationFileUploader(
                        configuration, deployment, ApplicationId.newInstance(1, 1), true)) {
            assertSame(uploadFailure, assertThrows(IOException.class, uploader::upload));
        }
        assertFalse(Files.exists(staging));
        assertTrue(Files.exists(existing.resolve("preserve")));
        assertTrue(deployment.getDistribution().isFile());
    }

    @Test
    void uploaderCloseRetainsFilesAndRegisteredResources() throws Exception {
        Configuration configuration = localConfiguration();
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(specification());
        Path staging;
        YarnLocalResourceDescriptor registered;
        try (YarnApplicationFileUploader uploader =
                new YarnApplicationFileUploader(
                        configuration, deployment, ApplicationId.newInstance(1, 1), true)) {
            staging = uploader.getApplicationDir();
            assertFalse(Files.exists(Paths.get(staging.toUri())));
            registered = uploader.upload();
            assertThrows(IllegalStateException.class, uploader::upload);
        }
        assertTrue(Files.exists(Paths.get(staging.toUri())));
        assertEquals("seatunnel/apache-seatunnel/", registered.getHome());
        assertEquals(3, registered.getResources().size());
        assertEquals(
                LocalResourceType.ARCHIVE,
                registered.getResources().get(YarnConstants.LOCALIZED_DISTRIBUTION_NAME).getType());
        registered
                .getResources()
                .values()
                .forEach(
                        resource ->
                                assertEquals(
                                        LocalResourceVisibility.APPLICATION,
                                        resource.getVisibility()));
        assertEquals(
                YarnContainerLaunchContextFactory.master(configuration, staging, 512)
                        .getLocalResources(),
                registered.getResources());
    }

    @Test
    void invalidArchiveIsRejectedBeforeStagingOrSubmission() throws Exception {
        ApplicationSpecification specification = specification();
        try (ZipOutputStream zip =
                new ZipOutputStream(
                        Files.newOutputStream(
                                Paths.get(specification.getOption(YarnOptions.DISTRIBUTION))))) {
            zip.putNextEntry(new ZipEntry("../starter/seatunnel-starter.jar"));
            zip.closeEntry();
        }
        YarnClient client = client();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationClusterDescriptor(
                                                localConfiguration(), () -> client, true)
                                        .deployApplication(specification));
        assertTrue(failure.getMessage().contains("Unsafe distribution archive entry"));
        assertFalse(Files.exists(temporary.toPath().resolve("application_1_0001")));
        verify(client, never()).submitApplication(any());
        verify(client, never()).killApplication(any());
        verify(client).stop();
    }

    @Test
    void lostSubmissionResponseKillsPotentiallyAcceptedApplicationAndCleansStaging()
            throws Exception {
        YarnClient client = client();
        when(client.submitApplication(any())).thenThrow(new IOException("lost response"));
        assertThrows(
                IOException.class,
                () ->
                        new YarnApplicationClusterDescriptor(
                                        localConfiguration(), () -> client, true)
                                .deployApplication(specification()));
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
                        new YarnApplicationClusterDescriptor(
                                        localConfiguration(), () -> client, true)
                                .deployApplication(specification()));
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
                        ApplicationId.newInstance(1, 1),
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
        try (YarnApplicationClient deployed =
                new YarnApplicationClient(
                        client,
                        localConfiguration(),
                        ApplicationId.newInstance(1, 1),
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
                IllegalArgumentException.class,
                () -> new YarnApplicationClusterDescriptor(configuration));
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
        options.put(YarnOptions.DISTRIBUTION.key(), archive.toString());
        options.put(YarnOptions.STAGING_DIRECTORY.key(), temporary.toURI().toString());
        options.put(YarnOptions.PRIORITY.key(), "3");
        options.put(YarnOptions.TAGS.key(), "batch,finance,batch");
        options.put(YarnOptions.MASTER_NODE_LABEL.key(), "master-pool");
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
        report.setHost("master");
        report.setRpcPort(5801);
        when(client.getApplicationReport(any())).thenReturn(report);
        return client;
    }
}
