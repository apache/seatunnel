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

import org.apache.seatunnel.shade.com.google.common.io.Closer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.deployment.SeatunnelClientProvider;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnApplicationCli;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationClient;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.YarnApplicationFileUploader;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;
import org.apache.seatunnel.resource.yarn.launch.factory.ApplicationMasterContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResourceRequirements;
import org.apache.seatunnel.resource.yarn.worker.SeatunnelYarnApplicationWorker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.hazelcast.client.config.ClientConfig;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "The YARN tests use a Unix container environment")
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
        try {
            Closer closer = Closer.create();
            try {
                MockedStatic<ConfigProvider> configurations = mockStatic(ConfigProvider.class);
                closer.register(configurations::close);
                MockedStatic<SeaTunnelServerStarter> starter =
                        mockStatic(SeaTunnelServerStarter.class);
                closer.register(starter::close);
                MockedStatic<YarnStagingDirectory> staging = mockStatic(YarnStagingDirectory.class);
                closer.register(staging::close);
                configurations.when(ConfigProvider::locateAndGetSeaTunnelConfig).thenReturn(config);
                SeatunnelYarnApplicationWorker.main(
                        new String[] {"yarn-app", "master:5801", "3", masterHome.toString()});
                starter.verify(
                        () ->
                                SeaTunnelServerStarter.createHazelcastInstance(
                                        eq(config), isNull(), any(ResourceManagerFactory.class)));
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
                assertFalse(config.getEngineConfig().getHttpConfig().isEnabled());
                assertEquals(
                        "true",
                        config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.enabled"));
                assertEquals(
                        "GRACEFUL",
                        config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.policy"));
                TaskGroupImmutableInformation task = new TaskGroupImmutableInformation();
                task.setJars(
                        Collections.singletonList(
                                Collections.singleton(
                                        masterHome.resolve("connector.jar").toUri().toURL())));
                config.getHazelcastConfig().getManagedContext().initialize(task);
                assertEquals(
                        Collections.singletonList(Collections.singleton(localJar.toUri().toURL())),
                        task.getJars());
            } catch (Throwable failure) {
                throw closer.rethrow(failure, Exception.class);
            } finally {
                closer.close();
            }
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
        MockedStatic<SeaTunnelServerStarter> starter = mockStatic(SeaTunnelServerStarter.class);
        try {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> SeatunnelYarnApplicationWorker.main(new String[] {"yarn-app"}));
            starter.verifyNoInteractions();
        } finally {
            starter.close();
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
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options);
        try {
            provider = descriptor.retrieve(id);
            assertTrue(nativeClients.constructed().isEmpty());
            first = provider.getClusterClient();
        } finally {
            descriptor.close();
        }
        verify(first, never()).close();
        Closer closer = Closer.create();
        try {
            SeaTunnelClient application = first;
            closer.register(application::close);
            SeaTunnelClient second = provider.getClusterClient();
            closer.register(second::close);
            assertSame(application, nativeClients.constructed().get(0));
            assertSame(second, nativeClients.constructed().get(1));
            assertNotSame(application, second);
            assertEquals("seatunnel-application-" + id, clientConfigs.get(0).getClusterName());
            assertEquals(
                    Collections.singletonList("master:5801"),
                    clientConfigs.get(0).getNetworkConfig().getAddresses());
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
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
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options());
        try {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            new YarnApplicationClusterDescriptorFactory()
                                    .parseApplicationId("not-a-yarn-application"));
        } finally {
            descriptor.close();
        }
        verify(client, never()).init(any());
        verify(client, never()).start();
    }

    @Test
    void keepsNativeYarnReportFailuresInsideTheProvider() throws Exception {
        YarnClient client = client();
        ApplicationId id = ApplicationId.newInstance(1, 1);
        YarnException unavailable = new YarnException("RM unreachable");
        when(client.getApplicationReport(id))
                .thenThrow(new ApplicationNotFoundException("expired"))
                .thenThrow(unavailable);
        YarnApplicationClient application =
                new YarnApplicationClient(
                        client, localConfiguration(), id, new Path(temporary.toURI()));
        assertEquals(ApplicationStatus.UNKNOWN, application.getStatus());
        YarnClientException failure =
                assertThrows(YarnClientException.class, application::getStatus);
        assertTrue(failure.getMessage().contains(id.toString()));
        assertSame(unavailable, failure.getCause());
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
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options);
        try {
            assertEquals(ApplicationStatus.SUCCEEDED, descriptor.getApplicationStatus(id));
            assertFalse(Files.exists(staging));
            assertThrows(IllegalStateException.class, () -> descriptor.retrieve(id));
            descriptor.cancelApplication(id);
            verify(client).killApplication(id);
            assertTrue(nativeClients.constructed().isEmpty());
        } finally {
            descriptor.close();
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
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options());
        try {
            assertEquals(
                    ApplicationId.newInstance(1, 1), descriptor.deployApplication(specification()));
            assertTrue(nativeClients.constructed().isEmpty());
        } finally {
            descriptor.close();
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
                                                localConfiguration(),
                                                () -> client,
                                                false,
                                                options())
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
        Map<String, String> options = options();
        options.put("application.startup-timeout-millis", "1");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        assertThrows(
                TimeoutException.class,
                () ->
                        new YarnApplicationClusterDescriptor(
                                        localConfiguration(), () -> client, true, options())
                                .deployApplication(specification));
        verify(client).killApplication(ApplicationId.newInstance(1, 1));
        assertFalse(Files.exists(temporary.toPath().resolve("application_1_0001")));
    }

    @Test
    void missingDistributionHasActionableValidationMessage() {
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", Collections.emptyMap());
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationClusterDescriptor(localConfiguration())
                                        .deployApplication(specification));
        assertEquals("Required option yarn.distribution is missing", failure.getMessage());
    }

    @Test
    void requiresAJobBeforeContactingYarn() throws Exception {
        YarnClient client = client();
        Map<String, String> options = options();
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options);
        try {
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    descriptor.deployApplication(
                                            SeatunnelApplicationConfig.parse("", options)));
            assertTrue(failure.getMessage().contains(YarnOptions.JOB_CONFIG_FILE.key()));
            verify(client, never()).createApplication();
        } finally {
            descriptor.close();
        }
    }

    @Test
    void localizesThePlatformJobOnlyForTheMaster() throws Exception {
        Map<String, String> options = options();
        java.nio.file.Path jobFile = temporary.toPath().resolve("platform.conf");
        Files.writeString(jobFile, "env { parallelism = 7 }");
        options.put(YarnOptions.JOB_CONFIG_FILE.key(), jobFile.toString());
        ApplicationSpecification inline =
                SeatunnelApplicationConfig.parse("env { NAME = INLINE_ONLY }", options);
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        inline, ReadonlyConfig.fromMap(new HashMap<>(options)));
        Configuration configuration = localConfiguration();
        Closer closer = Closer.create();
        try {
            YarnApplicationFileUploader uploader =
                    closer.register(
                            new YarnApplicationFileUploader(
                                    configuration,
                                    deployment,
                                    ApplicationId.newInstance(1, 1),
                                    true));
            YarnLocalResourceDescriptor master = uploader.upload();
            assertEquals(
                    LocalResourceType.FILE,
                    master.getResources().get(YarnConstants.LOCALIZED_JOB_CONFIG_NAME).getType());
            Path staging = uploader.getApplicationDir();
            java.nio.file.Path spec =
                    Paths.get(staging.toUri()).resolve(YarnConstants.LOCALIZED_SPECIFICATION_NAME);
            assertFalse(Files.readString(spec).contains("INLINE_ONLY"));
            assertEquals(
                    Files.readString(jobFile),
                    YarnApplicationConfiguration.read(spec).getSpecification().getJobConfig());
            assertFalse(
                    YarnContainerLaunchContextFactory.worker(
                                    configuration,
                                    staging,
                                    "cluster",
                                    "master:5801",
                                    inline.getWorkerSpecification(),
                                    "user",
                                    "/master")
                            .getLocalResources()
                            .containsKey(YarnConstants.LOCALIZED_JOB_CONFIG_NAME));
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
    }

    @Test
    void launchesBothRolesWithTheirLocalizedConfiguration() throws Exception {
        Map<String, String> options = options();
        java.nio.file.Path engine = temporary.toPath().resolve("engine.yaml");
        Files.writeString(engine, "seatunnel: {}");
        java.nio.file.Path deploymentFile = temporary.toPath().resolve("deployment.conf");
        Files.writeString(
                deploymentFile,
                "yarn { env.REGION = \"eu-west\", ship-files.\"engine.yaml\" = \""
                        + engine.toUri()
                        + "\" }");
        options = SeatunnelApplicationConfig.load(deploymentFile, options);
        options.put(YarnOptions.SEATUNNEL_CONFIG_FILE.key(), "files/engine.yaml");
        options.put(YarnOptions.JVM_OPTIONS.key(), "common.opts");
        options.put(YarnOptions.MASTER_JVM_OPTIONS.key(), "master.opts");
        options.put(YarnOptions.WORKER_JVM_OPTIONS.key(), YarnConstants.DISABLED);
        options.put(YarnOptions.MASTER_HAZELCAST_CONFIG.key(), "master.xml");
        options.put(YarnOptions.WORKER_HAZELCAST_CONFIG.key(), "worker.yaml");
        options.put(YarnOptions.LOG4J_CONFIG_FILE.key(), "/etc/seatunnel/log4j2.xml");
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        Configuration configuration = localConfiguration();
        Closer closer = Closer.create();
        try {
            YarnApplicationFileUploader uploader =
                    closer.register(
                            new YarnApplicationFileUploader(
                                    configuration,
                                    deployment,
                                    ApplicationId.newInstance(1, 1),
                                    true));
            YarnLocalResourceDescriptor localized = uploader.upload();
            Path staging = uploader.getApplicationDir();
            ContainerLaunchContext master =
                    YarnContainerLaunchContextFactory.master(staging, 512, localized, "submitter");
            ContainerLaunchContext worker =
                    YarnContainerLaunchContextFactory.worker(
                            configuration,
                            staging,
                            "cluster",
                            "host:5801",
                            deployment.getSpecification().getWorkerSpecification(),
                            "submitter",
                            "/master");
            Files.writeString(engine, "updated after submission");
            assertEquals(
                    "seatunnel: {}",
                    Files.readString(Paths.get(staging.toUri()).resolve("files/engine.yaml")));
            for (ContainerLaunchContext launch : Arrays.asList(master, worker)) {
                assertEquals(
                        LocalResourceType.FILE,
                        launch.getLocalResources().get("files/engine.yaml").getType());
                assertEquals("eu-west", launch.getEnvironment().get("REGION"));
                String command = launch.getCommands().get(0);
                assertTrue(command.contains("files/engine.yaml"));
                assertTrue(command.contains("/etc/seatunnel/log4j2.xml"));
                assertTrue(command.contains("/config/common.opts"));
            }
            String masterCommand = master.getCommands().get(0);
            assertTrue(masterCommand.indexOf("master.opts") > masterCommand.indexOf("common.opts"));
            assertTrue(masterCommand.contains("/config/master.xml"));
            String workerCommand = worker.getCommands().get(0);
            assertTrue(workerCommand.contains("/config/worker.yaml"));
            assertFalse(workerCommand.contains("master.opts"));
            assertFalse(workerCommand.contains("jvm_worker_options"));
            assertFalse(
                    worker.getLocalResources()
                            .containsKey(YarnConstants.LOCALIZED_SPECIFICATION_NAME));
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
    }

    @Test
    void loadsTheSelectedXmlMemberConfiguration() throws Exception {
        java.nio.file.Path xml = temporary.toPath().resolve("member.xml");
        Files.writeString(
                xml,
                "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"><cluster-name>yarn-member</cluster-name></hazelcast>");
        String previous = System.getProperty(YarnConstants.HAZELCAST_CONFIG_PROPERTY);
        try {
            System.setProperty(YarnConstants.HAZELCAST_CONFIG_PROPERTY, xml.toString());
            assertEquals(
                    "yarn-member",
                    YarnConfigurationUtils.loadEngineConfig()
                            .getHazelcastConfig()
                            .getClusterName());
        } finally {
            if (previous == null) {
                System.clearProperty(YarnConstants.HAZELCAST_CONFIG_PROPERTY);
            } else {
                System.setProperty(YarnConstants.HAZELCAST_CONFIG_PROPERTY, previous);
            }
        }
    }

    @Test
    void stagesInlineFilesVerbatimAndKeepsTheJobPrivateToTheMaster() throws Exception {
        String yaml = "# région 一\nseatunnel:\n  engine:\n    backup-count: 0\n";
        String job = "env { JOB_MARKER = \"${REMOTE_VALUE}\" }\n";
        java.nio.file.Path conf = temporary.toPath().resolve("inline.conf");
        Files.writeString(
                conf,
                "yarn {\n"
                        + "local-resources.generated {\n"
                        + "  content {\n"
                        + "    \"engine.yaml\" = \"\"\""
                        + yaml
                        + "\"\"\"\n"
                        + "    \"orders.conf\" = \"\"\""
                        + job
                        + "\"\"\"\n"
                        + "    \"empty.opts\" = \"\"\n"
                        + "  }\n"
                        + "  localize { \"engine.yaml\" = files/conf/engine.yaml }\n"
                        + "}\n"
                        + "seatunnel-config-file = files/conf/engine.yaml\n"
                        + "job-config-file = files/orders.conf\n}");
        Map<String, String> options = options();
        options.putAll(SeatunnelApplicationConfig.load(conf, Collections.emptyMap()));
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        Configuration configuration = localConfiguration();
        Closer closer = Closer.create();
        try {
            YarnApplicationFileUploader uploader =
                    closer.register(
                            new YarnApplicationFileUploader(
                                    configuration,
                                    deployment,
                                    ApplicationId.newInstance(1, 1),
                                    true));
            YarnLocalResourceDescriptor master = uploader.upload();
            Path staging = uploader.getApplicationDir();
            java.nio.file.Path directory = Paths.get(staging.toUri());
            assertEquals(yaml, Files.readString(directory.resolve("files/conf/engine.yaml")));
            assertEquals(0, Files.size(directory.resolve("files/empty.opts")));
            assertEquals(
                    job,
                    Files.readString(directory.resolve(YarnConstants.LOCALIZED_JOB_CONFIG_NAME)));
            assertFalse(Files.exists(directory.resolve("files/orders.conf")));
            String manifest = Files.readString(directory.resolve("distribution.properties"));
            assertFalse(manifest.contains("backup-count"));
            assertFalse(manifest.contains("JOB_MARKER"));
            assertFalse(manifest.contains("orders.conf"));
            assertTrue(master.getResources().containsKey(YarnConstants.LOCALIZED_JOB_CONFIG_NAME));
            ContainerLaunchContext worker =
                    YarnContainerLaunchContextFactory.worker(
                            configuration,
                            staging,
                            "cluster",
                            "master:5801",
                            deployment.getSpecification().getWorkerSpecification(),
                            "user",
                            "/master");
            assertTrue(worker.getLocalResources().containsKey("files/conf/engine.yaml"));
            assertFalse(
                    worker.getLocalResources()
                            .containsKey(YarnConstants.LOCALIZED_JOB_CONFIG_NAME));
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
    }

    @Test
    void rejectsTwoSourcesForTheSameLocalizedFile() throws Exception {
        Map<String, String> options = options();
        options.put(YarnOptions.shipFile("worker.opts").key(), "/etc/seatunnel/worker.opts");
        options.put(YarnOptions.inlineFile("worker.opts").key(), "-Dexample=true");
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                YarnApplicationConfiguration.forSubmission(
                                        specification(),
                                        ReadonlyConfig.fromMap(new HashMap<>(options))));
        assertTrue(failure.getMessage().contains("worker.opts"));
    }

    @Test
    void rejectsIncompleteGroupsAndConflictingLocalizationPaths() throws Exception {
        for (Map<String, String> group :
                List.of(
                        Map.of("yarn.local-resources.runtime.source-dir", "/etc/seatunnel"),
                        Map.of(
                                "yarn.local-resources.runtime.localize.engine.yaml",
                                "files/engine.yaml"),
                        Map.of(
                                "yarn.local-resources.runtime.content.a", "one",
                                "yarn.local-resources.runtime.localize.a", "files/../engine.yaml"),
                        Map.of(
                                "yarn.local-resources.runtime.source-dir", "/source",
                                "yarn.local-resources.runtime.content.a", "one",
                                "yarn.local-resources.runtime.localize.a", "files/a"),
                        Map.of(
                                "yarn.local-resources.runtime.content.a", "one",
                                "yarn.local-resources.other.content.a", "two"),
                        Map.of(
                                "yarn.local-resources.runtime.content.a", "one",
                                "yarn.local-resources.other.content.b", "two",
                                "yarn.local-resources.other.localize.b", "files/a/b"),
                        Map.of("yarn.local-resources.runtime.unknown", "ignored"))) {
            Map<String, String> options = options();
            options.putAll(group);
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            YarnApplicationConfiguration.forSubmission(
                                    specification(),
                                    ReadonlyConfig.fromMap(new HashMap<>(options))),
                    group.keySet().toString());
        }
    }

    @Test
    void rejectsInvalidContainerOptionsDuringSubmission() throws Exception {
        for (Map.Entry<String, String> entry :
                Map.of(
                                "yarn.env.1INVALID",
                                "x",
                                "yarn.env.HADOOP_USER_NAME",
                                "other",
                                "yarn.ship-files.../escape",
                                "local",
                                "yarn.inline-files.../escape",
                                "text",
                                "yarn.seatunnel-config-file",
                                "files/missing.yaml",
                                "yarn.master.hazelcast-config",
                                "../config.xml",
                                "yarn.worker.hazelcast-config",
                                "member.conf",
                                "yarn.active-deadline-seconds",
                                "0",
                                "yarn.master.http-port",
                                "65536")
                        .entrySet()) {
            Map<String, String> options = options();
            options.put(entry.getKey(), entry.getValue());
            IllegalArgumentException failure =
                    assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    YarnApplicationConfiguration.forSubmission(
                                            specification(),
                                            ReadonlyConfig.fromMap(new HashMap<>(options))));
            assertTrue(failure.getMessage().contains(entry.getKey()), failure.getMessage());
        }
    }

    @Test
    void resolvesLocalPathsAndHdfsUrisIndependentlyOfTheDefaultFileSystem() throws Exception {
        File local = Files.createFile(temporary.toPath().resolve("release archive.zip")).toFile();
        Map<String, String> options = options();
        ApplicationSpecification spec = specification();
        for (String location :
                Arrays.asList(
                        Paths.get("").toAbsolutePath().relativize(local.toPath()).toString(),
                        local.toURI().toASCIIString())) {
            options.put(YarnOptions.DISTRIBUTION.key(), location);
            YarnApplicationConfiguration deployment =
                    YarnApplicationConfiguration.forSubmission(
                            spec, ReadonlyConfig.fromMap(new HashMap<>(options)));
            assertEquals(local.getCanonicalFile(), deployment.getDistribution().getCanonicalFile());
        }
        String hdfs = "hdfs://nameservice/releases/seatunnel.zip";
        options.put(YarnOptions.DISTRIBUTION.key(), hdfs);
        assertEquals(
                hdfs,
                YarnApplicationConfiguration.forSubmission(
                                spec, ReadonlyConfig.fromMap(new HashMap<>(options)))
                        .getDistributionUri()
                        .toString());
        options.put(YarnOptions.DISTRIBUTION.key(), "https://example.org/seatunnel.zip");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        YarnApplicationConfiguration.forSubmission(
                                spec, ReadonlyConfig.fromMap(new HashMap<>(options))));
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
    void successfulStatusSurvivesStagingFailureAndRetriesOnTheNextQuery() throws Exception {
        YarnClient client = client();
        ApplicationId id = ApplicationId.newInstance(1, 1);
        ApplicationReport report = client.getApplicationReport(id);
        report.setYarnApplicationState(YarnApplicationState.FINISHED);
        report.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        java.nio.file.Path staging =
                Files.createDirectory(temporary.toPath().resolve(id.toString()));
        Configuration configuration = localConfiguration();
        Path directory = new Path(staging.toString());
        YarnApplicationClient application =
                new YarnApplicationClient(client, configuration, id, directory);
        try {
            FileSystem.setDefaultUri(configuration, "unavailable://staging");
            assertEquals(ApplicationStatus.SUCCEEDED, application.getStatus());
            assertTrue(Files.isDirectory(staging));

            FileSystem.setDefaultUri(configuration, "file:///");
            assertEquals(ApplicationStatus.SUCCEEDED, application.getStatus());
            assertFalse(Files.exists(staging));
        } finally {
            application.close();
        }
    }

    @Test
    void submitStagesPrivateArtifactsAndDisablesRetries() throws Exception {
        Configuration configuration = localConfiguration();
        configuration.set("seatunnel.test.hadoop-option", "localized-value");
        YarnClient client = client();
        ApplicationSpecification specification = specification();
        Map<String, String> deploymentOptions = options();
        deploymentOptions.put(YarnOptions.MASTER_HTTP_PORT.key(), "8095");
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        configuration, () -> client, true, deploymentOptions);
        UserGroupInformation submitter = UserGroupInformation.createRemoteUser("batch-submitter");
        try {
            ApplicationId deployed =
                    submitter.doAs(
                            (PrivilegedExceptionAction<ApplicationId>)
                                    () -> descriptor.deployApplication(specification));
            assertTrue(nativeClients.constructed().isEmpty());
            ArgumentCaptor<ApplicationSubmissionContext> context =
                    ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
            verify(client).submitApplication(context.capture());
            assertEquals(context.getValue().getApplicationId(), deployed);
            assertEquals(1, context.getValue().getMaxAppAttempts());
            assertEquals(
                    3600L,
                    context.getValue()
                            .getApplicationTimeouts()
                            .get(ApplicationTimeoutType.LIFETIME)
                            .longValue());
            assertEquals("test-application", context.getValue().getApplicationName());
            assertEquals(3, context.getValue().getPriority().getPriority());
            assertEquals(
                    new HashSet<>(Arrays.asList("batch", "finance")),
                    context.getValue().getApplicationTags());
            assertEquals("master-pool", context.getValue().getNodeLabelExpression());
            ContainerLaunchContext launch = context.getValue().getAMContainerSpec();
            assertEquals(3, launch.getLocalResources().size());
            assertTrue(launch.getCommands().get(0).contains("-Dseatunnel.home="));
            assertEquals("{{PWD}}", launch.getEnvironment().get(YarnConstants.HADOOP_CONF_DIR_ENV));
            assertEquals(
                    submitter.getShortUserName(),
                    launch.getEnvironment().get(YarnConstants.HADOOP_USER_NAME_ENV));
            assertTrue(
                    launch.getCommands()
                            .get(0)
                            .contains("seatunnel/apache-seatunnel/starter/seatunnel-starter.jar:"));
            assertFalse(launch.getCommands().get(0).contains("starter/*"));
            assertTrue(launch.getCommands().get(0).contains("starter/logging/*"));
            assertTrue(
                    launch.getCommands()
                            .get(0)
                            .contains(SeatunnelYarnApplicationCli.class.getName()));
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
            YarnApplicationConfiguration localizedDeployment =
                    YarnApplicationConfiguration.read(
                            stagedFiles.resolve(YarnConstants.LOCALIZED_SPECIFICATION_NAME));
            ApplicationSpecification localized = localizedDeployment.getSpecification();
            assertEquals(specification.getJobConfig(), localized.getJobConfig());
            assertEquals(specification.getJobId(), localized.getJobId());
            assertEquals("worker-pool", localizedDeployment.getWorkerNodeLabel());
            assertEquals(8095, localizedDeployment.getMasterHttpPort().intValue());
            assertEquals(submitter.getShortUserName(), localizedDeployment.getHadoopUserName());
            String localizedContent =
                    new String(
                            Files.readAllBytes(
                                    stagedFiles.resolve(
                                            YarnConstants.LOCALIZED_SPECIFICATION_NAME)),
                            java.nio.charset.StandardCharsets.UTF_8);
            assertFalse(localizedContent.contains("yarn.distribution"));
            assertFalse(localizedContent.contains("yarn.staging-dir"));
            assertFalse(localizedContent.contains("yarn.queue"));
            Configuration localizedConfiguration = new Configuration(false);
            localizedConfiguration.addResource(
                    new Path(staging, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME));
            assertEquals(
                    "localized-value", localizedConfiguration.get("seatunnel.test.hadoop-option"));
            assertArrayEquals(
                    Files.readAllBytes(temporary.toPath().resolve("distribution.zip")),
                    Files.readAllBytes(stagedFiles.resolve("distribution.zip")));
            assertTrue(Files.exists(Paths.get(staging.toUri())));
        } finally {
            descriptor.close();
        }
        verify(client, never()).killApplication(any());
        verify(client).stop();
    }

    @Test
    void failedUploadRemovesOnlyItsNewStagingDirectory() throws Exception {
        ApplicationSpecification specification = specification();
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        specification, ReadonlyConfig.fromMap(new HashMap<>(options())));
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
        Closer closer = Closer.create();
        try {
            YarnApplicationFileUploader uploader =
                    closer.register(
                            new YarnApplicationFileUploader(
                                    configuration,
                                    deployment,
                                    ApplicationId.newInstance(1, 1),
                                    true));
            assertSame(uploadFailure, assertThrows(IOException.class, uploader::upload));
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
        assertFalse(Files.exists(staging));
        assertTrue(Files.exists(existing.resolve("preserve")));
        assertTrue(deployment.getDistribution().isFile());
    }

    @Test
    void uploaderCloseRetainsFilesAndRegisteredResources() throws Exception {
        Configuration configuration = localConfiguration();
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options())));
        Path staging;
        YarnLocalResourceDescriptor registered;
        Closer closer = Closer.create();
        try {
            YarnApplicationFileUploader uploader =
                    closer.register(
                            new YarnApplicationFileUploader(
                                    configuration,
                                    deployment,
                                    ApplicationId.newInstance(1, 1),
                                    true));
            staging = uploader.getApplicationDir();
            assertFalse(Files.exists(Paths.get(staging.toUri())));
            registered = uploader.upload();
            assertThrows(IllegalStateException.class, uploader::upload);
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
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
                YarnContainerLaunchContextFactory.master(
                                configuration, staging, 512, deployment.getHadoopUserName())
                        .getLocalResources(),
                registered.getResources());
    }

    @Test
    void invalidArchiveIsRejectedBeforeStagingOrSubmission() throws Exception {
        ApplicationSpecification specification = specification();
        Closer closer = Closer.create();
        try {
            ZipOutputStream zip =
                    closer.register(
                            new ZipOutputStream(
                                    Files.newOutputStream(
                                            temporary.toPath().resolve("distribution.zip"))));
            zip.putNextEntry(new ZipEntry("../starter/seatunnel-starter.jar"));
            zip.closeEntry();
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
        YarnClient client = client();
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new YarnApplicationClusterDescriptor(
                                                localConfiguration(), () -> client, true, options())
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
        IOException transport = new IOException("lost response");
        when(client.submitApplication(any())).thenThrow(transport);
        YarnClientException failure =
                assertThrows(
                        YarnClientException.class,
                        () ->
                                new YarnApplicationClusterDescriptor(
                                                localConfiguration(), () -> client, true, options())
                                        .deployApplication(specification()));
        assertSame(transport, failure.getCause());
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
                                        localConfiguration(), () -> client, true, options())
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
        YarnApplicationClient deployed =
                new YarnApplicationClient(
                        client,
                        localConfiguration(),
                        ApplicationId.newInstance(1, 1),
                        new Path(staging.toURI()));
        try {
            assertEquals(ApplicationStatus.FAILED, deployed.getStatus());
            assertFalse(Files.exists(staging.toPath()));
        } finally {
            deployed.close();
        }
    }

    @Test
    void simpleSubmissionIgnoresKerberosOptions() throws Exception {
        Map<String, String> options = options();
        options.put(
                YarnOptions.KERBEROS_KEYTAB.key(),
                temporary.toPath().resolve("absent.keytab").toString());
        options.put(YarnOptions.HADOOP_USER_NAME.key(), "simple-user");
        YarnClient client = client();
        YarnApplicationClusterDescriptor descriptor =
                new YarnApplicationClusterDescriptor(
                        localConfiguration(), () -> client, true, options);
        try {
            descriptor.deployApplication(specification());
            ArgumentCaptor<ApplicationSubmissionContext> context =
                    ArgumentCaptor.forClass(ApplicationSubmissionContext.class);
            verify(client).submitApplication(context.capture());
            ContainerLaunchContext master = context.getValue().getAMContainerSpec();
            assertEquals(
                    "simple-user", master.getEnvironment().get(YarnConstants.HADOOP_USER_NAME_ENV));
            assertNull(master.getTokens());
        } finally {
            descriptor.close();
        }
    }

    @Test
    void submissionSpecPreservesResourceManagerDefaultsUnlessConfigured() throws Exception {
        Map<String, String> options = options();
        options.remove(YarnOptions.PRIORITY.key());
        options.remove(YarnOptions.TAGS.key());
        options.remove(YarnOptions.MASTER_NODE_LABEL.key());
        YarnApplicationConfiguration deployment =
                YarnApplicationConfiguration.forSubmission(
                        specification(), ReadonlyConfig.fromMap(new HashMap<>(options)));
        ApplicationSubmissionContext allocated =
                Records.newRecord(ApplicationSubmissionContext.class);
        allocated.setApplicationId(ApplicationId.newInstance(1, 2));
        allocated.setPriority(Priority.newInstance(5));
        allocated.setApplicationTags(Collections.singleton("cluster-default"));
        allocated.setNodeLabelExpression("default-pool");
        YarnResourceRequirements requirements =
                YarnResourceRequirements.forMaster(deployment.getSpecification());
        YarnLocalResourceDescriptor localized = mock(YarnLocalResourceDescriptor.class);
        when(localized.getHome()).thenReturn("seatunnel/");
        YarnLaunchParameters parameters =
                new YarnLaunchParameters(
                        localConfiguration(), new Path("/staging"), "submitter", localized);
        YarnContainerSpec master =
                new YarnContainerSpec(
                        EngineConfig.ClusterRole.MASTER,
                        requirements,
                        parameters,
                        "master.Main",
                        List.of());

        ApplicationSubmissionContext submission =
                ApplicationMasterContainerFactory.createSubmission(allocated, deployment, master);
        assertEquals(ApplicationId.newInstance(1, 2), submission.getApplicationId());
        assertEquals(5, submission.getPriority().getPriority());
        assertEquals(Collections.singleton("cluster-default"), submission.getApplicationTags());
        assertEquals("default-pool", submission.getNodeLabelExpression());
        assertEquals(requirements.toResource(), submission.getResource());
        assertEquals(master.getInternalResource(), submission.getAMContainerSpec());
    }

    @Test
    void kerberosKeytabRequiresAMatchingPrincipal() {
        Configuration configuration = localConfiguration();
        configuration.set("hadoop.security.authentication", "kerberos");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new YarnApplicationClusterDescriptor(
                                configuration,
                                Collections.singletonMap(
                                        YarnOptions.KERBEROS_KEYTAB.key(), "submitter.keytab")));
    }

    private Configuration localConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        return configuration;
    }

    private ApplicationSpecification specification() throws Exception {
        return SeatunnelApplicationConfig.parse("env {}", options());
    }

    private Map<String, String> options() throws Exception {
        File archive = new File(temporary, "distribution.zip");
        if (!archive.exists()) {
            Closer closer = Closer.create();
            try {
                ZipOutputStream zip =
                        closer.register(
                                new ZipOutputStream(Files.newOutputStream(archive.toPath())));
                zip.putNextEntry(new ZipEntry("apache-seatunnel/starter/seatunnel-starter.jar"));
                zip.write(new byte[] {1, 2, 3});
                zip.closeEntry();
            } catch (Throwable failure) {
                throw closer.rethrow(failure, Exception.class);
            } finally {
                closer.close();
            }
        }
        Map<String, String> options = new HashMap<>();
        options.put(YarnOptions.DISTRIBUTION.key(), archive.toString());
        options.put(YarnOptions.STAGING_DIRECTORY.key(), temporary.toURI().toString());
        options.put(YarnOptions.PRIORITY.key(), "3");
        options.put(YarnOptions.ACTIVE_DEADLINE_SECONDS.key(), "3600");
        options.put(YarnOptions.TAGS.key(), "batch,finance,batch");
        options.put(YarnOptions.MASTER_NODE_LABEL.key(), "master-pool");
        options.put(YarnOptions.WORKER_NODE_LABEL.key(), "worker-pool");
        options.put("application.name", "test-application");
        options.put("application.worker.memory-mb", "512");
        return options;
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
