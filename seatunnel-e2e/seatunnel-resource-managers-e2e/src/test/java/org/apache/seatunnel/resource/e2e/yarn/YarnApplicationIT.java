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

package org.apache.seatunnel.resource.e2e.yarn;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.engine.client.deployment.ApplicationClusterDeployer;
import org.apache.seatunnel.engine.client.deployment.ClusterClientServiceLoader;
import org.apache.seatunnel.engine.client.deployment.ClusterDescriptor;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnWorkerCli;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationClient;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executes packaged runtime artifacts in real NodeManager-launched JVMs backed by MiniDFSCluster.
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class YarnApplicationIT extends TestSuiteBase {
    @TempDir static File temporary;
    private MiniDFSCluster hdfs;
    private MiniYARNCluster yarn;
    private Configuration configuration;
    private ClusterDescriptor<ApplicationId> deployer;
    private final ClusterClientServiceLoader clientServiceLoader = new ClusterClientServiceLoader();
    private String distribution;
    private File distributionHome;
    private java.nio.file.Path applicationConfig;

    @BeforeAll
    void startCluster() throws Exception {
        distribution = prepareDistribution().getAbsolutePath();
        Configuration hdfsConfiguration = new HdfsConfiguration();
        hdfsConfiguration.set(
                MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporary.toPath().resolve("hdfs").toString());
        hdfsConfiguration.setBoolean("dfs.permissions.enabled", false);
        hdfs = new MiniDFSCluster.Builder(hdfsConfiguration).numDataNodes(1).build();
        hdfs.waitActive();
        YarnConfiguration yarnConfiguration = new YarnConfiguration(hdfsConfiguration);
        yarnConfiguration.set("fs.defaultFS", hdfs.getFileSystem().getUri().toString());
        yarnConfiguration.setInt(YarnConfiguration.NM_PMEM_MB, 8192);
        yarnConfiguration.setInt(YarnConfiguration.NM_VCORES, 16);
        yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, 4096);
        yarnConfiguration.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 8);
        yarnConfiguration.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        yarnConfiguration.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
        yarnConfiguration.setFloat("yarn.scheduler.capacity.maximum-am-resource-percent", 1.0f);
        yarnConfiguration.setInt("yarn.nodemanager.delete.debug-delay-sec", 600);
        yarnConfiguration.set(YarnConfiguration.NM_ENV_WHITELIST, "JAVA_HOME,PATH,LANG");
        yarn = new MiniYARNCluster("seatunnel-yarn-application", 1, 1, 1);
        yarn.init(yarnConfiguration);
        yarn.start();
        assertTrue(yarn.waitForNodeManagersToConnect(30000), "NodeManager did not register");
        configuration = new YarnConfiguration(yarn.getConfig());
        configuration.set("fs.defaultFS", hdfs.getFileSystem().getUri().toString());
        // MiniYARN's StaticMapping lives only in a Hadoop test jar, unavailable to real containers.
        configuration.set(
                "net.topology.node.switch.mapping.impl", ScriptBasedMapping.class.getName());
        File hadoopDirectory = new File(temporary, "hadoop-conf");
        Files.createDirectories(hadoopDirectory.toPath());
        try (OutputStream output =
                Files.newOutputStream(new File(hadoopDirectory, "core-site.xml").toPath())) {
            configuration.writeXml(output);
        }
        Map<String, String> options = new HashMap<>();
        options.put("yarn.config-dir", hadoopDirectory.getAbsolutePath());
        options.put("yarn.staging-dir", "/seatunnel-applications");
        options.put("yarn.distribution", distribution);
        options.put("application.name", "seatunnel-yarn-e2e");
        options.put("application.master.memory-mb", "1024");
        options.put("application.worker.memory-mb", "1024");
        options.put("application.worker.slots", "4");
        applicationConfig = temporary.toPath().resolve("application.config");
        Files.write(
                applicationConfig,
                ConfigFactory.parseMap(options)
                        .root()
                        .render(ConfigRenderOptions.concise())
                        .getBytes(StandardCharsets.UTF_8));
        deployer =
                clientServiceLoader
                        .<ApplicationId>getClusterClientFactory(DeployType.YARN)
                        .create(
                                SeatunnelApplicationConfig.load(
                                        applicationConfig, Collections.emptyMap()));
    }

    /** Prepares only the native layout needed for real YARN archive localization. */
    private File prepareDistribution() throws Exception {
        distributionHome = new File(temporary, "seatunnel");
        File repository = new File(ContainerUtil.PROJECT_ROOT_PATH);
        FileUtils.copyDirectory(
                new File(repository, "config"), new File(distributionHome, "config"));
        Files.copy(
                ContainerUtil.getResourcesFile("/yarn/seatunnel.yaml").toPath(),
                new File(distributionHome, "config/seatunnel.yaml").toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        FileUtils.copyDirectory(
                new File(repository, "seatunnel-core/seatunnel-starter/src/main/bin"),
                new File(distributionHome, "bin"));
        String[][] dependencies = {
            {"seatunnel-starter.jar", "starter"},
            {"seatunnel-shade-hadoop3-uber.jar", "lib"},
            {"connector-fake.jar", "connectors"},
            {"connector-console.jar", "connectors"},
            {"connector-assert.jar", "connectors"},
            {"seatunnel-resource-manager-yarn.jar", "resource-managers/yarn"}
        };
        for (String[] dependency : dependencies) {
            File directory = new File(distributionHome, dependency[1]);
            Files.createDirectories(directory.toPath());
            Files.copy(
                    DependencyJar.staged(dependency[0]).path(),
                    new File(directory, dependency[0]).toPath());
        }
        Files.copy(
                new File(repository, ContainerUtil.PLUGIN_MAPPING_FILE).toPath(),
                new File(distributionHome, "connectors/" + ContainerUtil.PLUGIN_MAPPING_FILE)
                        .toPath());
        return archiveDistribution(distributionHome, "distribution.tar.gz");
    }

    /** Creates a YARN-localizable archive from an exploded SeaTunnel distribution. */
    private File archiveDistribution(File home, String archiveName) throws Exception {
        File archive = new File(temporary, archiveName);
        try (TarArchiveOutputStream output =
                        new TarArchiveOutputStream(
                                new GZIPOutputStream(Files.newOutputStream(archive.toPath())));
                Stream<File> files = Files.walk(home.toPath()).map(path -> path.toFile())) {
            output.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            output.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
            Iterator<File> iterator = files.iterator();
            while (iterator.hasNext()) {
                File file = iterator.next();
                String name =
                        temporary
                                .toPath()
                                .relativize(file.toPath())
                                .toString()
                                .replace(File.separatorChar, '/');
                TarArchiveEntry entry = new TarArchiveEntry(file, name);
                entry.setMode(file.isDirectory() || name.contains("/bin/") ? 0755 : 0644);
                output.putArchiveEntry(entry);
                if (file.isFile()) {
                    Files.copy(file.toPath(), output);
                }
                output.closeArchiveEntry();
            }
        }
        return archive;
    }

    @AfterAll
    void stopCluster() throws Exception {
        if (deployer != null) {
            deployer.close();
        }
        if (yarn != null) {
            yarn.stop();
        }
        if (hdfs != null) {
            hdfs.shutdown();
        }
    }

    @Test
    void batchRunsInAllocatedWorkersAndCleansHdfsArtifacts() throws Exception {
        try (YarnApplicationClient client =
                deployApplication(specification(false, false, 120000, 2))) {
            ApplicationStatus status = awaitTerminal(client);
            assertEquals(ApplicationStatus.SUCCEEDED, status, diagnostics(client));
            assertEquals(
                    2,
                    launchedWorkers(client.getClusterId().toString()).size(),
                    "The successful batch must launch two distinct worker JVMs");
            assertEquals(
                    4,
                    outputRows(client.getClusterId().toString()),
                    "Both parallel readers must emit both splits to the Console sink");
            assertCleaned(client);
            assertEquals(
                    ApplicationStatus.SUCCEEDED,
                    client.getStatus(),
                    "Runner cleanup must preserve the successful application result");
            assertStatusFromApplicationCli(client.getClusterId().toString());
        }
    }

    /** Exercises the packaged CLI and its HOCON .config file against the real resource manager. */
    private void assertStatusFromApplicationCli(String applicationId) throws Exception {
        File output = new File(temporary, "application-status.log");
        ProcessBuilder builder =
                new ProcessBuilder(
                        "bash",
                        new File(distributionHome, "bin/seatunnel-application.sh")
                                .getAbsolutePath(),
                        "status",
                        "-t",
                        "yarn",
                        "--id",
                        applicationId,
                        "-a",
                        applicationConfig.toString(),
                        "-iyarn.staging-dir=/seatunnel-applications");
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        Process process = builder.redirectErrorStream(true).redirectOutput(output).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Application CLI did not finish");
            String text = new String(Files.readAllBytes(output.toPath()), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), text);
            assertTrue(text.contains("Status: SUCCEEDED"), text);
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void invalidJobReportsFailureAndReleasesWorkers() throws Exception {
        try (YarnApplicationClient client = deployApplication(specification(false, true, 120000))) {
            ApplicationStatus status = awaitTerminal(client);
            assertEquals(ApplicationStatus.FAILED, status, diagnostics(client));
            assertTrue(diagnostics(client).contains("NonexistentSink"), diagnostics(client));
            assertCleaned(client);
        }
    }

    @Test
    void cancelStopsTheRunningApplicationAndWorkers() throws Exception {
        String id;
        ApplicationSpecification specification = specification(true, false, 120000, 2);
        try (YarnApplicationClient client = deployApplication(specification)) {
            ContainerId worker = awaitWorker(client);
            String workerCommand =
                    yarn.getNodeManager(0)
                            .getNMContext()
                            .getContainers()
                            .get(worker)
                            .getLaunchContext()
                            .getCommands()
                            .get(0);
            assertTrue(
                    workerCommand.contains(
                            " '" + specification.getWorkerSpecification().getSlots() + "' "),
                    "The injected application specification must reach the worker launch command");
            assertTrue(
                    yarn.getNodeManager(0)
                            .getNMContext()
                            .getContainers()
                            .get(worker)
                            .getLaunchContext()
                            .getCommands()
                            .get(0)
                            .contains(
                                    SeatunnelApplicationConfig.clusterName(
                                            client.getClusterId().toString())));
            assertTrue(
                    yarn.getNodeManager(0)
                            .getNMContext()
                            .getContainers()
                            .get(worker)
                            .getLaunchContext()
                            .getCommands()
                            .get(0)
                            .contains(SeatunnelYarnWorkerCli.class.getName()));
            await().atMost(Duration.ofMinutes(3))
                    .untilAsserted(
                            () ->
                                    assertEquals(
                                            2,
                                            launchedWorkers(client.getClusterId().toString())
                                                    .size()));
            id = client.getClusterId().toString();
            Path staging = new Path("/seatunnel-applications/" + id);
            assertEquals(
                    (short) 0700,
                    hdfs.getFileSystem().getFileStatus(staging).getPermission().toShort());
            Set<String> stagedFiles = new HashSet<>();
            for (FileStatus file : hdfs.getFileSystem().listStatus(staging)) {
                stagedFiles.add(file.getPath().getName());
                assertTrue(file.isFile());
                assertTrue(file.getLen() > 0);
            }
            assertEquals(4, stagedFiles.size());
            assertTrue(stagedFiles.contains("distribution.tar.gz"));
            assertTrue(stagedFiles.contains("distribution.properties"));
            assertTrue(stagedFiles.contains("application.properties"));
            assertTrue(stagedFiles.contains("hadoop-conf.xml"));
        }
        try (YarnApplicationClient client = platformMonitor(id)) {
            assertEquals(ApplicationStatus.RUNNING, client.getStatus());
            assertNotNull(deployer.retrieve(ApplicationId.fromString(id)));
            cancelApplication(deployer, client.getClusterId().toString());
            assertEquals(ApplicationStatus.CANCELED, awaitTerminal(client));
            assertCleaned(client);
        }
    }

    @Test
    void workerLossFailsTheApplicationWithoutReplacement() throws Exception {
        try (YarnApplicationClient client = deployApplication(specification(true, false, 120000))) {
            ContainerId worker = awaitWorker(client);
            yarn.getNodeManager(0)
                    .getNMContext()
                    .getContainers()
                    .get(worker)
                    .handle(
                            new ContainerKillEvent(
                                    worker,
                                    ContainerExitStatus.KILLED_BY_APPMASTER,
                                    "Injected worker failure"));
            assertEquals(ApplicationStatus.FAILED, awaitTerminal(client));
            assertCleaned(client);
            assertEquals(
                    ApplicationStatus.FAILED,
                    client.getStatus(),
                    "Worker cleanup must preserve the original application failure");
        }
    }

    @Test
    void startupDeadlineFailsAndRemovesStagedConfiguration() throws Exception {
        assertThrows(
                TimeoutException.class, () -> deployApplication(specification(false, false, 1)));
        assertEquals(
                0, hdfs.getFileSystem().listStatus(new Path("/seatunnel-applications")).length);
        await().atMost(Duration.ofSeconds(60))
                .until(() -> yarn.getNodeManager(0).getNMContext().getContainers().isEmpty());
    }

    @Test
    void checkpointRestoresSourceProgressInANewApplication() throws Exception {
        Map<String, String> variables = new HashMap<>();
        variables.put("default_fs", hdfs.getFileSystem().getUri().toString());
        assertCheckpointRecovery(checkpointDistribution("hdfs", variables), this::latestCheckpoint);
    }

    private void assertCheckpointRecovery(File archive, CheckpointProbe checkpoints)
            throws Exception {
        long originalJobId = System.currentTimeMillis();
        long restoredJobId = originalJobId + 1;
        long checkpoint;
        try (YarnApplicationClient original =
                deployApplication(
                        checkpointSpecification(originalJobId, null), archive.getAbsolutePath())) {
            try {
                ContainerId worker = awaitWorker(original);
                // Wait beyond any snapshot that could have started before the first emitted row.
                awaitCheckpoint(
                        original,
                        originalJobId,
                        checkpoints.latest(originalJobId) + 1,
                        checkpoints);
                assertEquals(1, outputRows(original.getClusterId().toString()));
                yarn.getNodeManager(0)
                        .getNMContext()
                        .getContainers()
                        .get(worker)
                        .handle(
                                new ContainerKillEvent(
                                        worker,
                                        ContainerExitStatus.KILLED_BY_APPMASTER,
                                        "Fail application after a durable checkpoint"));
                assertEquals(ApplicationStatus.FAILED, awaitTerminal(original));
                assertCleaned(original);
                checkpoint = checkpoints.latest(originalJobId);
                assertTrue(checkpoint > 0, "Application cleanup deleted retained checkpoints");
            } finally {
                if (!original.getStatus().isTerminal()) {
                    cancelApplication(deployer, original.getClusterId().toString());
                }
            }
        }
        try (YarnApplicationClient restored =
                deployApplication(
                        checkpointSpecification(restoredJobId, originalJobId),
                        archive.getAbsolutePath())) {
            try {
                awaitCheckpoint(restored, restoredJobId, checkpoint, checkpoints);
                assertEquals(ApplicationStatus.RUNNING, restored.getStatus());
                assertTrue(
                        applicationLogContains(
                                restored.getClusterId().toString(),
                                "Restore checkpoint, job id: " + restoredJobId),
                        "Native engine did not restore checkpoint state in the new application");
                assertEquals(
                        0,
                        outputRows(restored.getClusterId().toString()),
                        "Restored FakeSource replayed rows already consumed before the checkpoint");
                cancelApplication(deployer, restored.getClusterId().toString());
                assertEquals(ApplicationStatus.CANCELED, awaitTerminal(restored));
                assertCleaned(restored);
                assertTrue(checkpoints.latest(originalJobId) > 0);
                assertTrue(checkpoints.latest(restoredJobId) > checkpoint);
            } finally {
                if (!restored.getStatus().isTerminal()) {
                    cancelApplication(deployer, restored.getClusterId().toString());
                }
            }
        }
    }

    private ApplicationSpecification checkpointSpecification(long jobId, Long restoreJobId)
            throws Exception {
        ApplicationSpecification base =
                specification(loadJobConfiguration("checkpoint_recovery.conf", 1), 120000, 1);
        return base.toBuilder().jobId(jobId).restoreJobId(restoreJobId).build();
    }

    private void awaitCheckpoint(
            YarnApplicationClient client, long jobId, long previous, CheckpointProbe checkpoints) {
        await().atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofMillis(250))
                .until(
                        () -> {
                            assertFalse(
                                    client.getStatus().isTerminal(),
                                    "Application terminated before persisting a checkpoint: "
                                            + diagnostics(client));
                            return checkpoints.latest(jobId) > previous;
                        });
    }

    private long latestCheckpoint(long jobId) throws Exception {
        Path directory = new Path("/seatunnel-checkpoints/" + jobId);
        if (!hdfs.getFileSystem().exists(directory)) {
            return 0;
        }
        long latest = 0;
        for (FileStatus file : hdfs.getFileSystem().listStatus(directory)) {
            String name = file.getPath().getName();
            if (file.isFile() && name.endsWith(".ser")) {
                latest = Math.max(latest, checkpointId(name));
            }
        }
        return latest;
    }

    private static long checkpointId(String name) {
        return Long.parseLong(name.substring(name.lastIndexOf('-') + 1, name.length() - 4));
    }

    @FunctionalInterface
    private interface CheckpointProbe {
        long latest(long jobId) throws Exception;
    }

    /** Replaces seatunnel.yaml with a backend-specific test resource. */
    private File checkpointDistribution(String backend, Map<String, String> variables)
            throws Exception {
        File home = new File(temporary, "seatunnel-" + backend);
        FileUtils.copyDirectory(distributionHome, home);
        String configuration =
                new String(
                        Files.readAllBytes(
                                ContainerUtil.getResourcesFile(
                                                "/yarn/seatunnel_" + backend + ".yaml")
                                        .toPath()),
                        StandardCharsets.UTF_8);
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            configuration =
                    configuration.replace("{{" + variable.getKey() + "}}", variable.getValue());
        }
        assertFalse(configuration.contains("{{"), "Unresolved engine configuration variable");
        Files.write(
                new File(home, "config/seatunnel.yaml").toPath(),
                configuration.getBytes(StandardCharsets.UTF_8));
        return archiveDistribution(home, "distribution-" + backend + ".tar.gz");
    }

    private ApplicationSpecification specification(boolean streaming, boolean invalid, long timeout)
            throws IOException {
        return specification(streaming, invalid, timeout, 1);
    }

    private ApplicationSpecification specification(
            boolean streaming, boolean invalid, long timeout, int workers) throws IOException {
        String template =
                invalid
                        ? "invalid_sink.conf"
                        : streaming ? "fake_streaming.conf" : "fake_batch.conf";
        return specification(loadJobConfiguration(template, workers), timeout, workers);
    }

    private String loadJobConfiguration(String template, int workers) throws IOException {
        String configuration =
                new String(
                                Files.readAllBytes(
                                        ContainerUtil.getResourcesFile("/common/" + template)
                                                .toPath()),
                                StandardCharsets.UTF_8)
                        .replace("{{parallelism}}", String.valueOf(workers))
                        .replace("{{row_count}}", String.valueOf(workers))
                        .replace("{{split_count}}", String.valueOf(workers))
                        .replace("{{marker}}", "APPLICATION_E2E_DATA");
        assertFalse(configuration.contains("{{"), "Unresolved job configuration variable");
        return configuration;
    }

    /** Exercises the same separate application/job inputs as the submit command. */
    private ApplicationSpecification specification(String job, long timeout, int workers)
            throws IOException {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("application.worker-count", String.valueOf(workers));
        overrides.put("application.startup-timeout-millis", String.valueOf(timeout));
        java.nio.file.Path jobConfig = Files.createTempFile(temporary.toPath(), "job-", ".config");
        Files.write(jobConfig, job.getBytes(StandardCharsets.UTF_8));
        return SeatunnelApplicationConfig.parse(
                jobConfig, SeatunnelApplicationConfig.load(applicationConfig, overrides));
    }

    private YarnApplicationClient deployApplication(ApplicationSpecification specification)
            throws Exception {
        return deployApplication(specification, distribution);
    }

    private YarnApplicationClient deployApplication(
            ApplicationSpecification specification, String archive) throws Exception {
        Map<String, String> deploymentOptions =
                SeatunnelApplicationConfig.load(
                        applicationConfig, Collections.singletonMap("yarn.distribution", archive));
        ApplicationId id =
                new ApplicationClusterDeployer(clientServiceLoader)
                        .run(DeployType.YARN, deploymentOptions, specification);
        return platformMonitor(id.toString());
    }

    private ApplicationStatus applicationStatus(
            ClusterDescriptor<ApplicationId> descriptor, String id) throws Exception {
        return descriptor.getApplicationStatus(ApplicationId.fromString(id));
    }

    private void cancelApplication(ClusterDescriptor<ApplicationId> descriptor, String id)
            throws Exception {
        descriptor.cancelApplication(ApplicationId.fromString(id));
    }

    private YarnApplicationClient platformMonitor(String id) {
        YarnClient client = YarnClient.createYarnClient();
        client.init(configuration);
        client.start();
        return new YarnApplicationClient(
                client,
                configuration,
                ApplicationId.fromString(id),
                new Path("/seatunnel-applications/" + id));
    }

    private String diagnostics(YarnApplicationClient client) throws Exception {
        try (YarnClient platformClient = YarnClient.createYarnClient()) {
            platformClient.init(configuration);
            platformClient.start();
            return platformClient.getApplicationReport(client.getClusterId()).getDiagnostics();
        }
    }

    private ApplicationStatus awaitTerminal(YarnApplicationClient client) {
        AtomicReference<ApplicationStatus> result = new AtomicReference<>();
        await().atMost(Duration.ofMinutes(5))
                .pollInterval(Duration.ofMillis(500))
                .until(
                        () -> {
                            result.set(client.getStatus());
                            if (!result.get().isTerminal()) {
                                return false;
                            }
                            assertEquals(
                                    result.get(),
                                    applicationStatus(deployer, client.getClusterId().toString()));
                            return true;
                        });
        return result.get();
    }

    private ContainerId awaitWorker(YarnApplicationClient client) {
        AtomicReference<ContainerId> worker = new AtomicReference<>();
        await().atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofMillis(250))
                .until(
                        () -> {
                            assertFalse(
                                    client.getStatus().isTerminal(),
                                    "Application terminated before launching a worker: "
                                            + diagnostics(client));
                            for (Map.Entry<ContainerId, Container> entry :
                                    yarn.getNodeManager(0)
                                            .getNMContext()
                                            .getContainers()
                                            .entrySet()) {
                                ContainerId id = entry.getKey();
                                if (id.getApplicationAttemptId()
                                                .getApplicationId()
                                                .toString()
                                                .equals(client.getClusterId().toString())
                                        && id.getContainerId() > 1
                                        && entry.getValue().isRunning()
                                        && outputRows(client.getClusterId().toString()) > 0) {
                                    worker.set(id);
                                    return true;
                                }
                            }
                            return false;
                        });
        return worker.get();
    }

    private Set<ContainerId> launchedWorkers(String applicationId) {
        Set<ContainerId> workers = new HashSet<>();
        for (String directory :
                yarn.getNodeManager(0)
                        .getConfig()
                        .getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)) {
            File[] containers = new File(directory, applicationId).listFiles();
            if (containers == null) {
                continue;
            }
            for (File container : containers) {
                if (container.getName().startsWith("container_")
                        && new File(container, "stdout").isFile()) {
                    ContainerId id = ContainerId.fromString(container.getName());
                    if (id.getContainerId() > 1) {
                        workers.add(id);
                    }
                }
            }
        }
        return workers;
    }

    private long outputRows(String applicationId) throws Exception {
        long rows = 0;
        for (String directory :
                yarn.getNodeManager(0)
                        .getConfig()
                        .getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)) {
            File[] containers = new File(directory, applicationId).listFiles();
            if (containers == null) {
                continue;
            }
            for (File container : containers) {
                File output = new File(container, "stdout");
                if (output.isFile()) {
                    rows +=
                            Files.readAllLines(output.toPath(), StandardCharsets.UTF_8).stream()
                                    .filter(
                                            line ->
                                                    line.contains("SeaTunnelRow#")
                                                            && line.contains(
                                                                    "APPLICATION_E2E_DATA"))
                                    .count();
                }
            }
        }
        return rows;
    }

    private boolean applicationLogContains(String applicationId, String marker) throws Exception {
        for (String directory :
                yarn.getNodeManager(0)
                        .getConfig()
                        .getTrimmedStrings(YarnConfiguration.NM_LOG_DIRS)) {
            File[] containers = new File(directory, applicationId).listFiles();
            if (containers == null) {
                continue;
            }
            for (File container : containers) {
                File output = new File(container, "stdout");
                if (output.isFile()
                        && new String(Files.readAllBytes(output.toPath()), StandardCharsets.UTF_8)
                                .contains(marker)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void assertCleaned(YarnApplicationClient client) throws Exception {
        assertFalse(
                hdfs.getFileSystem()
                        .exists(
                                new Path(
                                        "/seatunnel-applications/"
                                                + client.getClusterId().toString())));
        await().atMost(Duration.ofSeconds(60))
                .until(
                        () ->
                                yarn.getNodeManager(0).getNMContext().getContainers().keySet()
                                        .stream()
                                        .noneMatch(
                                                id ->
                                                        id.getApplicationAttemptId()
                                                                .getApplicationId()
                                                                .toString()
                                                                .equals(
                                                                        client.getClusterId()
                                                                                .toString())));
    }
}
