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

import org.apache.seatunnel.shade.com.google.common.io.Closer;

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
import org.apache.seatunnel.resource.yarn.client.YarnApplicationClient;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.worker.SeatunnelYarnApplicationWorker;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.OutputStream;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executes packaged runtime artifacts in real NodeManager-launched JVMs backed by MiniDFSCluster.
 */
@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "The YARN tests use a Unix container environment")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class YarnApplicationIT extends TestSuiteBase {
    @TempDir static File temporary;
    private MiniDFSCluster hdfs;
    private MiniYARNCluster yarn;
    private Configuration configuration;
    private ClusterDescriptor<ApplicationId> deployer;
    private final ClusterClientServiceLoader clientServiceLoader = new ClusterClientServiceLoader();
    private File distributionHome;
    private java.nio.file.Path applicationConfig;

    @BeforeAll
    void startCluster() throws Exception {
        applicationConfig =
                ContainerUtil.getResourcesFile("/yarn/batch/application.config").toPath();
        Map<String, String> options =
                SeatunnelApplicationConfig.load(applicationConfig, Collections.emptyMap());
        prepareDistribution();
        Configuration hdfsConfiguration = new HdfsConfiguration();
        hdfsConfiguration.addResource(
                new Path(ContainerUtil.getResourcesFile("/yarn/mini-cluster.xml").toURI()));
        hdfsConfiguration.set(
                MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporary.toPath().resolve("hdfs").toString());
        hdfs = new MiniDFSCluster.Builder(hdfsConfiguration).numDataNodes(1).build();
        hdfs.waitActive();
        assertTrue(
                hdfs.getFileSystem().getConf().getBoolean("dfs.permissions.enabled", false),
                "YARN E2E must run with HDFS permissions enabled");
        YarnConfiguration yarnConfiguration = new YarnConfiguration(hdfsConfiguration);
        yarnConfiguration.set("fs.defaultFS", hdfs.getFileSystem().getUri().toString());
        yarn = new MiniYARNCluster("seatunnel-yarn-application", 1, 1, 1);
        yarn.init(yarnConfiguration);
        yarn.start();
        assertTrue(yarn.waitForNodeManagersToConnect(30000), "NodeManager did not register");
        configuration = new YarnConfiguration(yarn.getConfig());
        configuration.set("fs.defaultFS", hdfs.getFileSystem().getUri().toString());
        // MiniYARN's StaticMapping lives only in a Hadoop test jar, unavailable to real containers.
        configuration.set(
                "net.topology.node.switch.mapping.impl", ScriptBasedMapping.class.getName());
        File hadoopDirectory = new File(options.get("yarn.config-dir"));
        Files.createDirectories(hadoopDirectory.toPath());
        Closer closer = Closer.create();
        try {
            OutputStream output =
                    closer.register(
                            Files.newOutputStream(
                                    new File(hadoopDirectory, "core-site.xml").toPath()));
            configuration.writeXml(output);
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
        deployer =
                clientServiceLoader
                        .<ApplicationId>getClusterClientFactory(DeployType.YARN)
                        .create(options);
    }

    /** Prepares only the native layout needed for real YARN archive localization. */
    private void prepareDistribution() throws Exception {
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
        archiveDistribution(distributionHome, "distribution.tar.gz");
    }

    /** Creates a YARN-localizable archive from an exploded SeaTunnel distribution. */
    private void archiveDistribution(File home, String archiveName) throws Exception {
        File archive = new File("target/yarn-application-e2e", archiveName);
        Files.createDirectories(archive.toPath().getParent());
        Closer closer = Closer.create();
        try {
            TarArchiveOutputStream output =
                    closer.register(
                            new TarArchiveOutputStream(
                                    new GZIPOutputStream(Files.newOutputStream(archive.toPath()))));
            Stream<File> files = Files.walk(home.toPath()).map(path -> path.toFile());
            closer.register(files::close);
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
        } catch (Throwable failure) {
            throw closer.rethrow(failure, Exception.class);
        } finally {
            closer.close();
        }
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void batchRunsInAllocatedWorkersAndCleansHdfsArtifacts(boolean useHdfsDistribution)
            throws Exception {
        Path release = hdfs.getFileSystem().makeQualified(new Path("/releases/seatunnel.tgz"));
        YarnApplicationClient client =
                useHdfsDistribution ? submitFromHdfs(release) : deployApplication("batch");
        try {
            String id = client.getClusterId().toString();
            await().atMost(Duration.ofMinutes(5))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> rawTerminal(id));
            // The AM must remove staging itself; this client has not yet called getStatus().
            await().atMost(Duration.ofSeconds(30))
                    .until(
                            () ->
                                    !hdfs.getFileSystem()
                                            .exists(new Path("/seatunnel-applications/" + id)));
            ApplicationStatus status = client.getStatus();
            assertEquals(ApplicationStatus.SUCCEEDED, status, diagnostics(client));
            assertEquals(
                    2,
                    launchedWorkers(id).size(),
                    "The successful batch must launch two distinct worker JVMs");
            assertEquals(
                    4,
                    outputRows(id),
                    "Both parallel readers must emit both splits to the Console sink");
            assertCleaned(client);
            if (useHdfsDistribution) {
                assertTrue(applicationLogContains(id, "[west $HOME/common/master]"));
                assertTrue(applicationLogContains(id, "[west $HOME/common/worker]"));
                assertTrue(
                        hdfs.getFileSystem().isFile(release),
                        "Cleanup must preserve the source archive");
            }
            assertEquals(
                    ApplicationStatus.SUCCEEDED,
                    client.getStatus(),
                    "Runner cleanup must preserve the successful application result");
            assertTrue(
                    applicationCli("status", "--id", id, "-a", applicationConfig.toString())
                            .contains("Status: SUCCEEDED"));
        } finally {
            client.close();
        }
    }

    @Test
    void masterPublishesItsConfiguredHttpEndpoint() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        ContainerUtil.getResourcesFile("/yarn/cancel/application.config").toPath(),
                        Collections.singletonMap(
                                YarnOptions.MASTER_HTTP_PORT.key(), Integer.toString(port)));
        ApplicationId id =
                new ApplicationClusterDeployer(
                                clientServiceLoader,
                                DeployType.YARN,
                                specification("cancel"),
                                options)
                        .run();
        try (YarnApplicationClient client = platformMonitor(id.toString())) {
            try {
                awaitWorker(client);
                URI direct = URI.create(applicationReport(id).getOriginalTrackingUrl());
                assertEquals("http", direct.getScheme());
                assertEquals(port, direct.getPort());
                HttpClient http =
                        HttpClient.newBuilder()
                                .proxy(ProxySelector.of(null))
                                .connectTimeout(Duration.ofSeconds(10))
                                .build();
                assertTrue(getHttp(http, direct.resolve("overview")).contains("\"workers\""));
                // MiniYARN allocates the RM web port after constructing the application reports.
                String rmAddress =
                        yarn.getResourceManager()
                                .getConfig()
                                .get(YarnConfiguration.RM_WEBAPP_ADDRESS);
                URI proxy = URI.create("http://" + rmAddress + "/proxy/" + id + "/");
                assertTrue(getHttp(http, proxy.resolve("overview")).contains("\"workers\""));
                // Distributions built with skip.ui still serve the REST API.
                File starterFile = new File(distributionHome, "starter/seatunnel-starter.jar");
                try (JarFile starter = new JarFile(starterFile)) {
                    if (starter.getJarEntry("ui/index.html") != null) {
                        String html = getHttp(http, direct);
                        assertTrue(html.contains("<div id=\"app\">"));
                        assertEquals(html, getHttp(http, proxy));
                        Matcher script = Pattern.compile("src=\"([^\"]+\\.js)\"").matcher(html);
                        assertTrue(script.find(), html);
                        assertFalse(getHttp(http, direct.resolve(script.group(1))).isEmpty());
                    }
                }
            } finally {
                cancelApplication(deployer, id.toString());
            }
            assertEquals(ApplicationStatus.CANCELED, awaitTerminal(client));
        }
    }

    private String getHttp(HttpClient http, URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), () -> uri + " returned " + response.body());
        return response.body();
    }

    @Test
    void masterAndWorkersLocalizeTheSameHdfsSnapshot() throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        ContainerUtil.getResourcesFile("/yarn/cancel/application.config").toPath(),
                        Collections.emptyMap());
        DistributedFileSystem fs = hdfs.getFileSystem();
        Path releases = fs.makeQualified(new Path("/snapshot-releases"));
        Path release = new Path(releases, "seatunnel.tgz");
        fs.mkdirs(releases);
        fs.copyFromLocalFile(
                new Path(new File(options.get(YarnOptions.DISTRIBUTION.key())).toURI()), release);
        fs.allowSnapshot(releases);
        Path archive =
                fs.makeQualified(new Path(fs.createSnapshot(releases, "v1"), release.getName()));
        FileStatus snapshot = fs.getFileStatus(archive);
        assertTrue(fs.delete(release, false));
        options.put(YarnOptions.DISTRIBUTION.key(), archive.toString());

        ApplicationClusterDeployer submitter =
                new ApplicationClusterDeployer(
                        clientServiceLoader, DeployType.YARN, specification("cancel"), options);
        ApplicationId id = submitter.run();
        try (YarnApplicationClient client = platformMonitor(id.toString())) {
            try {
                ContainerId workerId = awaitWorker(client);
                LocalResource master =
                        yarn.getResourceManager()
                                .getRMContext()
                                .getRMApps()
                                .get(id)
                                .getApplicationSubmissionContext()
                                .getAMContainerSpec()
                                .getLocalResources()
                                .get(YarnConstants.LOCALIZED_DISTRIBUTION_NAME);
                LocalResource worker =
                        yarn.getNodeManager(0)
                                .getNMContext()
                                .getContainers()
                                .get(workerId)
                                .getLaunchContext()
                                .getLocalResources()
                                .get(YarnConstants.LOCALIZED_DISTRIBUTION_NAME);
                assertEquals(archive, ConverterUtils.getPathFromYarnURL(master.getResource()));
                assertEquals(LocalResourceType.ARCHIVE, master.getType());
                assertEquals(snapshot.getLen(), master.getSize());
                assertEquals(snapshot.getModificationTime(), master.getTimestamp());
                assertEquals(master, worker);
                Path staging =
                        new Path(options.get(YarnOptions.STAGING_DIRECTORY.key()), id.toString());
                assertTrue(fs.isFile(new Path(staging, YarnConstants.DISTRIBUTION_MANIFEST)));
                assertFalse(fs.exists(new Path(staging, "distribution.tar.gz")));
            } finally {
                cancelApplication(deployer, id.toString());
            }
            assertEquals(ApplicationStatus.CANCELED, awaitTerminal(client));
            assertCleaned(client);
            assertTrue(fs.isFile(archive));
        }
    }

    @Test
    void distributionTemplateRunsInlineJobWithOnlyApplicationConfig() throws Exception {
        String template =
                Files.readString(
                        new File(distributionHome, "config/v1.yarn.conf.template").toPath());
        Map<String, String> options =
                SeatunnelApplicationConfig.load(applicationConfig, Collections.emptyMap());
        File archive = new File(options.get(YarnOptions.DISTRIBUTION.key()));
        java.nio.file.Path deployment =
                Files.writeString(
                        temporary.toPath().resolve("yarn-demo.conf"),
                        template.replace(
                                "/path/to/apache-seatunnel-<version>-bin.tar.gz",
                                archive.toURI().toString()));
        String output =
                applicationCli(
                        distributionHome,
                        Collections.emptyMap(),
                        "submit",
                        "-a",
                        deployment.toString(),
                        "--wait");
        assertTrue(output.contains("Status: SUCCEEDED"), output);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cliLocalizesNamedResourcesAsTheEffectiveSimpleUser(boolean explicitUser) throws Exception {
        File deployment =
                ContainerUtil.getResourcesFile("/yarn/local-resources/application.config");
        String gatewayUser = "gateway-etl";
        String owner = explicitUser ? "orders-runner" : gatewayUser;
        Map<String, String> gateway = Map.of(YarnConstants.HADOOP_USER_NAME_ENV, gatewayUser);
        DistributedFileSystem fs = hdfs.getFileSystem();
        Path root = new Path("/staging-" + owner);
        fs.mkdirs(root, new FsPermission((short) 0700));
        fs.setOwner(root, owner, null);
        List<String> arguments =
                new ArrayList<>(
                        List.of(
                                "-a",
                                deployment.getAbsolutePath(),
                                "-Dyarn.staging-dir=" + root,
                                "--wait"));
        if (explicitUser) {
            arguments.add("-Dyarn.hadoop-user-name=" + owner);
        }
        String output = applicationCli(null, gateway, "submit", arguments.toArray(new String[0]));
        assertTrue(output.contains("Status: SUCCEEDED"), output);
        ApplicationId id = submittedId(output);
        assertEquals(owner, applicationReport(id).getUser());
        assertEquals(2, launchedWorkers(id.toString()).size());

        // A caller can see the RM report without permission to remove this application's files.
        Path leftover = new Path(root, id.toString());
        assertFalse(fs.exists(leftover));
        fs.mkdirs(leftover);
        arguments =
                new ArrayList<>(
                        List.of("--id", id.toString(), "-Dyarn.staging-dir=" + root, "--wait"));
        Map<String, String> observer =
                Map.of(YarnConstants.HADOOP_USER_NAME_ENV, "status-observer");
        String status = applicationCli(null, observer, "status", arguments.toArray(new String[0]));
        assertTrue(status.contains("Status: SUCCEEDED"), status);
        assertTrue(fs.exists(leftover));
        if (explicitUser) {
            arguments.add("-Dyarn.hadoop-user-name=" + owner);
        }
        status = applicationCli(null, gateway, "status", arguments.toArray(new String[0]));
        assertTrue(status.contains("Status: SUCCEEDED"), status);
        assertFalse(fs.exists(leftover));
    }

    private YarnApplicationClient submitFromHdfs(Path release) throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(applicationConfig, Collections.emptyMap());
        File local = new File(options.get(YarnOptions.DISTRIBUTION.key()));
        hdfs.getFileSystem().mkdirs(release.getParent());
        hdfs.getFileSystem().copyFromLocalFile(new Path(local.toURI()), release);
        Path job = new Path(release.getParent(), "batch.conf");
        hdfs.getFileSystem()
                .copyFromLocalFile(
                        new Path(ContainerUtil.getResourcesFile("/yarn/batch/job.conf").toURI()),
                        job);
        Path engine = new Path(release.getParent(), "engine.yaml");
        hdfs.getFileSystem()
                .copyFromLocalFile(
                        new Path(ContainerUtil.getResourcesFile("/yarn/seatunnel.yaml").toURI()),
                        engine);
        java.nio.file.Path log = temporary.toPath().resolve("logging.properties");
        String pattern = "layout.pattern = ";
        Files.writeString(
                log,
                Files.readString(
                                new File(distributionHome, "config/log4j2_client.properties")
                                        .toPath())
                        .replace(
                                pattern,
                                pattern
                                        + "[${env:YARN_TEST_REGION}/${sys:yarn.test.common}/${sys:yarn.test.role}] "));
        java.nio.file.Path common =
                Files.writeString(
                        temporary.toPath().resolve("common.opts"), "-Dyarn.test.common=common");
        java.nio.file.Path master =
                Files.writeString(
                        temporary.toPath().resolve("master.opts"), "-Dyarn.test.role=master");
        String output =
                applicationCli(
                        "submit",
                        "-Dapplication.worker-count=2",
                        "-Dapplication.worker.slots=4",
                        "-Dyarn.staging-dir=/seatunnel-applications",
                        "-Dyarn.distribution=" + release,
                        "-Dyarn.job-config-file=" + job,
                        "-Dyarn.local-resources.runtime.source-dir=" + release.getParent(),
                        "-Dyarn.local-resources.runtime.localize.engine.yaml=files/engine.yaml",
                        "-Dyarn.seatunnel-config-file=files/engine.yaml",
                        "-Dyarn.ship-files.logging.properties=" + log,
                        "-Dyarn.log4j-config-file=files/logging.properties",
                        "-Dyarn.ship-files.common.opts=" + common,
                        "-Dyarn.jvm-options-file=files/common.opts",
                        "-Dyarn.ship-files.master.opts=" + master,
                        "-Dyarn.master.jvm-options-file=files/master.opts",
                        "-Dyarn.local-resources.worker.content.worker.opts=-Dyarn.test.role=worker",
                        "-Dyarn.worker.jvm-options-file=files/worker.opts",
                        "-Dyarn.master.hazelcast-config=hazelcast-master.yaml",
                        "-Dyarn.worker.hazelcast-config=hazelcast-worker.yaml",
                        "-Dyarn.env.YARN_TEST_REGION=west $HOME",
                        "-Dyarn.active-deadline-seconds=300",
                        "-Dyarn.kerberos.keytab=/unused-in-simple-mode.keytab");
        ApplicationId id = submittedId(output);
        ApplicationReport report = applicationReport(id);
        assertNotNull(report.getApplicationTimeouts().get(ApplicationTimeoutType.LIFETIME));
        assertNotEquals(
                "UNLIMITED",
                report.getApplicationTimeouts()
                        .get(ApplicationTimeoutType.LIFETIME)
                        .getExpiryTime());
        return platformMonitor(id.toString());
    }

    private ApplicationId submittedId(String output) {
        String prefix = "Application ID: ";
        return output.lines()
                .filter(line -> line.startsWith(prefix))
                .map(line -> ApplicationId.fromString(line.substring(prefix.length()).trim()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(output));
    }

    /** Exercises the packaged CLI against the real ResourceManager. */
    private String applicationCli(String operation, String... arguments) throws Exception {
        return applicationCli(null, Collections.emptyMap(), operation, arguments);
    }

    private String applicationCli(
            File directory, Map<String, String> environment, String operation, String... arguments)
            throws Exception {
        File output = new File(temporary, "application-" + operation + ".log");
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                "bash",
                                new File(distributionHome, "bin/seatunnel-application.sh")
                                        .getAbsolutePath(),
                                operation,
                                "-t",
                                "yarn"));
        command.addAll(Arrays.asList(arguments));
        ProcessBuilder builder = new ProcessBuilder(command).directory(directory);
        builder.environment().putAll(environment);
        builder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        builder.environment()
                .put(
                        YarnConstants.HADOOP_CONF_DIR_ENV,
                        new File(
                                        SeatunnelApplicationConfig.load(
                                                        applicationConfig, Collections.emptyMap())
                                                .get(YarnOptions.CONFIG_DIRECTORY.key()))
                                .getAbsolutePath());
        Process process = builder.redirectErrorStream(true).redirectOutput(output).start();
        try {
            assertTrue(process.waitFor(2, TimeUnit.MINUTES), "Application CLI did not finish");
            String text = new String(Files.readAllBytes(output.toPath()), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), text);
            return text;
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void invalidJobReportsFailureAndReleasesWorkers() throws Exception {
        try (YarnApplicationClient client = deployApplication("invalid-job")) {
            ApplicationStatus status = awaitTerminal(client);
            assertEquals(ApplicationStatus.FAILED, status, diagnostics(client));
            assertTrue(diagnostics(client).contains("NonexistentSink"), diagnostics(client));
            assertCleaned(client);
        }
    }

    @Test
    void cancelStopsTheRunningApplicationAndWorkers() throws Exception {
        String id;
        ApplicationSpecification specification = specification("cancel");
        try (YarnApplicationClient client = deployApplication(specification, "cancel")) {
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
                            .contains(SeatunnelYarnApplicationWorker.class.getName()));
            assertTrue(workerCommand.contains("-Dhazelcast.logging.type=log4j2"));
            assertTrue(workerCommand.contains("-Dlog4j2.configurationFile="));
            assertTrue(workerCommand.contains("-Dseatunnel.logs.path="));
            assertEquals(
                    UserGroupInformation.getCurrentUser().getShortUserName(),
                    yarn.getNodeManager(0)
                            .getNMContext()
                            .getContainers()
                            .get(worker)
                            .getLaunchContext()
                            .getEnvironment()
                            .get(YarnConstants.HADOOP_USER_NAME_ENV));
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
        try (YarnApplicationClient client = deployApplication("worker-loss")) {
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
            String failure = diagnostics(client);
            assertTrue(failure.contains(worker.toString()), failure);
            assertTrue(failure.contains("Injected worker failure"), failure);
            assertCleaned(client);
            assertEquals(
                    ApplicationStatus.FAILED,
                    client.getStatus(),
                    "Worker cleanup must preserve the original application failure");
        }
    }

    @Test
    void startupDeadlineFailsAndRemovesStagedConfiguration() throws Exception {
        assertThrows(TimeoutException.class, () -> deployApplication("startup-timeout"));
        assertEquals(
                0, hdfs.getFileSystem().listStatus(new Path("/seatunnel-applications")).length);
        // Canceling an AM during localization can interrupt Hadoop's LocalizerRunner.
        await().ignoreExceptionsMatching(
                        error ->
                                error instanceof YarnRuntimeException
                                        && error.getCause() instanceof InterruptedException)
                .atMost(Duration.ofSeconds(60))
                .until(() -> yarn.getNodeManager(0).getNMContext().getContainers().isEmpty());
    }

    @Test
    void checkpointRestoresSourceProgressInANewApplication() throws Exception {
        prepareCheckpointDistribution();
        ApplicationSpecification originalSpecification = specification("checkpoint");
        ApplicationSpecification restoredSpecification = specification("checkpoint-restore");
        long originalJobId = originalSpecification.getJobId();
        long restoredJobId = restoredSpecification.getJobId();
        long checkpoint;
        YarnApplicationClient original = deployApplication(originalSpecification, "checkpoint");
        try {
            try {
                ContainerId worker = awaitWorker(original);
                // Wait beyond any snapshot that could have started before the first emitted row.
                awaitCheckpoint(original, originalJobId, latestCheckpoint(originalJobId) + 1);
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
                checkpoint = latestCheckpoint(originalJobId);
                assertTrue(checkpoint > 0, "Application cleanup deleted retained checkpoints");
            } finally {
                if (!original.getStatus().isTerminal()) {
                    cancelApplication(deployer, original.getClusterId().toString());
                }
            }
        } finally {
            original.close();
        }
        YarnApplicationClient restored =
                deployApplication(restoredSpecification, "checkpoint-restore");
        try {
            try {
                awaitCheckpoint(restored, restoredJobId, checkpoint);
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
                assertTrue(latestCheckpoint(originalJobId) > 0);
                assertTrue(latestCheckpoint(restoredJobId) > checkpoint);
            } finally {
                if (!restored.getStatus().isTerminal()) {
                    cancelApplication(deployer, restored.getClusterId().toString());
                }
            }
        } finally {
            restored.close();
        }
    }

    private void awaitCheckpoint(YarnApplicationClient client, long jobId, long previous) {
        await().atMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofMillis(250))
                .until(
                        () -> {
                            assertFalse(
                                    client.getStatus().isTerminal(),
                                    "Application terminated before persisting a checkpoint: "
                                            + diagnostics(client));
                            return latestCheckpoint(jobId) > previous;
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

    /** Only the MiniDFS address is dynamic; checkpoint settings live in seatunnel_hdfs.yaml. */
    private void prepareCheckpointDistribution() throws Exception {
        File home = new File(temporary, "seatunnel-hdfs");
        FileUtils.copyDirectory(distributionHome, home);
        String engineConfiguration =
                new String(
                                Files.readAllBytes(
                                        ContainerUtil.getResourcesFile("/yarn/seatunnel_hdfs.yaml")
                                                .toPath()),
                                StandardCharsets.UTF_8)
                        .replace("{{default_fs}}", hdfs.getFileSystem().getUri().toString());
        Files.write(
                new File(home, "config/seatunnel.yaml").toPath(),
                engineConfiguration.getBytes(StandardCharsets.UTF_8));
        archiveDistribution(home, "distribution-hdfs.tar.gz");
    }

    /** Each scenario has complete application and job files; no options are rewritten here. */
    private ApplicationSpecification specification(String scenario) {
        return SeatunnelApplicationConfig.parse(
                ContainerUtil.getResourcesFile("/yarn/" + scenario + "/job.conf").toPath(),
                SeatunnelApplicationConfig.load(
                        ContainerUtil.getResourcesFile("/yarn/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap()));
    }

    private YarnApplicationClient deployApplication(String scenario) throws Exception {
        return deployApplication(specification(scenario), scenario);
    }

    private YarnApplicationClient deployApplication(
            ApplicationSpecification specification, String scenario) throws Exception {
        Map<String, String> options =
                SeatunnelApplicationConfig.load(
                        ContainerUtil.getResourcesFile("/yarn/" + scenario + "/application.config")
                                .toPath(),
                        Collections.emptyMap());
        ApplicationClusterDeployer deployer =
                new ApplicationClusterDeployer(
                        clientServiceLoader, DeployType.YARN, specification, options);
        ApplicationId id = deployer.run();
        return platformMonitor(id.toString());
    }

    private ApplicationStatus applicationStatus(
            ClusterDescriptor<ApplicationId> descriptor, String id) throws Exception {
        return descriptor.getApplicationStatus(ApplicationId.fromString(id));
    }

    private boolean rawTerminal(String id) throws Exception {
        YarnApplicationState state =
                applicationReport(ApplicationId.fromString(id)).getYarnApplicationState();
        return state == YarnApplicationState.FINISHED
                || state == YarnApplicationState.FAILED
                || state == YarnApplicationState.KILLED;
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
        return applicationReport(client.getClusterId()).getDiagnostics();
    }

    private ApplicationReport applicationReport(ApplicationId id) throws Exception {
        try (YarnClient platformClient = YarnClient.createYarnClient()) {
            platformClient.init(configuration);
            platformClient.start();
            return platformClient.getApplicationReport(id);
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
