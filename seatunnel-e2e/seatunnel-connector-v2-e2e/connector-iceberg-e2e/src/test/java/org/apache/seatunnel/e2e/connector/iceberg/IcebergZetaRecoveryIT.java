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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergFilesCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;
import org.apache.seatunnel.e2e.connector.iceberg.recovery.IcebergZetaRecoverySource;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSource;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSourceFactory;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSourceReader;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSplit;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceSplitEnumerator;
import org.apache.seatunnel.e2e.source.checkpointable.CheckpointableSequenceState;

import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/** Single-node JVM crash and explicit checkpoint restore, not a savepoint or HA-failover test. */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class IcebergZetaRecoveryIT {

    private static final String RESOURCES = "/iceberg-zeta-recovery/";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    public void testV2CdcAfterServerJvmCrash() throws Exception {
        runRecovery(2);
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    public void testV3CdcAfterServerJvmCrash() throws Exception {
        runRecovery(3);
    }

    private void runRecovery(int version) throws Exception {
        Path root =
                Files.createTempDirectory(Paths.get("/tmp"), "iceberg-zeta-recovery-").toRealPath();
        try (RecoveryContainer runtime = new RecoveryContainer(root, version)) {
            runtime.startUp();
            String sourceJobId = Long.toString(JobIdGenerator.newJobId());
            runtime.submit(sourceJobId, null);
            runtime.awaitStatus(sourceJobId, "RUNNING");
            awaitCommittedRows(runtime, sourceJobId, Map.of(1L, "A0", 2L, "B0", 3L, "C0"));
            Assertions.assertEquals(
                    "0", Files.readString(root.resolve(IcebergZetaRecoverySource.FIRST_OFFSET)));

            Files.createFile(root.resolve(IcebergZetaRecoverySource.CDC_GATE));
            awaitCommittedRows(runtime, sourceJobId, Map.of(1L, "A1", 3L, "C0", 4L, "D0"));
            Table beforeCrash = loadTable(root);
            Map<Long, Long> checkpointsBefore = checkpointSnapshots(beforeCrash);
            if (version == 3) {
                assertDeletionVector(beforeCrash);
            }

            String oldPid = runtime.killServerJvm();
            Map<String, String> durableCheckpoints = checkpointDigests(root, sourceJobId);
            Assertions.assertFalse(
                    durableCheckpoints.isEmpty(), "Crash must leave a durable checkpoint");
            Files.delete(root.resolve(IcebergZetaRecoverySource.FIRST_OFFSET));

            runtime.startServerJvm();
            Assertions.assertNotEquals(oldPid, runtime.serverPid());
            Assertions.assertEquals(
                    durableCheckpoints,
                    checkpointDigests(root, sourceJobId),
                    "Server restart must not remove or rewrite the source job's checkpoint files");
            Assertions.assertTrue(
                    runtime.readJson(5801, "/hazelcast/rest/maps/running-jobs").isEmpty(),
                    "No automatic job recovery may race this explicit restore");

            String restoredJobId = Long.toString(JobIdGenerator.newJobId());
            runtime.submit(restoredJobId, sourceJobId);
            runtime.awaitStatus(restoredJobId, "RUNNING");
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Path cursor = root.resolve(IcebergZetaRecoverySource.FIRST_OFFSET);
                                Assertions.assertTrue(Files.isRegularFile(cursor));
                                Assertions.assertEquals(
                                        Long.toString(IcebergZetaRecoverySource.RESTORE_OFFSET),
                                        Files.readString(cursor),
                                        "The engine must assign the checkpointed split cursor, not start over");
                            });
            Assertions.assertEquals(
                    Map.of(1L, "A1", 3L, "C0", 4L, "D0"), readRows(loadTable(root)));
            Files.createFile(root.resolve(IcebergZetaRecoverySource.RESTORE_GATE));

            awaitCommittedRows(runtime, restoredJobId, Map.of(1L, "A1", 3L, "C1", 5L, "E0"));
            if (version == 3) {
                assertDeletionVector(loadTable(root));
            }
            Files.createFile(root.resolve(IcebergZetaRecoverySource.FINISH_GATE));

            // --async only detaches the submission CLI. A successful submission is not test
            // success.
            runtime.awaitStatus(restoredJobId, "FINISHED");
            Table afterRestore = loadTable(root);
            Assertions.assertEquals(Map.of(1L, "A1", 3L, "C1", 5L, "E0"), readRows(afterRestore));
            Assertions.assertEquals(
                    version,
                    ((HasTableOperations) afterRestore).operations().current().formatVersion());
            Map<Long, Long> checkpointsAfter = checkpointSnapshots(afterRestore);
            checkpointsBefore.forEach(
                    (checkpoint, snapshot) ->
                            Assertions.assertEquals(
                                    snapshot,
                                    checkpointsAfter.get(checkpoint),
                                    "Already committed checkpoint must retain its original snapshot ID"));
            Assertions.assertTrue(
                    checkpointsAfter.size() > checkpointsBefore.size(),
                    "Restore must produce a new committed snapshot, not only retain old snapshots");
            Assertions.assertTrue(
                    checkpointsAfter.keySet().stream()
                            .anyMatch(id -> id > Collections.max(checkpointsBefore.keySet())),
                    "Post-restore checkpoint IDs must advance");
            if (version == 3) {
                assertDeletionVector(afterRestore);
            }
            log.info(
                    "Zeta v{} JVM crash recovery: oldPid={}, newPid={}, sourceJob={}, restoredJob={}, before={}, after={}, checkpointFiles={}",
                    version,
                    oldPid,
                    runtime.serverPid(),
                    sourceJobId,
                    restoredJobId,
                    checkpointsBefore,
                    checkpointsAfter,
                    durableCheckpoints);
        }
    }

    private void awaitCommittedRows(
            RecoveryContainer runtime, String jobId, Map<Long, String> expected) {
        await().atMost(90, TimeUnit.SECONDS)
                .ignoreException(NoSuchTableException.class)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals("RUNNING", runtime.status(jobId));
                            Table table = loadTable(runtime.root);
                            Assertions.assertEquals(expected, readRows(table));
                            long checkpointId =
                                    Long.parseLong(
                                            table.currentSnapshot()
                                                    .summary()
                                                    .get(
                                                            IcebergFilesCommitter
                                                                    .SNAPSHOT_PROPERTY_CHECKPOINT_ID));
                            Assertions.assertTrue(runtime.completedCheckpoints(jobId) > 0);
                            try (Stream<Path> files =
                                    Files.list(
                                            runtime.root.resolve("checkpoints").resolve(jobId))) {
                                Assertions.assertTrue(
                                        files.anyMatch(
                                                path ->
                                                        path.getFileName()
                                                                        .toString()
                                                                        .endsWith(
                                                                                "-"
                                                                                        + checkpointId
                                                                                        + ".ser")
                                                                && path.toFile().length() > 0),
                                        "The externally committed checkpoint must also exist as a finalized storage file");
                            }
                        });
    }

    private static Table loadTable(Path root) throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put("type", "hadoop");
        props.put("warehouse", root.resolve("warehouse").toUri().toString());
        props.put("io-impl", HadoopStreamFileIO.class.getName());
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "zeta_recovery");
        config.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        config.put(IcebergCommonOptions.KEY_TABLE.key(), "zeta_recovery");
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), props);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(config)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private static Map<Long, String> readRows(Table table) throws IOException {
        Map<Long, String> rows = new HashMap<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                Long id = (Long) record.getField("id");
                Assertions.assertFalse(rows.containsKey(id), "Duplicate live primary key " + id);
                rows.put(id, (String) record.getField("name"));
            }
        }
        return rows;
    }

    private static Map<Long, Long> checkpointSnapshots(Table table) {
        Map<Long, Long> snapshots = new LinkedHashMap<>();
        for (Snapshot snapshot : table.snapshots()) {
            String value =
                    snapshot.summary().get(IcebergFilesCommitter.SNAPSHOT_PROPERTY_CHECKPOINT_ID);
            Assertions.assertNotNull(value, "Every test snapshot must identify its checkpoint");
            long checkpoint = Long.parseLong(value);
            Assertions.assertNull(
                    snapshots.put(checkpoint, snapshot.snapshotId()),
                    "Duplicate snapshot for checkpoint " + checkpoint);
        }
        Assertions.assertFalse(snapshots.isEmpty());
        return snapshots;
    }

    private static void assertDeletionVector(Table table) throws IOException {
        boolean found = false;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                for (org.apache.iceberg.DeleteFile file : task.deletes()) {
                    if (file.content() == FileContent.POSITION_DELETES) {
                        Assertions.assertEquals(FileFormat.PUFFIN, file.format());
                        Assertions.assertEquals(task.file().location(), file.referencedDataFile());
                        Assertions.assertTrue(
                                file.contentOffset() > 0 && file.contentSizeInBytes() > 0);
                        found = true;
                    }
                }
            }
        }
        Assertions.assertTrue(found, "v3 CDC must persist actual deletion vectors");
    }

    private static Map<String, String> checkpointDigests(Path root, String jobId) throws Exception {
        Map<String, String> files = new LinkedHashMap<>();
        try (Stream<Path> paths = Files.list(root.resolve("checkpoints").resolve(jobId))) {
            for (Path path :
                    paths.filter(p -> p.getFileName().toString().endsWith(".ser"))
                            .sorted()
                            .collect(Collectors.toList())) {
                byte[] digest =
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
                files.put(path.getFileName().toString(), Arrays.toString(digest));
            }
        }
        return files;
    }

    /**
     * Owns only a fresh test directory and network; never calls the destructive shared lifecycle.
     */
    private static final class RecoveryContainer extends SeaTunnelContainer
            implements AutoCloseable {

        private static final String SERVER_CLASS =
                "org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer";
        private final Path root;
        private final int version;
        private final Network network = Network.newNetwork();

        private RecoveryContainer(Path root, int version) {
            this.root = root;
            this.version = version;
        }

        @Override
        public void startUp() throws Exception {
            Path config = writeResource("job.conf", "iceberg-zeta-recovery.conf");
            Path yaml = writeResource("seatunnel.yaml", "iceberg-zeta-recovery-seatunnel.yaml");
            Path sourceJar = createSourceJar();
            Path mapping = root.resolve("iceberg-zeta-recovery-plugin-mapping.properties");
            String base =
                    Files.readString(
                            Paths.get(
                                    ContainerUtil.PROJECT_ROOT_PATH, "plugin-mapping.properties"));
            Files.writeString(
                    mapping,
                    base
                            + "\nseatunnel.source."
                            + IcebergZetaRecoverySource.NAME
                            + " = iceberg-zeta-recovery-source\n");
            Files.createDirectories(
                    root.resolve("warehouse/seatunnel_namespace/zeta_recovery/data"));
            Files.createDirectories(
                    root.resolve("warehouse/seatunnel_namespace/zeta_recovery/metadata"));
            Files.createDirectories(root.resolve("checkpoints"));

            // PID 1 stays alive when only SeaTunnelServer receives SIGKILL; mounts and files
            // survive.
            server =
                    new GenericContainer<>(getDockerImage())
                            .withNetwork(network)
                            .withNetworkAliases("server")
                            .withEnv("TZ", "UTC")
                            .withCommand("sh", "-c", "while :; do sleep 1; done")
                            .withExposedPorts(5801, 8080)
                            .withFileSystemBind(root.toString(), root.toString())
                            .waitingFor(
                                    Wait.forSuccessfulCommand(
                                            "test -f /tmp/seatunnel/bin/seatunnel-cluster.sh"));
            copySeaTunnelStarterToContainer(server);
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(
                            ContainerUtil.PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                    SEATUNNEL_HOME + "/config");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(yaml), SEATUNNEL_HOME + "/config/seatunnel.yaml");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(config),
                    SEATUNNEL_HOME + "/iceberg-zeta-recovery.conf");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(sourceJar),
                    SEATUNNEL_HOME + "/connectors/iceberg-zeta-recovery-source.jar");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(mapping),
                    SEATUNNEL_HOME + "/connectors/plugin-mapping.properties");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(MavenJarUtil.getHadoop3UberJarPath()),
                    CONTAINER_HADOOP_JAR_PATH.toString());
            List<File> jars =
                    ContainerUtil.getConnectorFiles(
                            new File(ContainerUtil.PROJECT_ROOT_PATH, "seatunnel-connectors-v2"),
                            Collections.singleton("connector-iceberg"),
                            "connector-");
            Assertions.assertEquals(
                    1, jars.size(), "Exactly one built Iceberg connector is required");
            server.withCopyFileToContainer(
                    MountableFile.forHostPath(jars.get(0).toPath()),
                    SEATUNNEL_HOME + "/connectors/" + jars.get(0).getName());
            DependencyJar.of(Zstd.class).addTo(server, SEATUNNEL_HOME + "/plugins/Iceberg/lib");
            applyJavaToolOptions(server);
            server.start();
            startServerJvm();
        }

        private Path writeResource(String resource, String name) throws IOException {
            Path target = root.resolve(name);
            try (InputStream in =
                    IcebergZetaRecoveryIT.class.getResourceAsStream(RESOURCES + resource)) {
                Assertions.assertNotNull(in, resource);
                String text =
                        new String(in.readAllBytes(), StandardCharsets.UTF_8)
                                .replace("@ROOT@", root.toString())
                                .replace("@VERSION@", Integer.toString(version));
                Files.writeString(target, text);
            }
            return target;
        }

        private void startServerJvm() throws Exception {
            Assertions.assertTrue(
                    serverPids().isEmpty(), "Never start two server JVMs on this warehouse");
            check(
                    server.execInContainer(
                            "bash", SEATUNNEL_HOME + "/bin/seatunnel-cluster.sh", "-d"));
            await().atMost(90, TimeUnit.SECONDS)
                    .ignoreException(IOException.class)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(1, serverPids().size());
                                check(
                                        server.execInContainer(
                                                "grep",
                                                "-q",
                                                "received new worker register:",
                                                SEATUNNEL_HOME + "/logs/seatunnel-server.out"));
                                Assertions.assertNotNull(
                                        readJson(5801, "/hazelcast/rest/maps/running-jobs"));
                            });
        }

        private String killServerJvm() throws Exception {
            String pid = serverPid();
            check(server.execInContainer("kill", "-9", pid));
            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            serverPids().isEmpty(),
                                            "Old JVM must be gone before restart"));
            Assertions.assertTrue(
                    server.isRunning(), "Crash must preserve the container and mounted storage");
            return pid;
        }

        private List<String> serverPids() throws Exception {
            Container.ExecResult result = server.execInContainer("jps", "-l");
            check(result);
            return Arrays.stream(result.getStdout().split("\n"))
                    .map(String::trim)
                    .filter(line -> line.endsWith(" " + SERVER_CLASS))
                    .map(line -> line.split("\\s+")[0])
                    .collect(Collectors.toList());
        }

        private String serverPid() throws Exception {
            List<String> pids = serverPids();
            Assertions.assertEquals(1, pids.size(), "Exactly one dedicated server JVM is required");
            Assertions.assertTrue(pids.get(0).matches("[0-9]+"));
            return pids.get(0);
        }

        private void submit(String jobId, String restoreSource) throws Exception {
            List<String> command =
                    new ArrayList<>(
                            Arrays.asList(
                                    "timeout",
                                    "90",
                                    "bash",
                                    SEATUNNEL_HOME + "/bin/seatunnel.sh",
                                    "--config",
                                    SEATUNNEL_HOME + "/iceberg-zeta-recovery.conf",
                                    "--set-job-id",
                                    jobId,
                                    "--async"));
            if (restoreSource != null) {
                command.add("--restore-with-checkpoint");
                command.add(restoreSource);
            }
            check(server.execInContainer(command.toArray(new String[0])));
        }

        private void awaitStatus(String jobId, String expected) {
            await().atMost(90, TimeUnit.SECONDS)
                    .ignoreException(IOException.class)
                    .untilAsserted(
                            () -> {
                                String actual = status(jobId);
                                if ("FAILED".equals(actual) || "CANCELED".equals(actual)) {
                                    throw new IllegalStateException(
                                            "Recovery job unexpectedly ended " + actual);
                                }
                                Assertions.assertEquals(expected, actual);
                            });
        }

        private String status(String jobId) throws IOException {
            return readJson(8080, "/job-info/" + jobId).path("jobStatus").asText();
        }

        private long completedCheckpoints(String jobId) throws IOException {
            JsonNode pipelines = readJson(8080, "/jobs/checkpoints/" + jobId).path("pipelines");
            return pipelines.path(0).path("counts").path("completed").asLong();
        }

        private JsonNode readJson(int port, String path) throws IOException {
            HttpURLConnection connection =
                    (HttpURLConnection)
                            new URL("http", server.getHost(), server.getMappedPort(port), path)
                                    .openConnection();
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);
            try {
                if (connection.getResponseCode() != 200) {
                    throw new IOException(
                            "Unexpected REST response "
                                    + connection.getResponseCode()
                                    + " for "
                                    + path);
                }
                try (InputStream in = connection.getInputStream()) {
                    return JSON.readTree(in);
                }
            } finally {
                connection.disconnect();
            }
        }

        private Path createSourceJar() throws IOException {
            Path jar = root.resolve("iceberg-zeta-recovery-source.jar");
            try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
                for (Class<?> type :
                        Arrays.asList(
                                IcebergZetaRecoverySource.class,
                                CheckpointableSequenceSource.class,
                                CheckpointableSequenceSourceFactory.class,
                                CheckpointableSequenceSourceReader.class,
                                CheckpointableSequenceSplit.class,
                                CheckpointableSequenceSplitEnumerator.class,
                                CheckpointableSequenceState.class)) {
                    addClass(out, type);
                }
                out.putNextEntry(new JarEntry("META-INF/services/" + Factory.class.getName()));
                out.write(
                        (IcebergZetaRecoverySource.RecoveryFactory.class.getName() + "\n")
                                .getBytes(StandardCharsets.UTF_8));
                out.closeEntry();
            }
            return jar;
        }

        private void addClass(JarOutputStream out, Class<?> type) throws IOException {
            String name = type.getName().replace('.', '/') + ".class";
            out.putNextEntry(new JarEntry(name));
            try (InputStream in = type.getClassLoader().getResourceAsStream(name)) {
                if (in == null) {
                    throw new IOException("Missing compiled test fixture " + name);
                }
                in.transferTo(out);
            }
            out.closeEntry();
            for (Class<?> nested : type.getDeclaredClasses()) {
                addClass(out, nested);
            }
        }

        private static void check(Container.ExecResult result) {
            Assertions.assertEquals(
                    0, result.getExitCode(), result.getStdout() + result.getStderr());
        }

        @Override
        public void close() throws Exception {
            try {
                if (server != null && server.isRunning()) {
                    if (!serverPids().isEmpty()) {
                        killServerJvm();
                    }
                    // Only this freshly created directory is removed, never the shared E2E mount.
                    check(
                            server.execInContainer(
                                    "find", root.toString(), "-mindepth", "1", "-delete"));
                }
            } finally {
                try {
                    if (server != null) {
                        server.stop();
                    }
                } finally {
                    network.close();
                    try (Stream<Path> paths = Files.walk(root)) {
                        for (Path path :
                                paths.sorted(Comparator.reverseOrder())
                                        .collect(Collectors.toList())) {
                            Files.deleteIfExists(path);
                        }
                    }
                }
            }
        }
    }
}
