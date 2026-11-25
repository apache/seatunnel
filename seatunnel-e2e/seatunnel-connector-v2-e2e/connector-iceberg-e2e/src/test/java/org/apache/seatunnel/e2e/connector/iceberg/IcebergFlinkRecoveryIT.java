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
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.HadoopStreamFileIO;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.connector.iceberg.recovery.IcebergFlinkRecoverySinkFactory;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import com.github.dockerjava.api.exception.DockerException;
import com.github.luben.zstd.Zstd;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import static org.awaitility.Awaitility.await;

/** Native checkpoint replay after a JVM crash on either side of external publication. */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
@DisabledOnContainer(
        value = {
            TestContainerId.FLINK_1_13,
            TestContainerId.FLINK_1_14,
            TestContainerId.FLINK_1_15,
            TestContainerId.FLINK_1_16,
            TestContainerId.FLINK_1_17
        },
        type = {EngineType.SEATUNNEL, EngineType.SPARK},
        disabledReason = "Exercises native Flink 1.18 and 1.20 checkpoint recovery")
public class IcebergFlinkRecoveryIT extends TestSuiteBase {

    private static final String RESOURCE = "/iceberg/flink_recovery_cdc.conf";
    private static final String TABLE = "flink_recovery_cdc";
    private static final String NAMESPACE = "seatunnel_namespace";
    private static final String TASK_MANAGER_CLASS =
            "org.apache.flink.runtime.taskexecutor.TaskManagerRunner";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TestTemplate
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testV2TaskManagerCrashRecovery(TestContainer container) throws Exception {
        verifyRecovery((AbstractTestFlinkContainer) container, 2, "after");
    }

    @TestTemplate
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testV3TaskManagerCrashRecovery(TestContainer container) throws Exception {
        verifyRecovery((AbstractTestFlinkContainer) container, 3, "after");
    }

    @TestTemplate
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testV2CrashBeforePublication(TestContainer container) throws Exception {
        verifyRecovery((AbstractTestFlinkContainer) container, 2, "before");
    }

    @TestTemplate
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    public void testV3CrashBeforePublication(TestContainer container) throws Exception {
        verifyRecovery((AbstractTestFlinkContainer) container, 3, "before");
    }

    private void verifyRecovery(AbstractTestFlinkContainer flink, int version, String boundary)
            throws Exception {
        String root = "/tmp/seatunnel_mnt/iceberg/flink-recovery-" + UUID.randomUUID();
        String warehouse = root + "/warehouse";
        String tableDirectory = warehouse + "/" + NAMESPACE + "/" + TABLE;
        Path fixture = Files.createTempDirectory("iceberg-flink-recovery-plugin-");
        Path mapping = fixture.resolve("plugin-mapping.properties");
        List<File> connectors =
                ContainerUtil.getConnectorFiles(
                        new File(ContainerUtil.PROJECT_ROOT_PATH, "seatunnel-connectors-v2"),
                        Collections.singleton("connector-iceberg"),
                        "connector-");
        Assertions.assertEquals(
                1, connectors.size(), "Exactly one built Iceberg connector is required");
        Path jar = createSinkJar(fixture, connectors.get(0).toPath());
        Files.writeString(
                mapping,
                Files.readString(
                                Paths.get(
                                        ContainerUtil.PROJECT_ROOT_PATH,
                                        "plugin-mapping.properties"))
                        + "\nseatunnel.sink."
                        + IcebergFlinkRecoverySinkFactory.NAME
                        + " = connector-iceberg\n");
        AtomicReference<GenericContainer<?>> taskManagerRef = new AtomicReference<>();
        flink.executeExtraCommands(
                c -> {
                    assertExec(
                            c.execInContainer(
                                    "sh",
                                    "-c",
                                    "mkdir -p "
                                            + tableDirectory
                                            + "/data "
                                            + tableDirectory
                                            + "/metadata "
                                            + root
                                            + "/checkpoints && chmod -R 777 "
                                            + root));
                    DependencyJar.of(Zstd.class).copyTo(c, "/tmp/seatunnel/plugins/Iceberg/lib");
                    c.copyFileToContainer(
                            MountableFile.forHostPath(jar),
                            "/tmp/seatunnel/connectors/" + connectors.get(0).getName());
                    c.copyFileToContainer(
                            MountableFile.forHostPath(mapping),
                            "/tmp/seatunnel/connectors/plugin-mapping.properties");
                    if (c.getNetworkAliases().contains("taskmanager")) {
                        taskManagerRef.set(c);
                    }
                });
        Files.writeString(Paths.get(root, "boundary"), boundary);
        GenericContainer<?> taskManager = taskManagerRef.get();
        Assertions.assertNotNull(taskManager, "TaskManager container must be accessible");
        FlinkRest rest = new FlinkRest(flink);
        Set<String> previousJobs = rest.jobIds();
        int initialLogLength = taskManager.getLogs().length();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Container.ExecResult> submission =
                executor.submit(
                        () ->
                                flink.executeJob(
                                        RESOURCE,
                                        Arrays.asList(
                                                "flink_recovery_warehouse=file://" + warehouse,
                                                "flink_recovery_checkpoints=file://"
                                                        + root
                                                        + "/checkpoints",
                                                "flink_recovery_version=" + version)));
        AtomicReference<String> jobIdRef = new AtomicReference<>();
        Throwable primaryFailure = null;
        try {
            // The pinned amd64 engines may run under emulation, including the client JVM.
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(180, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                Set<String> jobs = rest.jobIds();
                                jobs.removeAll(previousJobs);
                                Assertions.assertEquals(
                                        1, jobs.size(), "Exactly one submitted job");
                                jobIdRef.set(jobs.iterator().next());
                            });
            String jobId = jobIdRef.get();
            // Client/plugin initialization is not task deployment. Give each bounded phase its
            // own wait, so a slow submission cannot cancel an otherwise healthy deployment.
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(90, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                rest.assertRunning(jobId);
                            });
            // FakeSource logs this only after collecting the complete literal script under its
            // checkpoint lock. Streaming mode then idles; it does not generate another split.
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(60, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                rest.assertRunning(jobId);
                                String logs = taskManager.getLogs();
                                Assertions.assertTrue(
                                        logs.substring(Math.min(initialLogLength, logs.length()))
                                                .contains(
                                                        "8 rows of custom data have been generated"
                                                                + " in the last batch"),
                                        "The entire CDC script must precede the first checkpoint");
                            });
            long firstCheckpoint = rest.completeCheckpoint(jobId, 0);
            // Only the legacy global-committer bridge needs a second completion notification
            // to publish the first payload. With the native 1.20 committer, a second checkpoint
            // can clear that payload before the crash and miss the already-published replay.
            long checkpoint =
                    flink.identifier() == TestContainerId.FLINK_1_18
                            ? rest.completeCheckpoint(jobId, firstCheckpoint)
                            : firstCheckpoint;
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                Assertions.assertTrue(
                                        Files.isRegularFile(Paths.get(root, "publication-gated")),
                                        "The real committer must stop at the selected publication boundary");
                            });
            Table table = loadTable(warehouse);
            Assertions.assertEquals(
                    version, ((HasTableOperations) table).operations().current().formatVersion());
            Set<Long> snapshots = snapshotIds(table);
            Assertions.assertEquals("before".equals(boundary) ? 0 : 1, snapshots.size());
            Set<String> files = "before".equals(boundary) ? null : liveFiles(table, version);
            if ("before".equals(boundary)) {
                Assertions.assertNull(
                        table.currentSnapshot(), "No external publication before crash");
            } else {
                assertRows(table);
            }
            JsonNode before = rest.get("/jobs/" + jobId + "/checkpoints");
            Assertions.assertEquals(
                    checkpoint, before.path("latest").path("completed").path("id").asLong(-1));
            Assertions.assertEquals(0, before.path("counts").path("restored").asLong(-1));
            Assertions.assertEquals(0, before.path("counts").path("in_progress").asLong(-1));
            String checkpointPath =
                    before.path("latest").path("completed").path("external_path").asText();
            Assertions.assertTrue(
                    checkpointPath.contains(root + "/checkpoints/"), before.toString());

            Set<String> oldTaskManagers = rest.taskManagerIds();
            Assertions.assertEquals(1, oldTaskManagers.size());
            crashTaskManager(taskManager);
            // Restart the existing container, preserving its filesystem and mounted checkpoints.
            // Testcontainers.stop/start would remove/recreate it; harness teardown deletes the
            // mount.
            DockerClientFactory.instance()
                    .client()
                    .restartContainerCmd(taskManager.getContainerId())
                    .withTimeout(1)
                    .exec();
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(180, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                Set<String> registered = rest.taskManagerIds();
                                Assertions.assertEquals(1, registered.size());
                                Assertions.assertTrue(
                                        Collections.disjoint(oldTaskManagers, registered),
                                        "The restarted worker must register a new resource ID");
                            });
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(180, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                rest.assertRunning(jobId);
                                JsonNode stats = rest.get("/jobs/" + jobId + "/checkpoints");
                                Assertions.assertEquals(
                                        1, stats.path("counts").path("restored").asLong(-1));
                                Assertions.assertEquals(
                                        checkpoint,
                                        stats.path("latest")
                                                .path("restored")
                                                .path("id")
                                                .asLong(-1));
                                Assertions.assertEquals(
                                        checkpointPath,
                                        stats.path("latest")
                                                .path("restored")
                                                .path("external_path")
                                                .asText());
                                Assertions.assertFalse(
                                        stats.path("latest")
                                                .path("restored")
                                                .path("is_savepoint")
                                                .asBoolean(true));
                            });
            // Two new barriers require post-restore task progress, including another checkpoint
            // after the first checkpoint-completion notification. Merely seeing RUNNING is
            // insufficient.
            long next = rest.completeCheckpoint(jobId, checkpoint);
            rest.completeCheckpoint(jobId, next);
            assertSubmissionActive(submission);
            rest.assertRunning(jobId);
            table.refresh();
            assertRows(table);
            Set<String> publishedLocations = new HashSet<>();
            for (org.apache.iceberg.DataFile file :
                    table.currentSnapshot().addedDataFiles(table.io())) {
                publishedLocations.add(file.location());
            }
            for (DeleteFile file : table.currentSnapshot().addedDeleteFiles(table.io())) {
                publishedLocations.add(file.location());
            }
            Assertions.assertEquals(
                    new HashSet<>(Files.readAllLines(Paths.get(root, "pending-files"))),
                    publishedLocations,
                    "Recovery must publish the original staged files, not regenerated files");
            Assertions.assertEquals(1, snapshotIds(table).size(), "One and only one publication");
            if (files != null) {
                Assertions.assertEquals(
                        snapshots, snapshotIds(table), "Replay must not add snapshots");
                Assertions.assertEquals(
                        files,
                        liveFiles(table, version),
                        "Replay must not add or replace data/delete files");
            } else {
                liveFiles(table, version);
            }
            Assertions.assertEquals(
                    1,
                    rest.get("/jobs/" + jobId + "/checkpoints")
                            .path("counts")
                            .path("restored")
                            .asLong(-1));
            log.info(
                    "Flink {} Iceberg v{} "
                            + boundary
                            + " publication restored job {} from checkpoint {} and completed fresh"
                            + " checkpoints",
                    flink.identifier(),
                    version,
                    jobId,
                    checkpoint);
        } catch (Exception | AssertionError failure) {
            primaryFailure = failure;
            log.error(
                    "Flink recovery failure; job={}, warehouse={}\n{}",
                    jobIdRef.get(),
                    warehouse,
                    flink.getServerLogs());
            throw failure;
        } finally {
            try {
                cancelJobAndSubmission(rest, previousJobs, submission, executor);
            } catch (Exception | AssertionError cleanupFailure) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else {
                    throw cleanupFailure;
                }
            }
            // Preserve this run's unique files for diagnosis; normal container teardown owns
            // cleanup.
            Files.deleteIfExists(jar);
            Files.deleteIfExists(mapping);
            Files.deleteIfExists(fixture);
        }
        if ("after".equals(boundary)) {
            verifyNewJobReusesCheckpointId(flink, rest, warehouse, root, taskManager);
        }
    }

    private void verifyNewJobReusesCheckpointId(
            AbstractTestFlinkContainer flink,
            FlinkRest rest,
            String warehouse,
            String root,
            GenericContainer<?> taskManager)
            throws Exception {
        Table table = loadTable(warehouse);
        long oldSnapshot = table.currentSnapshot().snapshotId();
        String oldCheckpoint = table.currentSnapshot().summary().get("seatunnel.checkpoint-id");
        Set<String> previous = rest.jobIds();
        int logOffset = taskManager.getLogs().length();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Container.ExecResult> submission =
                executor.submit(
                        () ->
                                flink.executeJob(
                                        RESOURCE,
                                        Arrays.asList(
                                                "flink_recovery_warehouse=file://" + warehouse,
                                                "flink_recovery_checkpoints=file://"
                                                        + root
                                                        + "/reuse-checkpoints",
                                                "flink_recovery_version="
                                                        + ((HasTableOperations) table)
                                                                .operations()
                                                                .current()
                                                                .formatVersion())));
        try {
            AtomicReference<String> id = new AtomicReference<>();
            await().atMost(180, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                Set<String> jobs = rest.jobIds();
                                jobs.removeAll(previous);
                                Assertions.assertEquals(1, jobs.size());
                                String job = jobs.iterator().next();
                                rest.assertRunning(job);
                                id.set(job);
                            });
            // Wait for the second literal source script too, not just RUNNING/deployment.
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                String logs = taskManager.getLogs();
                                Assertions.assertTrue(
                                        logs.substring(Math.min(logOffset, logs.length()))
                                                .contains(
                                                        "8 rows of custom data have been generated"));
                            });
            long checkpoint = rest.completeCheckpoint(id.get(), 0);
            Assertions.assertEquals(
                    1, checkpoint, "The new job must start checkpoint numbering again");
            rest.completeCheckpoint(id.get(), checkpoint);
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertSubmissionActive(submission);
                                table.refresh();
                                Assertions.assertNotEquals(
                                        oldSnapshot,
                                        table.currentSnapshot().snapshotId(),
                                        "A reused checkpoint ID must not discard new immutable files");
                                Assertions.assertEquals(
                                        oldCheckpoint,
                                        table.currentSnapshot()
                                                .summary()
                                                .get("seatunnel.checkpoint-id"));
                                Assertions.assertEquals(2, snapshotIds(table).size());
                                assertRows(table);
                            });
        } finally {
            cancelJobAndSubmission(rest, previous, submission, executor);
        }
    }

    private static Path createSinkJar(Path directory, Path connector) throws IOException {
        Path jar = directory.resolve("iceberg-recovery-test.jar");
        String services = "META-INF/services/" + Factory.class.getName();
        try (JarFile source = new JarFile(connector.toFile());
                JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            java.util.Enumeration<JarEntry> entries = source.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (services.equals(entry.getName())) {
                    continue;
                }
                out.putNextEntry(new JarEntry(entry.getName()));
                try (InputStream in = source.getInputStream(entry)) {
                    in.transferTo(out);
                }
                out.closeEntry();
            }
            addClass(out, IcebergFlinkRecoverySinkFactory.class);
            out.putNextEntry(new JarEntry(services));
            try (InputStream in = source.getInputStream(source.getJarEntry(services))) {
                in.transferTo(out);
            }
            out.write('\n');
            out.write(
                    (IcebergFlinkRecoverySinkFactory.class.getName() + "\n")
                            .getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return jar;
    }

    private static void addClass(JarOutputStream out, Class<?> type) throws IOException {
        String name = type.getName().replace('.', '/') + ".class";
        out.putNextEntry(new JarEntry(name));
        try (InputStream in = type.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("Missing compiled fixture " + name);
            }
            in.transferTo(out);
        }
        out.closeEntry();
        for (Class<?> nested : type.getDeclaredClasses()) {
            addClass(out, nested);
        }
    }

    private static void cancelJobAndSubmission(
            FlinkRest rest,
            Set<String> previousJobs,
            Future<Container.ExecResult> submission,
            ExecutorService executor)
            throws Exception {
        try {
            // Also find a submitted job when failure occurred before job discovery completed.
            Set<String> jobs = rest.jobIds();
            jobs.removeAll(previousJobs);
            for (String job : jobs) {
                if (!isTerminal(rest.get("/jobs/" + job).path("state").asText())) {
                    rest.request("PATCH", "/jobs/" + job + "?mode=cancel", null);
                    await().pollInterval(Duration.ofSeconds(1))
                            .atMost(30, TimeUnit.SECONDS)
                            .until(
                                    () ->
                                            isTerminal(
                                                    rest.get("/jobs/" + job)
                                                            .path("state")
                                                            .asText()));
                }
            }
            // The attached CLI must terminate; do not leave an orphaned submission thread.
            submission.get(30, TimeUnit.SECONDS);
        } finally {
            submission.cancel(true);
            executor.shutdownNow();
            Assertions.assertTrue(
                    executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Submission thread did not terminate");
        }
    }

    private static boolean isTerminal(String state) {
        return "CANCELED".equals(state) || "FAILED".equals(state) || "FINISHED".equals(state);
    }

    private static void crashTaskManager(GenericContainer<?> taskManager) throws Exception {
        String pid = taskManagerPid(taskManager);
        Assertions.assertTrue(
                pid.matches("[0-9]+"), "Exactly one TaskManager JVM is required: " + pid);
        log.info(
                "Sending SIGKILL to TaskManager JVM pid={} container={}",
                pid,
                taskManager.getContainerId());
        try {
            if ("1".equals(pid)) {
                // A namespace's init ignores SIGKILL sent from inside that namespace. Ask the
                // Docker daemon to deliver it from outside; retain the container for restart.
                DockerClientFactory.instance()
                        .client()
                        .killContainerCmd(taskManager.getContainerId())
                        .withSignal("KILL")
                        .exec();
            } else {
                Container.ExecResult result =
                        taskManager.execInContainer("sh", "-c", "kill -KILL " + pid);
                Assertions.assertTrue(
                        result.getExitCode() == 0 || result.getExitCode() == 137,
                        result.getStderr());
            }
        } catch (IOException | DockerException expectedWhenPidOneExits) {
            // An exec transport error is not crash evidence; independently verify JVM
            // disappearance.
            log.info("TaskManager kill exec disconnected: {}", expectedWhenPidOneExits.toString());
        }
        await().pollInterval(Duration.ofSeconds(1))
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        !taskManager.isRunning()
                                                || taskManagerPid(taskManager).isEmpty(),
                                        "The actual TaskManager JVM must be gone before"
                                                + " restarting"));
    }

    private static String taskManagerPid(GenericContainer<?> taskManager) throws Exception {
        Container.ExecResult result =
                taskManager.execInContainer(
                        "sh",
                        "-c",
                        // Some pinned engines contain a JRE without jps. Match the exact main
                        // class argument, not a substring that could select this shell or awk.
                        "ps -eo pid,args | awk '{for (i = 2; i <= NF; i++) if ($i == \""
                                + TASK_MANAGER_CLASS
                                + "\") {print $1; break}}'");
        assertExec(result);
        return result.getStdout().trim();
    }

    private static void assertExec(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private static void assertSubmissionActive(Future<Container.ExecResult> submission)
            throws Exception {
        if (submission.isDone()) {
            Container.ExecResult result = submission.get();
            Assertions.fail(
                    "Streaming submission exited early: "
                            + result.getExitCode()
                            + "\n"
                            + result.getStdout()
                            + result.getStderr());
        }
    }

    private static Table loadTable(String warehouse) throws IOException {
        Map<String, Object> catalog = new HashMap<>();
        catalog.put("type", "hadoop");
        catalog.put("warehouse", "file://" + warehouse);
        catalog.put("io-impl", HadoopStreamFileIO.class.getName());
        Map<String, Object> config = new HashMap<>();
        config.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "flink_recovery_test");
        config.put(IcebergCommonOptions.KEY_NAMESPACE.key(), NAMESPACE);
        config.put(IcebergCommonOptions.KEY_TABLE.key(), TABLE);
        config.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalog);
        try (IcebergTableLoader loader =
                IcebergTableLoader.create(
                        new IcebergSourceConfig(ReadonlyConfig.fromMap(config)))) {
            loader.open();
            return loader.loadTable();
        }
    }

    private static void assertRows(Table table) throws IOException {
        Map<Long, String> rows = new HashMap<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record row : records) {
                Assertions.assertNull(
                        rows.put((Long) row.getField("id"), (String) row.getField("name")),
                        "Duplicate live primary key");
            }
        }
        Map<Long, String> expected = new HashMap<>();
        expected.put(1L, "A_2");
        expected.put(3L, "C_1");
        Assertions.assertEquals(expected, rows);
    }

    private static Set<Long> snapshotIds(Table table) {
        Set<Long> snapshots = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            snapshots.add(snapshot.snapshotId());
        }
        return snapshots;
    }

    private static Set<String> liveFiles(Table table, int version) throws IOException {
        Set<String> files = new TreeSet<>();
        boolean foundVector = false;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                files.add("data:" + task.file().location() + ":" + task.file().recordCount());
                for (DeleteFile delete : task.deletes()) {
                    files.add(
                            "delete:"
                                    + delete.location()
                                    + ":"
                                    + delete.content()
                                    + ":"
                                    + delete.recordCount()
                                    + ":"
                                    + delete.dataSequenceNumber()
                                    + ":"
                                    + delete.referencedDataFile()
                                    + ":"
                                    + delete.contentOffset()
                                    + ":"
                                    + delete.contentSizeInBytes());
                    if (version == 3 && delete.content() == FileContent.POSITION_DELETES) {
                        Assertions.assertEquals(FileFormat.PUFFIN, delete.format());
                        Assertions.assertEquals(
                                task.file().location(), delete.referencedDataFile());
                        Assertions.assertTrue(delete.contentOffset() > 0);
                        Assertions.assertTrue(delete.contentSizeInBytes() > 0);
                        foundVector = true;
                    }
                    if (version == 2) {
                        Assertions.assertNotEquals(FileFormat.PUFFIN, delete.format());
                    }
                }
            }
        }
        if (version == 3) {
            Assertions.assertTrue(foundVector, "The recovery checkpoint must contain real DVs");
        }
        return files;
    }

    private static final class FlinkRest {
        private final HttpClient client =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        private final String base;

        private FlinkRest(AbstractTestFlinkContainer flink) throws Exception {
            base =
                    new URI(
                                    "http",
                                    null,
                                    flink.getJobManagerHost(),
                                    flink.getJobManagerRestPort(),
                                    null,
                                    null,
                                    null)
                            .toString();
        }

        private JsonNode get(String path) throws Exception {
            // A busy emulated JobManager can time out one read while processing deployment.
            // Retry only reads; a timed-out checkpoint POST may already have taken effect.
            for (int attempt = 0; ; attempt++) {
                try {
                    return request("GET", path, null);
                } catch (HttpTimeoutException timeout) {
                    if (attempt == 2) {
                        throw timeout;
                    }
                }
            }
        }

        private JsonNode request(String method, String path, String body) throws Exception {
            HttpRequest request =
                    HttpRequest.newBuilder(URI.create(base + path))
                            .timeout(Duration.ofSeconds(5))
                            .header("Content-Type", "application/json")
                            .method(
                                    method,
                                    body == null
                                            ? HttpRequest.BodyPublishers.noBody()
                                            : HttpRequest.BodyPublishers.ofString(body))
                            .build();
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            Assertions.assertTrue(
                    response.statusCode() >= 200 && response.statusCode() < 300,
                    method + " " + path + " => " + response.statusCode() + " " + response.body());
            return response.body().isEmpty()
                    ? MAPPER.createObjectNode()
                    : MAPPER.readTree(response.body());
        }

        private Set<String> jobIds() throws Exception {
            Set<String> jobs = new HashSet<>();
            for (JsonNode job : get("/jobs/overview").path("jobs")) {
                jobs.add(job.path("jid").asText());
            }
            return jobs;
        }

        private Set<String> taskManagerIds() throws Exception {
            Set<String> ids = new HashSet<>();
            for (JsonNode worker : get("/taskmanagers").path("taskmanagers")) {
                ids.add(worker.path("id").asText());
            }
            return ids;
        }

        private void assertRunning(String jobId) throws Exception {
            JsonNode job = get("/jobs/" + jobId);
            String state = job.path("state").asText();
            if ("FAILED".equals(state) || "CANCELED".equals(state) || "FINISHED".equals(state)) {
                throw new IllegalStateException("Job terminated: " + job);
            }
            Assertions.assertEquals("RUNNING", state);
            Assertions.assertTrue(job.path("vertices").size() > 0);
            for (JsonNode vertex : job.path("vertices")) {
                Assertions.assertEquals(
                        "RUNNING", vertex.path("status").asText(), vertex.toString());
            }
        }

        private long completeCheckpoint(String jobId, long previous) throws Exception {
            JsonNode trigger = request("POST", "/jobs/" + jobId + "/checkpoints", "{}");
            Assertions.assertFalse(
                    trigger.path("request-id").asText().isEmpty(), trigger.toString());
            String requestId = trigger.path("request-id").asText();
            AtomicReference<Long> completed = new AtomicReference<>();
            await().pollInterval(Duration.ofSeconds(1))
                    .atMost(90, TimeUnit.SECONDS)
                    .ignoreException(HttpTimeoutException.class)
                    .untilAsserted(
                            () -> {
                                assertRunning(jobId);
                                JsonNode result =
                                        get("/jobs/" + jobId + "/checkpoints/" + requestId);
                                Assertions.assertEquals(
                                        "COMPLETED",
                                        result.path("status").path("id").asText(),
                                        result.toString());
                                JsonNode operation = result.path("operation");
                                if (!operation.path("failureCause").isMissingNode()
                                        && !operation.path("failureCause").isNull()) {
                                    throw new IllegalStateException(
                                            "Checkpoint trigger failed: " + result);
                                }
                                JsonNode stats = get("/jobs/" + jobId + "/checkpoints");
                                Assertions.assertEquals(
                                        0,
                                        stats.path("counts").path("failed").asLong(-1),
                                        stats.toString());
                                JsonNode checkpoint = stats.path("latest").path("completed");
                                Assertions.assertEquals(
                                        "COMPLETED", checkpoint.path("status").asText());
                                Assertions.assertTrue(
                                        checkpoint.path("id").asLong(-1) > previous,
                                        stats.toString());
                                Assertions.assertEquals(
                                        operation.path("checkpointId").asLong(-1),
                                        checkpoint.path("id").asLong(-2),
                                        result.toString());
                                completed.set(checkpoint.path("id").asLong());
                            });
            return completed.get();
        }
    }
}
