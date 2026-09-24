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

package org.apache.seatunnel.core.starter.seatunnel.application;

import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.HdfsFileStorageInstance;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.core.classloader.ApplicationJarPathResolver;
import org.apache.seatunnel.resource.core.config.ApplicationClusterConfig;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Exercises application lifecycle against real master/worker engine instances and native jobs. */
@Timeout(120)
class ApplicationRuntimeTest {
    // The existing HDFS storage plugin caches one filesystem instance per JVM. Real application
    // masters run in separate JVMs; these in-process fixtures must share its configured location.
    @TempDir static Path checkpoints;

    private static Field storageSingleton;
    private static Object previousStorage;

    @BeforeAll
    static void isolateProcessScopedCheckpointStorage() throws ReflectiveOperationException {
        // Other starter tests initialize the same process-scoped plugin with different settings.
        // Model the application's fresh process here, then restore the prior fixture at class end.
        storageSingleton = HdfsFileStorageInstance.class.getDeclaredField("HDFS_STORAGE");
        storageSingleton.setAccessible(true);
        previousStorage = storageSingleton.get(null);
        storageSingleton.set(null, null);
    }

    @AfterAll
    static void restoreProcessScopedCheckpointStorage() throws IllegalAccessException {
        if (storageSingleton != null) {
            storageSingleton.set(null, previousStorage);
        }
    }

    private static final String JOB =
            "env { parallelism = 1, job.mode = BATCH }\n"
                    + "source { FakeSource { row.num = 10, schema { fields { id = int } } } }\n"
                    + "sink { Console {} }";

    @Test
    void cancelsJobThatFinishesSubmissionDuringShutdown() {
        ClientJobProxy submittedJob = mock(ClientJobProxy.class);

        assertThrows(
                IllegalStateException.class,
                () -> ApplicationRuntime.cancelSubmittedJobIfClosing(submittedJob, true));

        verify(submittedJob).cancelJob();
    }

    @Test
    void runsNativeBatchOnAnIsolatedWorkerAndCleansResources() throws Exception {
        LocalDriver driver = new LocalDriver();
        ApplicationResult result = run(JOB, driver, 30000);
        assertEquals(ApplicationStatus.SUCCEEDED, result.getStatus(), result.getDiagnostics());
        assertEquals(ApplicationStatus.SUCCEEDED, driver.finalStatus);
        assertEquals(1, driver.requests);
        assertEquals(1, driver.releases);
        assertNotNull(driver.worker);
        assertFalse(driver.worker.getLifecycleService().isRunning());
        assertTrue(driver.closed);
    }

    @Test
    void cleanupFailureChangesThePublishedOutcome() throws Exception {
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public void releaseWorker(WorkerRegistration registration) {
                        super.releaseWorker(registration);
                        throw new IllegalStateException("release acknowledgment failed");
                    }
                };
        ApplicationResult result = run(JOB, driver, 30000);
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertTrue(result.getDiagnostics().contains("release acknowledgment failed"));
        assertTrue(driver.closed);
    }

    @Test
    void malformedJobFailsAndCleansWorker() throws Exception {
        LocalDriver driver = new LocalDriver();
        ApplicationResult result = run("not valid {", driver, 30000);
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertEquals(1, driver.releases);
        assertTrue(driver.closed);
        assertFalse(driver.worker.getLifecycleService().isRunning());
    }

    @Test
    void missingCheckpointFailsWithoutExecutingFreshJob() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "42");
        options.put(ApplicationOptions.JOB_ID.key(), "43");
        LocalDriver driver = new LocalDriver();
        ApplicationResult result =
                ApplicationRuntime.run(
                        new ApplicationId(DeployType.KUBERNETES, "missing-checkpoint"),
                        specification(JOB, 30000, options),
                        driver,
                        engineConfig());
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertTrue(
                result.getDiagnostics().contains("No eligible checkpoint found for source job 42"));
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertEquals(1, driver.releases);
        assertTrue(driver.closed);
    }

    @Test
    void restoresPersistentCheckpointAfterOriginalApplicationStops() throws Exception {
        String streamingJob =
                "env { parallelism = 1, job.mode = STREAMING, checkpoint.interval = 500 }\n"
                        + "source { FakeSource { row.num = 1, split.read-interval = 500, "
                        + "schema { fields { id = int } } } }\n"
                        + "sink { Console {} }";
        Map<String, String> options = new HashMap<>();
        ApplicationSpecification source = specification(streamingJob, 30000, options);
        List<Long> sourceCheckpoints = runUntilCheckpoints(source, checkpoints, true);
        long lastSourceCheckpoint = Collections.max(sourceCheckpoints);

        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), Long.toString(source.getJobId()));
        ApplicationSpecification restored = specification(streamingJob, 30000, options);
        List<Long> restoredCheckpoints = runUntilCheckpoints(restored, checkpoints, false);
        assertTrue(
                Collections.min(restoredCheckpoints) > lastSourceCheckpoint,
                "The new application must continue the persisted checkpoint sequence");
        assertFalse(checkpointIds(checkpoints.resolve(Long.toString(source.getJobId()))).isEmpty());
    }

    private List<Long> runUntilCheckpoints(
            ApplicationSpecification specification, Path checkpointRoot, boolean failWorker)
            throws Exception {
        LocalDriver driver = new LocalDriver();
        AtomicReference<ApplicationResult> outcome = new AtomicReference<>();
        Thread application =
                new Thread(
                        () ->
                                outcome.set(
                                        ApplicationRuntime.run(
                                                new ApplicationId(
                                                        DeployType.KUBERNETES,
                                                        "checkpoint-" + specification.getJobId()),
                                                specification,
                                                driver,
                                                engineConfig())));
        Path jobCheckpoints = checkpointRoot.resolve(Long.toString(specification.getJobId()));
        application.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (checkpointIds(jobCheckpoints).size() < 2
                    && application.isAlive()
                    && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            assertTrue(
                    checkpointIds(jobCheckpoints).size() >= 2,
                    () ->
                            outcome.get() == null
                                    ? "Application did not persist two completed checkpoints"
                                    : outcome.get().getDiagnostics());
            if (failWorker) {
                // The external driver can report failure before Hazelcast or the native job does.
                // Cleanup then cancels that running job; its persisted state must remain usable.
                driver.context.onWorkerTerminated("local-worker", "simulated platform failure");
                application.join(30000);
            }
        } finally {
            application.interrupt();
            application.join(30000);
        }
        assertFalse(application.isAlive());
        assertEquals(
                failWorker ? ApplicationStatus.FAILED : ApplicationStatus.CANCELED,
                outcome.get().getStatus(),
                outcome.get().getDiagnostics());
        assertTrue(driver.closed);
        return checkpointIds(jobCheckpoints);
    }

    private List<Long> checkpointIds(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return Collections.emptyList();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".ser"))
                    .map(
                            name ->
                                    Long.parseLong(
                                            name.substring(
                                                    name.lastIndexOf('-') + 1, name.length() - 4)))
                    .collect(Collectors.toList());
        }
    }

    @Test
    void workerStopsWhenItsApplicationMasterDisappears() throws Exception {
        String clusterName = "application-master-loss-" + UUID.randomUUID();
        SeaTunnelConfig masterConfig = engineConfig();
        ApplicationClusterConfig.configure(masterConfig, clusterName, null, 2);
        HazelcastInstance master =
                SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
        HazelcastInstance worker = null;
        try {
            String address =
                    master.getCluster().getLocalMember().getAddress().getHost()
                            + ":"
                            + master.getCluster().getLocalMember().getAddress().getPort();
            worker = ApplicationWorker.start(clusterName, address, 2, engineConfig());
            master.shutdown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (worker.getLifecycleService().isRunning() && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            assertFalse(
                    worker.getLifecycleService().isRunning(),
                    "Orphan worker must stop without master HA");
        } finally {
            if (worker != null) {
                worker.shutdown();
            }
            master.shutdown();
        }
    }

    @Test
    void workerLoadsRelocatedArtifactThroughExplicitResolver(@TempDir Path directory)
            throws Exception {
        Path masterHome = directory.resolve("master-distribution");
        Path workerHome = directory.resolve("worker-distribution");
        Path jar = workerHome.resolve("connectors/plugin.jar");
        Files.createDirectories(jar.getParent());
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry("localized-artifact.txt"));
            output.write('w');
            output.closeEntry();
        }
        String clusterName = "application-artifact-" + UUID.randomUUID();
        SeaTunnelConfig masterConfig = engineConfig();
        ApplicationClusterConfig.configure(masterConfig, clusterName, null, 2);
        HazelcastInstance master =
                SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
        HazelcastInstance worker = null;
        try {
            worker =
                    ApplicationWorker.start(
                            clusterName,
                            master.getCluster().getLocalMember().getAddress().getHost()
                                    + ":"
                                    + master.getCluster().getLocalMember().getAddress().getPort(),
                            2,
                            engineConfig(),
                            new ApplicationJarPathResolver(
                                    masterHome.toString(), workerHome.toString()));
            SeaTunnelServer server =
                    ((HazelcastInstanceImpl) worker)
                            .node
                            .getNodeEngine()
                            .getService(SeaTunnelServer.SERVICE_NAME);
            URL original = masterHome.resolve("connectors/plugin.jar").toUri().toURL();
            ClassLoader loader =
                    server.getClassLoaderService()
                            .getClassLoader(17L, Collections.singletonList(original));
            try (InputStream resource = loader.getResourceAsStream("localized-artifact.txt")) {
                assertNotNull(resource);
                assertEquals('w', resource.read());
            }
            server.getClassLoaderService()
                    .releaseClassLoader(17L, Collections.singletonList(original));
        } finally {
            if (worker != null) {
                worker.shutdown();
            }
            master.shutdown();
        }
    }

    @Test
    void allocationFailureIsReportedAndDriverIsClosed() throws Exception {
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public CompletableFuture<WorkerRegistration> requestWorker(
                            WorkerSpecification specification) {
                        CompletableFuture<WorkerRegistration> failed = new CompletableFuture<>();
                        failed.completeExceptionally(
                                new IllegalStateException("allocation rejected"));
                        return failed;
                    }
                };
        ApplicationResult result = run(JOB, driver, 30000);
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertTrue(result.getDiagnostics().contains("allocation rejected"));
        assertTrue(driver.closed);
    }

    @Test
    void canceledAllocationThatLaunchesLateIsDrainedBeforeMasterStops() throws Exception {
        CompletableFuture<WorkerRegistration> pending = new CompletableFuture<>();
        AtomicReference<ResourceManagerContext> context = new AtomicReference<>();
        AtomicReference<HazelcastInstance> lateWorker = new AtomicReference<>();
        AtomicBoolean joinedExistingMaster = new AtomicBoolean();
        AtomicBoolean futureWasCanceledBeforeLaunch = new AtomicBoolean();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public void initialize(ResourceManagerContext runtimeContext) {
                        super.initialize(runtimeContext);
                        context.set(runtimeContext);
                    }

                    @Override
                    public CompletableFuture<WorkerRegistration> requestWorker(
                            WorkerSpecification specification) {
                        context.get()
                                .onError(
                                        new IllegalStateException(
                                                "stop while allocation is pending"));
                        return pending;
                    }

                    @Override
                    public void stopWorkers() {
                        futureWasCanceledBeforeLaunch.set(pending.isCancelled());
                        // Model a create accepted by the platform just before its future was
                        // canceled.
                        HazelcastInstance worker =
                                ApplicationWorker.start(
                                        context.get().getClusterName(),
                                        context.get().getMasterAddress(),
                                        2,
                                        engineConfig());
                        lateWorker.set(worker);
                        joinedExistingMaster.set(
                                worker.getCluster().getMembers().stream()
                                        .anyMatch(member -> !member.isLiteMember()));
                        pending.complete(new WorkerRegistration("late-worker"));
                        worker.shutdown();
                    }

                    @Override
                    public void close() {
                        super.close();
                        if (lateWorker.get() != null) {
                            lateWorker.get().shutdown();
                        }
                    }
                };
        ApplicationResult result = run(JOB, driver, 30000);
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertTrue(result.getDiagnostics().contains("stop while allocation is pending"));
        assertTrue(futureWasCanceledBeforeLaunch.get());
        assertTrue(
                joinedExistingMaster.get(),
                "Master must remain available until pending allocations are drained");
        assertFalse(lateWorker.get().getLifecycleService().isRunning());
        assertTrue(driver.closed);
    }

    @Test
    void pendingAllocationTimesOutAndIsCanceled() throws Exception {
        CompletableFuture<WorkerRegistration> pending = new CompletableFuture<>();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public CompletableFuture<WorkerRegistration> requestWorker(
                            WorkerSpecification specification) {
                        return pending;
                    }
                };
        ApplicationResult result = run(JOB, driver, 5000);
        assertEquals(ApplicationStatus.FAILED, result.getStatus());
        assertTrue(result.getDiagnostics().contains("Timed out"));
        assertTrue(pending.isCancelled());
        assertTrue(driver.closed);
    }

    @Test
    void interruptedApplicationPublishesCanceledAndCleansPendingAllocation() throws Exception {
        CountDownLatch requested = new CountDownLatch(1);
        CompletableFuture<WorkerRegistration> pending = new CompletableFuture<>();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public CompletableFuture<WorkerRegistration> requestWorker(
                            WorkerSpecification specification) {
                        requested.countDown();
                        return pending;
                    }
                };
        ApplicationSpecification specification = specification(JOB, 30000);
        AtomicReference<ApplicationResult> outcome = new AtomicReference<>();
        Thread application =
                new Thread(
                        () ->
                                outcome.set(
                                        ApplicationRuntime.run(
                                                new ApplicationId(
                                                        DeployType.KUBERNETES, "interrupted"),
                                                specification,
                                                driver,
                                                engineConfig())));
        application.start();
        try {
            assertTrue(requested.await(30, TimeUnit.SECONDS));
            application.interrupt();
            application.join(30000);
            assertFalse(application.isAlive());
            assertEquals(ApplicationStatus.CANCELED, outcome.get().getStatus());
            assertEquals(ApplicationStatus.CANCELED, driver.finalStatus);
            assertTrue(pending.isCancelled());
            assertTrue(driver.closed);
        } finally {
            application.interrupt();
            application.join(30000);
        }
    }

    @Test
    void mapsOnlySuccessfulNativeTerminationToSuccess() {
        assertEquals(
                ApplicationStatus.SUCCEEDED,
                ApplicationRuntime.applicationStatus(JobStatus.FINISHED));
        assertEquals(
                ApplicationStatus.SUCCEEDED,
                ApplicationRuntime.applicationStatus(JobStatus.SAVEPOINT_DONE));
        assertEquals(
                ApplicationStatus.CANCELED,
                ApplicationRuntime.applicationStatus(JobStatus.CANCELED));
        assertEquals(
                ApplicationStatus.FAILED, ApplicationRuntime.applicationStatus(JobStatus.FAILED));
        assertEquals(
                ApplicationStatus.FAILED,
                ApplicationRuntime.applicationStatus(JobStatus.UNKNOWABLE));
    }

    private ApplicationResult run(String config, LocalDriver driver, long timeout)
            throws Exception {
        return ApplicationRuntime.run(
                new ApplicationId(DeployType.KUBERNETES, "test-application"),
                specification(config, timeout),
                driver,
                engineConfig());
    }

    private ApplicationSpecification specification(String config, long timeout) throws Exception {
        return specification(config, timeout, Collections.emptyMap());
    }

    private ApplicationSpecification specification(
            String config, long timeout, Map<String, String> additionalOptions) throws Exception {
        Map<String, String> options = new HashMap<>();
        try (ServerSocket socket = new ServerSocket(0)) {
            options.put("application.master.port", Integer.toString(socket.getLocalPort()));
        }
        options.put("application.startup-timeout-millis", Long.toString(timeout));
        options.putAll(additionalOptions);
        return new ApplicationSpecification(
                DeployType.KUBERNETES,
                "application-test",
                config,
                1,
                new WorkerSpecification(1024, 1, 2),
                options);
    }

    private static SeaTunnelConfig engineConfig() {
        SeaTunnelConfig config = new SeaTunnelConfig();
        CheckpointStorageConfig storage = new CheckpointStorageConfig();
        storage.setStorage("hdfs");
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("storage.type", "local");
        storageOptions.put("namespace", checkpoints.toString());
        storage.setStoragePluginConfig(storageOptions);
        CheckpointConfig checkpoint = new CheckpointConfig();
        checkpoint.setStorage(storage);
        config.getEngineConfig().setCheckpointConfig(checkpoint);
        config.getEngineConfig().setStateCleanupDelayMillis(0);
        config.getHazelcastConfig().getNetworkConfig().setPort(0);
        config.getHazelcastConfig().setProperty("hazelcast.operation.thread.count", "2");
        config.getHazelcastConfig().setProperty("hazelcast.operation.generic.thread.count", "2");
        config.getHazelcastConfig().setProperty("hazelcast.io.input.thread.count", "1");
        config.getHazelcastConfig().setProperty("hazelcast.io.output.thread.count", "1");
        config.getHazelcastConfig().setProperty("hazelcast.event.thread.count", "2");
        config.getHazelcastConfig().setProperty("hazelcast.partition.count", "17");
        config.getHazelcastConfig().setProperty("hazelcast.logging.type", "slf4j");
        return config;
    }

    private static class LocalDriver implements ResourceManagerDriver {
        private ResourceManagerContext context;
        private HazelcastInstance worker;
        private int requests;
        private int releases;
        private boolean closed;
        private ApplicationStatus finalStatus;

        @Override
        public void initialize(ResourceManagerContext context) {
            this.context = context;
        }

        @Override
        public CompletableFuture<WorkerRegistration> requestWorker(
                WorkerSpecification specification) {
            requests++;
            worker =
                    ApplicationWorker.start(
                            context.getClusterName(),
                            context.getMasterAddress(),
                            specification.getSlots(),
                            engineConfig());
            return CompletableFuture.completedFuture(new WorkerRegistration("local-worker"));
        }

        @Override
        public void releaseWorker(WorkerRegistration registration) {
            releases++;
            worker.shutdown();
        }

        @Override
        public void finish(ApplicationStatus status, String diagnostics) {
            finalStatus = status;
        }

        @Override
        public void close() {
            closed = true;
            if (worker != null) {
                worker.shutdown();
            }
        }
    }
}
