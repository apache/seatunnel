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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.HdfsFileStorageInstance;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.classloader.ApplicationJarPathResolver;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.application.ApplicationJobExecutionEnvironment;
import org.apache.seatunnel.engine.server.application.ApplicationJobRunner;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises application lifecycle against real master/worker engine instances and native jobs. */
@Timeout(120)
class ApplicationLifecycleTest {
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
    void executionTracksNativeCompletionWithoutOwningResources() throws Exception {
        SeaTunnelServer server = mock(SeaTunnelServer.class, RETURNS_DEEP_STUBS);
        CoordinatorService coordinator = server.getCoordinatorService();
        CompletableFuture<Void> submitted = new CompletableFuture<>();
        CompletableFuture<JobResult> nativeResult = new CompletableFuture<>();
        when(coordinator.submitJob(eq(73L), isNull(), eq(false)))
                .thenReturn(new PassiveCompletableFuture<>(submitted));
        when(coordinator.waitForJobComplete(73L))
                .thenReturn(new PassiveCompletableFuture<>(nativeResult));
        CompletableFuture<JobResult> execution =
                jobEnvironment(server).execute(new CompletableFuture<>());

        assertFalse(execution.isDone());
        verify(coordinator, never()).waitForJobComplete(73L);
        submitted.complete(null);
        assertFalse(execution.isDone());
        JobResult result = new JobResult(JobStatus.FINISHED, null);
        nativeResult.complete(result);
        assertSame(result, execution.get(5, TimeUnit.SECONDS));
        verify(coordinator, never()).getResourceManager();
        verify(coordinator, never()).cancelJob(73L);
    }

    @Test
    void cancellationBeforeSubmissionAcknowledgementCancelsTheAcceptedJob() throws Exception {
        SeaTunnelServer server = mock(SeaTunnelServer.class, RETURNS_DEEP_STUBS);
        CoordinatorService coordinator = server.getCoordinatorService();
        CompletableFuture<Void> submitted = new CompletableFuture<>();
        CompletableFuture<JobResult> nativeResult = new CompletableFuture<>();
        when(coordinator.submitJob(eq(73L), isNull(), eq(false)))
                .thenReturn(new PassiveCompletableFuture<>(submitted));
        when(coordinator.waitForJobComplete(73L))
                .thenReturn(new PassiveCompletableFuture<>(nativeResult));
        when(coordinator.cancelJob(73L))
                .thenReturn(
                        new PassiveCompletableFuture<>(CompletableFuture.completedFuture(null)));
        CompletableFuture<Void> cancellation = new CompletableFuture<>();
        CompletableFuture<JobResult> execution = jobEnvironment(server).execute(cancellation);

        cancellation.complete(null);
        verify(coordinator, never()).cancelJob(73L);
        submitted.complete(null);
        verify(coordinator).cancelJob(73L);
        assertFalse(execution.isDone(), "Cancellation acknowledgement is not job termination");
        nativeResult.complete(new JobResult(JobStatus.CANCELED, null));
        assertEquals(JobStatus.CANCELED, execution.get(5, TimeUnit.SECONDS).getStatus());
    }

    @Test
    void submissionFailureIsPropagatedWithoutWaitingForAnUnknownJob() {
        SeaTunnelServer server = mock(SeaTunnelServer.class, RETURNS_DEEP_STUBS);
        CoordinatorService coordinator = server.getCoordinatorService();
        CompletableFuture<Void> submitted = new CompletableFuture<>();
        when(coordinator.submitJob(eq(73L), isNull(), eq(false)))
                .thenReturn(new PassiveCompletableFuture<>(submitted));
        CompletableFuture<JobResult> execution =
                jobEnvironment(server).execute(new CompletableFuture<>());
        IllegalStateException failure = new IllegalStateException("submission rejected");
        submitted.completeExceptionally(failure);
        assertSame(failure, assertThrows(ExecutionException.class, execution::get).getCause());
        verify(coordinator, never()).waitForJobComplete(73L);
    }

    @Test
    void cancellationBeforeExecutionDoesNotSubmit() {
        SeaTunnelServer server = mock(SeaTunnelServer.class, RETURNS_DEEP_STUBS);
        ApplicationJobExecutionEnvironment environment = jobEnvironment(server);
        assertThrows(
                CancellationException.class,
                () -> environment.execute(CompletableFuture.completedFuture(null)));
        verify(server.getCoordinatorService(), never()).submitJob(eq(73L), any(), eq(false));
    }

    private ApplicationJobExecutionEnvironment jobEnvironment(SeaTunnelServer server) {
        ApplicationJobExecutionEnvironment environment =
                spy(
                        new ApplicationJobExecutionEnvironment(
                                new JobConfig(), ConfigFactory.empty(), server, 73L, null));
        doReturn(mock(LogicalDag.class)).when(environment).getLogicalDag();
        // Keep serialization outside these submission-order tests.
        when(server.getNodeEngine().toData(any())).thenReturn(null);
        return environment;
    }

    @Test
    void runsNativeBatchOnAnIsolatedWorkerAndCleansResources() throws Exception {
        LocalDriver driver = new LocalDriver();
        run(JOB, driver, 30000);
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
                    public CompletableFuture<Void> releaseWorker(ResourceID registration) {
                        super.releaseWorker(registration);
                        CompletableFuture<Void> result = new CompletableFuture<>();
                        result.completeExceptionally(
                                new IllegalStateException("release acknowledgment failed"));
                        return result;
                    }
                };
        Exception failure = assertThrows(Exception.class, () -> run(JOB, driver, 30000));
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertTrue(failure.getMessage().contains("release acknowledgment failed"));
        assertTrue(driver.closed);
    }

    @Test
    void malformedJobFailsAndCleansWorker() throws Exception {
        LocalDriver driver = new LocalDriver();
        ApplicationSpecification valid = specification(JOB, 30000);
        ApplicationSpecification malformed = valid.toBuilder().jobConfig("not valid {").build();
        assertThrows(
                Exception.class,
                () -> runApplication("test-application", malformed, driver, engineConfig()));
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertEquals(1, driver.releases);
        assertTrue(driver.closed);
        assertFalse(driver.worker.getLifecycleService().isRunning());
    }

    @Test
    void executionFailureKeepsCleanupFailureSuppressed() throws Exception {
        LocalDriver driver =
                new LocalDriver("failed-with-cleanup") {
                    @Override
                    public CompletableFuture<Void> releaseWorker(ResourceID registration) {
                        super.releaseWorker(registration);
                        CompletableFuture<Void> failed = new CompletableFuture<>();
                        failed.completeExceptionally(
                                new IllegalStateException("cleanup acknowledgment failed"));
                        return failed;
                    }
                };
        Map<String, String> options = new HashMap<>();
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "42");
        Exception failure =
                assertThrows(
                        Exception.class,
                        () ->
                                runApplication(
                                        "failed-with-cleanup",
                                        specification(JOB, 30000, options),
                                        driver,
                                        engineConfig()));
        assertTrue(failure.getMessage().contains("No checkpoint found"));
        assertTrue(
                java.util.Arrays.stream(failure.getSuppressed())
                        .anyMatch(
                                suppressed ->
                                        suppressed
                                                .getMessage()
                                                .contains("cleanup acknowledgment failed")));
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertTrue(driver.closed);
    }

    @Test
    void missingCheckpointFailsWithoutExecutingFreshJob() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "42");
        options.put(ApplicationOptions.JOB_ID.key(), "43");
        LocalDriver driver = new LocalDriver("missing-checkpoint");
        Exception failure =
                assertThrows(
                        Exception.class,
                        () ->
                                runApplication(
                                        "missing-checkpoint",
                                        specification(JOB, 30000, options),
                                        driver,
                                        engineConfig()));
        assertTrue(failure.getMessage().contains("No checkpoint found"));
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
        assertEquals(1, driver.releases);
        assertTrue(driver.closed);
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason = "Local Hadoop checkpoint storage requires winutils on Windows")
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
        LocalDriver driver = new LocalDriver("checkpoint-" + specification.getJobId());
        AtomicReference<Exception> outcome = new AtomicReference<>();
        Thread application =
                new Thread(
                        () -> {
                            try {
                                runApplication(
                                        "checkpoint-" + specification.getJobId(),
                                        specification,
                                        driver,
                                        engineConfig());
                            } catch (Exception failure) {
                                outcome.set(failure);
                            }
                        });
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
                                    : outcome.get().getMessage());
            if (failWorker) {
                // The external driver can report failure before Hazelcast or the native job does.
                // Cleanup then cancels that running job; its persisted state must remain usable.
                driver.events.onWorkerTerminated(
                        new ResourceID("local-worker"), "simulated platform failure");
                application.join(30000);
            }
        } finally {
            application.interrupt();
            application.join(30000);
        }
        assertFalse(application.isAlive());
        assertEquals(
                failWorker ? ApplicationStatus.FAILED : ApplicationStatus.CANCELED,
                driver.finalStatus,
                outcome.get() == null ? "" : outcome.get().getMessage());
        assertNotNull(outcome.get());
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
    void serverEntryPointOnlyCreatesTheConfiguredServer() {
        try (MockedStatic<SeaTunnelServerStarter> starter =
                mockStatic(SeaTunnelServerStarter.class)) {
            starter.when(() -> SeaTunnelServerStarter.main(any(String[].class)))
                    .thenCallRealMethod();
            SeaTunnelServerStarter.main(new String[0]);
            starter.verify(SeaTunnelServerStarter::createHazelcastInstance);
            starter.verify(() -> SeaTunnelServerStarter.main(any(String[].class)));
            starter.verifyNoMoreInteractions();
        }
    }

    @Test
    void applicationWorkerIsStoppedByItsOwner() throws Exception {
        String applicationId = "worker-lifecycle-" + UUID.randomUUID();
        String clusterName = SeatunnelApplicationConfig.clusterName(applicationId);
        SeaTunnelConfig masterConfig = engineConfig();
        SeatunnelApplicationConfig.configure(masterConfig, clusterName, null, 2);
        HazelcastInstance master = SeaTunnelServerStarter.createHazelcastInstance(masterConfig);
        HazelcastInstance worker = null;
        try {
            String address =
                    master.getCluster().getLocalMember().getAddress().getHost()
                            + ":"
                            + master.getCluster().getLocalMember().getAddress().getPort();
            SeaTunnelConfig workerConfig = engineConfig();
            worker = startWorker(clusterName, address, 2, workerConfig, JarPathResolver.identity());
            assertEquals(
                    "true",
                    workerConfig
                            .getHazelcastConfig()
                            .getProperty("hazelcast.shutdownhook.enabled"));
            assertEquals(
                    "GRACEFUL",
                    workerConfig.getHazelcastConfig().getProperty("hazelcast.shutdownhook.policy"));
            assertTrue(worker.getCluster().getLocalMember().isLiteMember());
            assertEquals(2, worker.getCluster().getMembers().size());
            assertEquals(2, workerConfig.getEngineConfig().getSlotServiceConfig().getSlotNum());
            assertTrue(worker.getLifecycleService().isRunning());
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClusterName(clusterName);
            clientConfig.getNetworkConfig().setAddresses(Collections.singletonList(address));
            try (SeaTunnelClient nativeClient = new SeaTunnelClient(clientConfig)) {
                assertEquals(
                        JobStatus.UNKNOWABLE.name(), nativeClient.getJobStatus(Long.MAX_VALUE));
            }
            assertTrue(master.getLifecycleService().isRunning());
            assertTrue(worker.getLifecycleService().isRunning());
            worker.shutdown();
            assertFalse(worker.getLifecycleService().isRunning());
            assertTrue(master.getLifecycleService().isRunning());
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
        SeatunnelApplicationConfig.configure(masterConfig, clusterName, null, 2);
        HazelcastInstance master = SeaTunnelServerStarter.createHazelcastInstance(masterConfig);
        HazelcastInstance worker = null;
        try {
            worker =
                    startWorker(
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
            try (SeaTunnelChildFirstClassLoader loader =
                    (SeaTunnelChildFirstClassLoader)
                            server.getClassLoaderService()
                                    .getClassLoader(17L, Collections.singletonList(original))) {
                URL marker = loader.getResource("localized-artifact.txt");
                assertNotNull(marker);
                URLConnection connection = marker.openConnection();
                // This test-owned connection must not retain a global cached JarFile on Windows.
                connection.setUseCaches(false);
                try (InputStream resource = connection.getInputStream()) {
                    assertEquals('w', resource.read());
                }
            } finally {
                server.getClassLoaderService()
                        .releaseClassLoader(17L, Collections.singletonList(original));
            }
        } finally {
            if (worker != null) {
                worker.shutdown();
            }
            master.shutdown();
        }
        Files.delete(jar);
    }

    @Test
    void allocationFailureIsReportedAndDriverIsClosed() throws Exception {
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public CompletableFuture<ResourceID> requestWorker(
                            WorkerSpecification specification) {
                        CompletableFuture<ResourceID> failed = new CompletableFuture<>();
                        failed.completeExceptionally(
                                new IllegalStateException("allocation rejected"));
                        return failed;
                    }
                };
        Exception failure = assertThrows(Exception.class, () -> run(JOB, driver, 30000));
        assertTrue(failure.getMessage().contains("allocation rejected"));
        assertTrue(driver.closed);
    }

    @Test
    void canceledAllocationThatLaunchesLateIsDrainedBeforeMasterStops() throws Exception {
        CompletableFuture<ResourceID> pending = new CompletableFuture<>();
        AtomicReference<Supplier<String>> masterAddress = new AtomicReference<>();
        AtomicReference<ResourceEventHandler<ResourceID>> events = new AtomicReference<>();
        AtomicReference<HazelcastInstance> lateWorker = new AtomicReference<>();
        AtomicBoolean joinedExistingMaster = new AtomicBoolean();
        AtomicBoolean futureWasCanceledBeforeLaunch = new AtomicBoolean();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public void initialize(
                            ResourceEventHandler<ResourceID> publisher,
                            ScheduledExecutorService mainThreadExecutor,
                            Executor ioExecutor,
                            Supplier<String> address) {
                        super.initialize(publisher, mainThreadExecutor, ioExecutor, address);
                        masterAddress.set(address);
                        events.set(publisher);
                    }

                    @Override
                    public CompletableFuture<ResourceID> requestWorker(
                            WorkerSpecification specification) {
                        events.get()
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
                                startWorker(
                                        SeatunnelApplicationConfig.clusterName("test-application"),
                                        masterAddress.get().get(),
                                        2,
                                        engineConfig(),
                                        JarPathResolver.identity());
                        lateWorker.set(worker);
                        joinedExistingMaster.set(
                                worker.getCluster().getMembers().stream()
                                        .anyMatch(member -> !member.isLiteMember()));
                        pending.complete(new ResourceID("late-worker"));
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
        Exception failure = assertThrows(Exception.class, () -> run(JOB, driver, 30000));
        assertTrue(failure.getMessage().contains("stop while allocation is pending"));
        assertTrue(futureWasCanceledBeforeLaunch.get());
        assertTrue(
                joinedExistingMaster.get(),
                "Master must remain available until pending allocations are drained");
        assertFalse(lateWorker.get().getLifecycleService().isRunning());
        assertTrue(driver.closed);
    }

    @Test
    void driverInitializationIsBoundedByTheStartupTimeout() throws Exception {
        AtomicBoolean initializationStopped = new AtomicBoolean();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public void initialize(
                            ResourceEventHandler<ResourceID> events,
                            ScheduledExecutorService mainThreadExecutor,
                            Executor ioExecutor,
                            Supplier<String> masterAddress) {
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException e) {
                            initializationStopped.set(true);
                            Thread.currentThread().interrupt();
                        }
                    }
                };
        Exception failure = assertThrows(Exception.class, () -> run(JOB, driver, 500));
        assertTrue(failure.getMessage().contains("Timed out"));
        assertTrue(initializationStopped.get());
        assertTrue(driver.closed);
        assertEquals(ApplicationStatus.FAILED, driver.finalStatus);
    }

    @Test
    void pendingAllocationTimesOutAndIsCanceled() throws Exception {
        CompletableFuture<ResourceID> pending = new CompletableFuture<>();
        LocalDriver driver =
                new LocalDriver() {
                    @Override
                    public CompletableFuture<ResourceID> requestWorker(
                            WorkerSpecification specification) {
                        return pending;
                    }
                };
        Exception failure = assertThrows(Exception.class, () -> run(JOB, driver, 5000));
        assertTrue(failure.getMessage().contains("Timed out"));
        assertTrue(pending.isCancelled());
        assertTrue(driver.closed);
    }

    @Test
    void interruptedApplicationPublishesCanceledAndCleansPendingAllocation() throws Exception {
        CountDownLatch requested = new CountDownLatch(1);
        CompletableFuture<ResourceID> pending = new CompletableFuture<>();
        LocalDriver driver =
                new LocalDriver("interrupted") {
                    @Override
                    public CompletableFuture<ResourceID> requestWorker(
                            WorkerSpecification specification) {
                        requested.countDown();
                        return pending;
                    }
                };
        ApplicationSpecification specification = specification(JOB, 30000);
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Exception> outcome = new AtomicReference<>();
        Thread application =
                new Thread(
                        () -> {
                            try {
                                runApplication(
                                        "interrupted", specification, driver, engineConfig());
                            } catch (Exception failure) {
                                outcome.set(failure);
                                interruptRestored.set(Thread.currentThread().isInterrupted());
                            }
                        });
        application.start();
        try {
            assertTrue(requested.await(30, TimeUnit.SECONDS));
            application.interrupt();
            application.join(30000);
            assertFalse(application.isAlive());
            assertTrue(outcome.get() instanceof InterruptedException);
            assertTrue(interruptRestored.get());
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
                ApplicationJobRunner.applicationStatus(JobStatus.FINISHED));
        assertEquals(
                ApplicationStatus.SUCCEEDED,
                ApplicationJobRunner.applicationStatus(JobStatus.SAVEPOINT_DONE));
        assertEquals(
                ApplicationStatus.CANCELED,
                ApplicationJobRunner.applicationStatus(JobStatus.CANCELED));
        assertEquals(
                ApplicationStatus.FAILED, ApplicationJobRunner.applicationStatus(JobStatus.FAILED));
        assertEquals(
                ApplicationStatus.FAILED,
                ApplicationJobRunner.applicationStatus(JobStatus.UNKNOWABLE));
    }

    private void run(String config, LocalDriver driver, long timeout) throws Exception {
        runApplication("test-application", specification(config, timeout), driver, engineConfig());
    }

    private void runApplication(
            String id,
            ApplicationSpecification specification,
            LocalDriver driver,
            SeaTunnelConfig config)
            throws Exception {
        String clusterName = SeatunnelApplicationConfig.clusterName(id);
        assertEquals(clusterName, driver.clusterName);
        SeatunnelApplicationConfig.configure(
                config, clusterName, null, specification.getWorkerSpecification().getSlots());
        SeatunnelApplicationConfig.configureCheckpointRetention(config);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .setPort(specification.getMasterPort())
                .setPortAutoIncrement(false);
        HazelcastInstanceImpl master =
                SeaTunnelServerStarter.createHazelcastInstance(
                        config,
                        null,
                        JarPathResolver.identity(),
                        new ResourceManagerFactory(
                                DeployType.KUBERNETES, id, specification, driver));
        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        try {
            new ApplicationJobRunner(server, specification).run();
            assertTrue(
                    master.getLifecycleService().isRunning(),
                    "The runner must leave master shutdown to its caller");
        } finally {
            boolean interrupted = Thread.interrupted();
            try {
                if (server.getCoordinatorService().getInitializedResourceManager() == null) {
                    driver.close();
                }
            } finally {
                master.shutdown();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
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
        options.put(ApplicationOptions.NAME.key(), "application-test");
        return SeatunnelApplicationConfig.parse(
                ConfigFactory.parseString(config)
                        .resolve()
                        .root()
                        .render(ConfigRenderOptions.concise()),
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

    private static HazelcastInstanceImpl startWorker(
            String clusterName,
            String address,
            int slots,
            SeaTunnelConfig config,
            JarPathResolver resolver) {
        SeatunnelApplicationConfig.configure(config, clusterName, address, slots);
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "true");
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.policy", "GRACEFUL");
        return SeaTunnelServerStarter.createHazelcastInstance(
                config, null, resolver, new ResourceManagerFactory());
    }

    private static class LocalDriver implements ResourceManagerDriver<ResourceID> {
        private final String clusterName;
        private Supplier<String> masterAddress;
        private ResourceEventHandler<ResourceID> events;
        private HazelcastInstance worker;
        private int requests;
        private int releases;
        private boolean closed;
        private ApplicationStatus finalStatus;

        private LocalDriver() {
            this("test-application");
        }

        private LocalDriver(String applicationId) {
            this.clusterName = SeatunnelApplicationConfig.clusterName(applicationId);
        }

        @Override
        public void initialize(
                ResourceEventHandler<ResourceID> events,
                ScheduledExecutorService mainThreadExecutor,
                Executor ioExecutor,
                Supplier<String> masterAddress) {
            this.masterAddress = masterAddress;
            this.events = events;
        }

        @Override
        public CompletableFuture<ResourceID> requestWorker(WorkerSpecification specification) {
            requests++;
            worker =
                    startWorker(
                            clusterName,
                            masterAddress.get(),
                            specification.getSlots(),
                            engineConfig(),
                            JarPathResolver.identity());
            return CompletableFuture.completedFuture(new ResourceID("local-worker"));
        }

        @Override
        public CompletableFuture<Void> releaseWorker(ResourceID registration) {
            releases++;
            worker.shutdown();
            return CompletableFuture.completedFuture(null);
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
