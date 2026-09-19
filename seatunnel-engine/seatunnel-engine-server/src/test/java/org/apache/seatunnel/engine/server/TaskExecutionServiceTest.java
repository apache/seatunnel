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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.BlockTask;
import org.apache.seatunnel.engine.server.execution.ExceptionTestTask;
import org.apache.seatunnel.engine.server.execution.FixedCallTestTimeTask;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.StopTimeTestTask;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.TaskTracker;
import org.apache.seatunnel.engine.server.execution.TestTask;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.CANCELED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FAILED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FINISHED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskExecutionServiceTest extends AbstractSeaTunnelServerTest {

    static FlakeIdGenerator FLAKE_ID_GENERATOR;
    long taskRunTime = 2000;
    long jobId = 10001;
    int pipeLineId = 100001;

    @BeforeAll
    public void before() {
        super.before();
        FLAKE_ID_GENERATOR = instance.getFlakeIdGenerator("test");
    }

    private PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
            TaskExecutionService taskExecutionService, @NonNull TaskGroup taskGroup) {
        Long taskId = taskGroup.getTasks().iterator().next().getTaskID();
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        classLoaders.put(taskId, Thread.currentThread().getContextClassLoader());
        return taskExecutionService.deployLocalTask(
                FLAKE_ID_GENERATOR.newId(),
                taskGroup,
                classLoaders,
                new ConcurrentHashMap<>(),
                () -> {},
                failure -> {});
    }

    @Test
    public void testCancel() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        TaskGroupDefaultImpl ts =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(testTask1, testTask2));
        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, ts);

        taskExecutionService.cancelTaskGroup(ts.getTaskGroupLocation());

        await().atMost(sleepTime + 10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertEquals(CANCELED, completableFuture.get().getExecutionState()));
    }

    @Test
    public void testCancelBlockTask() throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        BlockTask testTask1 = new BlockTask();
        BlockTask testTask2 = new BlockTask();

        TaskGroupDefaultImpl ts =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(testTask1, testTask2));
        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, ts);

        Thread.sleep(5000);

        taskExecutionService.cancelTaskGroup(ts.getTaskGroupLocation());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertEquals(CANCELED, completableFuture.get().getExecutionState()));
    }

    @Test
    public void testFinish() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        final CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "ts",
                                Lists.newArrayList(testTask1, testTask2)));
        completableFuture.whenComplete((unused, throwable) -> futureMark.set(true));
        stop.set(true);

        await().atMost(sleepTime + 10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(FINISHED, completableFuture.get().getExecutionState());
                        });
        assertTrue(futureMark.get());
    }

    @Test
    public void testClassloaderSplit() throws IOException {
        File console = File.createTempFile("console", ".jar");
        File fake = File.createTempFile("fake", ".jar");
        String consoleFile = console.toURI().toURL().toString();
        String fakeFile = fake.toURI().toURL().toString();

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        long jobId = System.currentTimeMillis();

        TaskGroupLocation location = new TaskGroupLocation(jobId, 1, 1);
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                new TaskGroupImmutableInformation(
                        jobId,
                        1,
                        TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                        location,
                        "testClassloaderSplit",
                        Arrays.asList(
                                nodeEngine.getSerializationService().toData(testTask1),
                                nodeEngine.getSerializationService().toData(testTask2)),
                        Arrays.asList(
                                Collections.singleton(new URL(fakeFile)),
                                Collections.singleton(new URL(consoleFile))),
                        Arrays.asList(emptySet(), emptySet()));

        Data data = nodeEngine.getSerializationService().toData(taskGroupImmutableInformation);

        final TaskDeployState taskDeployState = taskExecutionService.deployTask(data);

        Assertions.assertEquals(TaskDeployState.success(), taskDeployState);

        TaskGroupContext taskGroupContext =
                taskExecutionService.getActiveExecutionContext(location);
        Assertions.assertIterableEquals(
                Collections.singleton(new URL(fakeFile)),
                taskGroupContext.getJars().get(testTask1.getTaskID()));
        Assertions.assertIterableEquals(
                Collections.singleton(new URL(consoleFile)),
                taskGroupContext.getJars().get(testTask2.getTaskID()));

        Assertions.assertIterableEquals(
                Collections.singletonList(new URL(fakeFile)),
                Arrays.asList(
                        ((URLClassLoader) taskGroupContext.getClassLoader(testTask1.getTaskID()))
                                .getURLs()));
        Assertions.assertIterableEquals(
                Collections.singletonList(new URL(consoleFile)),
                Arrays.asList(
                        ((URLClassLoader) taskGroupContext.getClassLoader(testTask2.getTaskID()))
                                .getURLs()));

        taskExecutionService.cancelTaskGroup(location);

        fake.delete();
        console.delete();
    }

    /**
     * Verifies that a partially constructed task group does not retain classloader references when
     * a later task cannot be deserialized.
     */
    @Test
    public void testDeployTaskReleasesClassLoadersWhenDeserializationFails() throws IOException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        DefaultClassLoaderService classLoaderService =
                (DefaultClassLoaderService) server.getClassLoaderService();

        File testJar = File.createTempFile("failed-deployment", ".jar");
        testJar.deleteOnExit();
        URL testJarUrl = testJar.toURI().toURL();
        Set<URL> testJars = Collections.singleton(testJarUrl);
        long testJobId = System.currentTimeMillis();
        TestTask validTask = new TestTask(new AtomicBoolean(false), 300, true);
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                new TaskGroupImmutableInformation(
                        testJobId,
                        1,
                        TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                        new TaskGroupLocation(testJobId, 1, 1),
                        "testDeployTaskReleasesClassLoadersWhenDeserializationFails",
                        Arrays.asList(
                                nodeEngine.getSerializationService().toData(validTask),
                                nodeEngine.getSerializationService().toData("not a task")),
                        Arrays.asList(testJars, testJars),
                        Arrays.asList(emptySet(), emptySet()));

        TaskDeployState taskDeployState =
                taskExecutionService.deployTask(taskGroupImmutableInformation);

        Assertions.assertFalse(taskDeployState.isSuccess());
        Assertions.assertThrows(
                TaskGroupContextNotFoundException.class,
                () ->
                        taskExecutionService.getActiveExecutionContext(
                                taskGroupImmutableInformation.getTaskGroupLocation()));
        Assertions.assertTrue(
                classLoaderService.queryClassLoaderById(testJobId, testJars).isPresent());
        Assertions.assertEquals(
                0, classLoaderService.queryClassLoaderReferenceCount(testJobId, testJars));
        testJar.delete();
    }

    /**
     * Verifies that a failure before context publication releases the acquired classloader and is
     * still reported to the master.
     */
    @Test
    public void testDeployTaskHandlesFailureBeforeContextPublication() throws IOException {
        TaskExecutionService taskExecutionService = Mockito.spy(server.getTaskExecutionService());
        Mockito.doNothing()
                .when(taskExecutionService)
                .notifyTaskStatusToMaster(Mockito.any(), Mockito.any());
        DefaultClassLoaderService classLoaderService =
                (DefaultClassLoaderService) server.getClassLoaderService();

        File testJar = File.createTempFile("failed-context-publication", ".jar");
        testJar.deleteOnExit();
        URL testJarUrl = testJar.toURI().toURL();
        Set<URL> testJars = Collections.singleton(testJarUrl);
        long testJobId = System.currentTimeMillis();
        TaskGroupLocation location = new TaskGroupLocation(testJobId, 1, 1);
        Task task = new ContextInitializationFailureTask();
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                new TaskGroupImmutableInformation(
                        testJobId,
                        1,
                        TaskGroupType.DEFAULT,
                        location,
                        "testDeployTaskHandlesFailureBeforeContextPublication",
                        Collections.singletonList(
                                nodeEngine.getSerializationService().toData(task)),
                        Collections.singletonList(testJars),
                        Collections.singletonList(emptySet()));

        TaskDeployState taskDeployState =
                taskExecutionService.deployTask(taskGroupImmutableInformation);

        Assertions.assertEquals(TaskDeployState.success(), taskDeployState);
        Assertions.assertThrows(
                TaskGroupContextNotFoundException.class,
                () -> taskExecutionService.getActiveExecutionContext(location));
        Assertions.assertTrue(
                classLoaderService.queryClassLoaderById(testJobId, testJars).isPresent());
        Assertions.assertEquals(
                0, classLoaderService.queryClassLoaderReferenceCount(testJobId, testJars));
        Mockito.verify(taskExecutionService, Mockito.timeout(5000))
                .notifyTaskStatusToMaster(
                        Mockito.eq(location),
                        Mockito.argThat(state -> state.getExecutionState() == FAILED));
        testJar.delete();
    }

    /** Test task execution time is the same as the timer timeout */
    @Test
    public void testCriticalCallTime() throws InterruptedException {
        AtomicBoolean stopMark = new AtomicBoolean(false);
        CopyOnWriteArrayList<Long> stopTime = new CopyOnWriteArrayList<>();

        int count = 100;

        // Must be the same as the timer timeout
        int callTime = 50;

        // Create tasks with critical delays
        List<Task> criticalTask = buildStopTestTask(callTime, count, stopMark, stopTime);

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> taskCts =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t1",
                                Lists.newArrayList(criticalTask)));

        // Run it for a while
        Thread.sleep(taskRunTime);

        // stop task
        stopMark.set(true);

        // Check all task ends right
        await().atMost(count * callTime, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, taskCts.get().getExecutionState()));

        // Check that each Task is only Done once
        assertEquals(count, stopTime.size());
    }

    @Test
    public void testThrowException() throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        AtomicBoolean stopMark = new AtomicBoolean(false);

        long t1Sleep = 100;
        long t2Sleep = 50;

        long lowLagSleep = 50;
        long highLagSleep = 300;

        List<Throwable> t1throwable = new ArrayList<>();
        ExceptionTestTask t1 = new ExceptionTestTask(t1Sleep, "t1", t1throwable);

        List<Throwable> t2throwable = new ArrayList<>();
        ExceptionTestTask t2 = new ExceptionTestTask(t2Sleep, "t2", t2throwable);

        // Create low lat tasks
        List<Task> lowLagTask =
                buildFixedTestTask(lowLagSleep, 10, stopMark, new CopyOnWriteArrayList<>());

        // Create high lat tasks
        List<Task> highLagTask =
                buildFixedTestTask(highLagSleep, 5, stopMark, new CopyOnWriteArrayList<>());

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        CompletableFuture<TaskExecutionState> taskCts =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "ts",
                                Lists.newArrayList(tasks)));

        CompletableFuture<TaskExecutionState> t1c =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t1",
                                Lists.newArrayList(t1)));

        CompletableFuture<TaskExecutionState> t2c =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t2",
                                Lists.newArrayList(t2)));

        Thread.sleep(taskRunTime);

        t1throwable.add(new IOException());
        t2throwable.add(new IOException());

        await().atMost(t1Sleep + t2Sleep + 1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(FAILED, t1c.get().getExecutionState());
                            assertEquals(FAILED, t2c.get().getExecutionState());
                        });

        stopMark.set(true);

        await().atMost(lowLagSleep * 10 + highLagSleep + 1000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, taskCts.get().getExecutionState()));
    }

    @RepeatedTest(2)
    public void testDelay() throws InterruptedException {

        long lowLagSleep = 10;
        long highLagSleep = 300;

        AtomicBoolean stopMark = new AtomicBoolean(false);

        CopyOnWriteArrayList<Long> lowLagList = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> highLagList = new CopyOnWriteArrayList<>();

        // Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(lowLagSleep, 10, stopMark, lowLagList);

        // Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(highLagSleep, 5, stopMark, highLagList);

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        TaskGroupDefaultImpl taskGroup =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(tasks));

        LOGGER.info("task size is : " + taskGroup.getTasks().size());

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, taskGroup);

        // stop tasks
        Thread.sleep(taskRunTime);
        stopMark.set(true);

        // Check all task ends right
        await().atMost(lowLagSleep * 100 + highLagSleep * 50, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertEquals(FINISHED, completableFuture.get().getExecutionState()));

        // Computation Delay
        double lowAvg = lowLagList.stream().mapToLong(x -> x).average().getAsDouble();
        double highAvg = highLagList.stream().mapToLong(x -> x).average().getAsDouble();

        assertTrue(lowAvg < highLagSleep * 5);

        LOGGER.info("lowAvg : " + lowAvg);
        LOGGER.info("highAvg : " + highAvg);
    }

    /**
     * Verifies the {@code onContextPublished} half of the post-publication boundary in
     * apache/seatunnel#12164 (paired with {@link
     * #testDeployLocalTaskRollsBackAfterPartialBlockingSubmitRejection} which covers task
     * submission): a failure after context publication rolls back {@code executionContexts} and
     * {@code cancellationFutures}, so a later {@link TaskExecutionService#deployTask(Data)} for the
     * same {@link TaskGroupLocation} actually redeploys instead of hitting the master-failover skip
     * branch forever.
     */
    @Test
    public void testDeployLocalTaskRollsBackAfterPostPublishFailureAndAllowsRedeploy()
            throws Exception {
        TaskExecutionService taskExecutionService = Mockito.spy(server.getTaskExecutionService());
        Mockito.doNothing()
                .when(taskExecutionService)
                .notifyTaskStatusToMaster(Mockito.any(), Mockito.any());

        long testJobId = System.currentTimeMillis();
        TaskGroupLocation location = new TaskGroupLocation(testJobId, 1, 1);
        TestTask firstAttemptTask = new TestTask(new AtomicBoolean(false), 300, true);
        TaskGroupDefaultImpl firstAttemptGroup =
                new TaskGroupDefaultImpl(
                        location, "post-publish-failure", Lists.newArrayList(firstAttemptTask));

        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        classLoaders.put(
                firstAttemptTask.getTaskID(), Thread.currentThread().getContextClassLoader());
        ConcurrentHashMap<Long, Collection<URL>> jars = new ConcurrentHashMap<>();

        RejectedExecutionException publishFailure =
                new RejectedExecutionException("simulated executor rejection after publish");
        PassiveCompletableFuture<TaskExecutionState> failedFuture =
                taskExecutionService.deployLocalTask(
                        FLAKE_ID_GENERATOR.newId(),
                        firstAttemptGroup,
                        classLoaders,
                        jars,
                        () -> {
                            throw publishFailure;
                        },
                        failure -> {});

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertTrue(failedFuture.isCompletedExceptionally()));
        Assertions.assertThrows(
                TaskGroupContextNotFoundException.class,
                () -> taskExecutionService.getActiveExecutionContext(location));
        Assertions.assertFalse(
                hasCancellationFutureForLocation(taskExecutionService, location),
                "cancellation future must not leak after post-publish failure");
        ConcurrentMap<TaskGroupLocation, TaskGroupContext> finishedExecutionContexts =
                getField(taskExecutionService, "finishedExecutionContexts");
        Assertions.assertTrue(
                finishedExecutionContexts.containsKey(location),
                "rolled-back deployment must be recorded in finishedExecutionContexts");

        AtomicBoolean stop = new AtomicBoolean(false);
        ExecutionMarkerTask.reset();
        Task redeployTask = new ExecutionMarkerTask(stop);

        TaskGroupImmutableInformation redeployInfo =
                new TaskGroupImmutableInformation(
                        testJobId,
                        FLAKE_ID_GENERATOR.newId(),
                        TaskGroupType.DEFAULT,
                        location,
                        "post-publish-failure-redeploy",
                        Collections.singletonList(
                                nodeEngine.getSerializationService().toData(redeployTask)),
                        Collections.singletonList(emptySet()),
                        Collections.singletonList(emptySet()));
        Data redeployData = nodeEngine.getSerializationService().toData(redeployInfo);

        TaskDeployState redeployState = taskExecutionService.deployTask(redeployData);
        assertEquals(TaskDeployState.success(), redeployState);
        Assertions.assertNotNull(taskExecutionService.getActiveExecutionContext(location));

        await().atMost(10, TimeUnit.SECONDS).until(ExecutionMarkerTask::wasExecuted);
        Assertions.assertTrue(
                ExecutionMarkerTask.wasExecuted(),
                "second deployTask must actually execute after post-publish rollback");

        stop.set(true);
        taskExecutionService.cancelTaskGroup(location);
    }

    /**
     * Regression for the task-submission half of the post-publication boundary in
     * apache/seatunnel#12164 (paired with {@link
     * #testDeployLocalTaskRollsBackAfterPostPublishFailureAndAllowsRedeploy} which covers {@code
     * onContextPublished}): {@code submitBlockingTask} throws {@link RejectedExecutionException}
     * after at least one blocking worker was already accepted and after a thread-share task was
     * already enqueued.
     *
     * <p>Asserts the failed attempt is fully rolled back (no active context, no cancellation
     * future, no residual cooperative-queue work, no leaked classloader reference) and a later
     * {@link TaskExecutionService#deployTask(Data)} for the same {@link TaskGroupLocation} actually
     * executes rather than only returning success via the master-failover skip branch.
     */
    @Test
    public void testDeployLocalTaskRollsBackAfterPartialBlockingSubmitRejection() throws Exception {
        TaskExecutionService realService = server.getTaskExecutionService();
        TaskExecutionService taskExecutionService = Mockito.spy(realService);
        Mockito.doNothing()
                .when(taskExecutionService)
                .notifyTaskStatusToMaster(Mockito.any(), Mockito.any());

        ThreadShareMode previousMode =
                realService
                        .getSeaTunnelConfig()
                        .getEngineConfig()
                        .getTaskExecutionThreadShareMode();
        realService
                .getSeaTunnelConfig()
                .getEngineConfig()
                .setTaskExecutionThreadShareMode(ThreadShareMode.PART);

        AtomicInteger acceptedBlockingSubmits = new AtomicInteger();
        ExecutorService rejectingExecutor =
                new ThreadPoolExecutor(
                        2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    public Future<?> submit(Runnable task) {
                        if (acceptedBlockingSubmits.getAndIncrement() >= 1) {
                            throw new RejectedExecutionException(
                                    "reject after partial blocking submit");
                        }
                        return super.submit(task);
                    }
                };
        // Mockito.spy() may not share instance fields with the real object; set the override on
        // both so submitBlockingTask always sees the rejecting executor.
        setBlockingTaskExecutorOverride(realService, rejectingExecutor);
        setBlockingTaskExecutorOverride(taskExecutionService, rejectingExecutor);

        DefaultClassLoaderService classLoaderService =
                (DefaultClassLoaderService) server.getClassLoaderService();
        File testJar = File.createTempFile("post-publish-partial-submit", ".jar");
        testJar.deleteOnExit();
        URL testJarUrl = testJar.toURI().toURL();
        Set<URL> testJars = Collections.singleton(testJarUrl);

        long testJobId = System.currentTimeMillis();
        TaskGroupLocation location = new TaskGroupLocation(testJobId, 1, 1);
        PartialSubmitProbeTask.reset();
        PartialSubmitProbeTask shareTask = new PartialSubmitProbeTask(1L, true);
        PartialSubmitProbeTask firstBlockingTask = new PartialSubmitProbeTask(2L, false);
        PartialSubmitProbeTask secondBlockingTask = new PartialSubmitProbeTask(3L, false);

        TaskGroupImmutableInformation info =
                new TaskGroupImmutableInformation(
                        testJobId,
                        FLAKE_ID_GENERATOR.newId(),
                        TaskGroupType.DEFAULT,
                        location,
                        "partial-blocking-submit-rejection",
                        Arrays.asList(
                                nodeEngine.getSerializationService().toData(shareTask),
                                nodeEngine.getSerializationService().toData(firstBlockingTask),
                                nodeEngine.getSerializationService().toData(secondBlockingTask)),
                        Arrays.asList(testJars, testJars, testJars),
                        Arrays.asList(emptySet(), emptySet(), emptySet()));

        try {
            TaskDeployState deployState = taskExecutionService.deployTask(info);
            assertEquals(TaskDeployState.success(), deployState);
            Assertions.assertTrue(
                    acceptedBlockingSubmits.get() >= 2,
                    "test must reach a RejectedExecutionException after a successful blocking submit");

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertThrows(
                                            TaskGroupContextNotFoundException.class,
                                            () ->
                                                    taskExecutionService.getActiveExecutionContext(
                                                            location)));
            Assertions.assertFalse(
                    hasCancellationFutureForLocation(realService, location),
                    "cancellation future must not leak after partial-submit rollback");
            Assertions.assertFalse(
                    threadShareQueueContainsLocation(realService, location),
                    "cooperative queue must not retain trackers from the failed attempt");
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                int calls = PartialSubmitProbeTask.callCount();
                                try {
                                    Thread.sleep(200);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                Assertions.assertEquals(
                                        calls,
                                        PartialSubmitProbeTask.callCount(),
                                        "orphaned tasks from the failed attempt must stop running");
                            });
            Assertions.assertTrue(
                    classLoaderService.queryClassLoaderById(testJobId, testJars).isPresent());
            Assertions.assertEquals(
                    0,
                    classLoaderService.queryClassLoaderReferenceCount(testJobId, testJars),
                    "classloader references must be released by post-publish rollback");

            // Clear the rejecting executor and restore the original thread-share mode before
            // redeploy so the second attempt uses the normal worker pool.
            setBlockingTaskExecutorOverride(realService, null);
            setBlockingTaskExecutorOverride(taskExecutionService, null);
            realService
                    .getSeaTunnelConfig()
                    .getEngineConfig()
                    .setTaskExecutionThreadShareMode(previousMode);

            AtomicBoolean stop = new AtomicBoolean(false);
            ExecutionMarkerTask.reset();
            Task redeployTask = new ExecutionMarkerTask(stop);
            TaskGroupImmutableInformation redeployInfo =
                    new TaskGroupImmutableInformation(
                            testJobId,
                            FLAKE_ID_GENERATOR.newId(),
                            TaskGroupType.DEFAULT,
                            location,
                            "partial-submit-redeploy",
                            Collections.singletonList(
                                    nodeEngine.getSerializationService().toData(redeployTask)),
                            Collections.singletonList(emptySet()),
                            Collections.singletonList(emptySet()));
            Data redeployData = nodeEngine.getSerializationService().toData(redeployInfo);

            // Use deployTask (not deployLocalTask) so this assertion exercises the same
            // executionContexts.containsKey skip branch that permanently blocked redeploy before
            // the rollback fix.
            TaskDeployState redeployState = taskExecutionService.deployTask(redeployData);
            assertEquals(TaskDeployState.success(), redeployState);
            Assertions.assertNotNull(taskExecutionService.getActiveExecutionContext(location));
            await().atMost(10, TimeUnit.SECONDS).until(ExecutionMarkerTask::wasExecuted);
            Assertions.assertTrue(
                    ExecutionMarkerTask.wasExecuted(),
                    "second deployTask must actually execute after partial-submit rollback");
            stop.set(true);
            taskExecutionService.cancelTaskGroup(location);
        } finally {
            setBlockingTaskExecutorOverride(realService, null);
            setBlockingTaskExecutorOverride(taskExecutionService, null);
            rejectingExecutor.shutdownNow();
            realService
                    .getSeaTunnelConfig()
                    .getEngineConfig()
                    .setTaskExecutionThreadShareMode(previousMode);
            testJar.delete();
        }
    }

    private static void setBlockingTaskExecutorOverride(
            TaskExecutionService taskExecutionService, ExecutorService override) throws Exception {
        Field field = TaskExecutionService.class.getDeclaredField("blockingTaskExecutorOverride");
        field.setAccessible(true);
        field.set(taskExecutionService, override);
    }

    @SuppressWarnings("unchecked")
    private static boolean hasCancellationFutureForLocation(
            TaskExecutionService taskExecutionService, TaskGroupLocation location)
            throws Exception {
        Field field = TaskExecutionService.class.getDeclaredField("cancellationFutures");
        field.setAccessible(true);
        ConcurrentMap<TaskGroupContext, ?> map =
                (ConcurrentMap<TaskGroupContext, ?>) field.get(taskExecutionService);
        return map.keySet().stream()
                .anyMatch(
                        context -> location.equals(context.getTaskGroup().getTaskGroupLocation()));
    }

    @SuppressWarnings("unchecked")
    private static boolean threadShareQueueContainsLocation(
            TaskExecutionService taskExecutionService, TaskGroupLocation location)
            throws Exception {
        Field queueField = TaskExecutionService.class.getDeclaredField("threadShareTaskQueue");
        queueField.setAccessible(true);
        java.util.concurrent.BlockingDeque<TaskTracker> queue =
                (java.util.concurrent.BlockingDeque<TaskTracker>)
                        queueField.get(taskExecutionService);
        Field taskGroupField =
                TaskExecutionService.TaskGroupExecutionTracker.class.getDeclaredField("taskGroup");
        taskGroupField.setAccessible(true);
        for (TaskTracker tracker : queue) {
            TaskGroup taskGroup = (TaskGroup) taskGroupField.get(tracker.taskGroupExecutionTracker);
            if (location.equals(taskGroup.getTaskGroupLocation())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verifies that {@link TaskExecutionService#deployTask(Data)} is idempotent when the
     * TaskGroupLocation is already present in {@code executionContexts} (task actively running).
     *
     * <p>During master failover, the new master restores job state from the IMap and calls {@code
     * deployTask()} for every task group it finds in RUNNING or DEPLOYING state. Those task groups
     * may still be executing on the worker. Before this fix a second {@code deployTask()} call for
     * the same location threw {@code RuntimeException("TaskGroupLocation: ... already exists")},
     * causing the job to enter an infinite FAILED/restore loop. After this fix the call returns
     * {@link TaskDeployState#success()} without interrupting the running task, allowing the master
     * to reconnect normally.
     */
    @Test
    public void testDeployTaskIdempotentWhenAlreadyRunning() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, 500, true);
        TestTask testTask2 = new TestTask(stop, 500, false);

        long testJobId = System.currentTimeMillis();
        TaskGroupLocation location = new TaskGroupLocation(testJobId, 1, 1);

        TaskGroupImmutableInformation info =
                new TaskGroupImmutableInformation(
                        testJobId,
                        1,
                        TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                        location,
                        "idempotency-test",
                        Arrays.asList(
                                nodeEngine.getSerializationService().toData(testTask1),
                                nodeEngine.getSerializationService().toData(testTask2)),
                        Arrays.asList(emptySet(), emptySet()),
                        Arrays.asList(emptySet(), emptySet()));

        Data data = nodeEngine.getSerializationService().toData(info);

        // First deploy — must succeed normally.
        TaskDeployState firstResult = taskExecutionService.deployTask(data);
        assertEquals(TaskDeployState.success(), firstResult);
        Assertions.assertNotNull(taskExecutionService.getActiveExecutionContext(location));

        // Second deploy while task is still active — simulates master-failover re-deploy.
        // Before this fix this threw RuntimeException("TaskGroupLocation: ... already exists").
        TaskDeployState secondResult = taskExecutionService.deployTask(data);
        assertEquals(TaskDeployState.success(), secondResult);

        // The original task group must still be active — not interrupted by the second deploy.
        Assertions.assertNotNull(taskExecutionService.getActiveExecutionContext(location));

        stop.set(true);
        taskExecutionService.cancelTaskGroup(location);
    }

    @Test
    public void testStaleTaskDoneCleansOnlyOwnedGenerationResources() throws Exception {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation location = newTaskGroupLocation();
        Task oldTask = new TestTask(new AtomicBoolean(true), 0, true);
        TaskGroup oldTaskGroup =
                new TaskGroupDefaultImpl(location, "old-generation", Lists.newArrayList(oldTask));
        TaskGroup newTaskGroup =
                new TaskGroupDefaultImpl(
                        location,
                        "new-generation",
                        Lists.newArrayList(new TestTask(new AtomicBoolean(true), 0, true)));
        TaskGroupContext oldContext = newTaskGroupContext(1L, oldTaskGroup);
        TaskGroupContext newContext = newTaskGroupContext(2L, newTaskGroup);
        CompletableFuture<Void> oldCancellationFuture = new CompletableFuture<>();
        CompletableFuture<Void> newCancellationFuture = new CompletableFuture<>();
        CompletableFuture<TaskExecutionState> oldResultFuture = new CompletableFuture<>();
        TaskExecutionService.TaskGroupExecutionTracker oldTracker =
                taskExecutionService
                .new TaskGroupExecutionTracker(oldCancellationFuture, oldContext, oldResultFuture);

        CompletableFuture<?> oldAsyncFuture = new CompletableFuture<>();
        CompletableFuture<?> newAsyncFuture = new CompletableFuture<>();
        Map<String, CompletableFuture<?>> oldAsyncFutures = new ConcurrentHashMap<>();
        Map<String, CompletableFuture<?>> newAsyncFutures = new ConcurrentHashMap<>();
        oldAsyncFutures.put("old-generation-async", oldAsyncFuture);
        newAsyncFutures.put("new-generation-async", newAsyncFuture);
        TaskLocation oldTaskLocation = new TaskLocation(location, oldTask.getTaskID(), 0);
        TaskLocation newTaskLocation =
                new TaskLocation(
                        location, newTaskGroup.getTasks().iterator().next().getTaskID(), 0);
        ScheduledFuture<?> oldTimerFlushFuture = newPendingScheduledFuture();
        ScheduledFuture<?> newTimerFlushFuture = newPendingScheduledFuture();
        ConcurrentMap<TaskLocation, ScheduledFuture<?>> oldTimerFlushFutures =
                new ConcurrentHashMap<>();
        ConcurrentMap<TaskLocation, ScheduledFuture<?>> newTimerFlushFutures =
                new ConcurrentHashMap<>();
        oldTimerFlushFutures.put(oldTaskLocation, oldTimerFlushFuture);
        newTimerFlushFutures.put(newTaskLocation, newTimerFlushFuture);

        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                getField(taskExecutionService, "executionContexts");
        ConcurrentMap<TaskGroupLocation, TaskGroupContext> finishedExecutionContexts =
                getField(taskExecutionService, "finishedExecutionContexts");
        ConcurrentMap<TaskGroupContext, CompletableFuture<Void>> cancellationFutures =
                getField(taskExecutionService, "cancellationFutures");
        ConcurrentMap<TaskGroupContext, Map<String, CompletableFuture<?>>> asyncFutures =
                getField(taskExecutionService, "taskAsyncFunctionFuture");
        ConcurrentMap<TaskGroupContext, ConcurrentMap<TaskLocation, ScheduledFuture<?>>>
                timerFlushFutures = getField(taskExecutionService, "timerFlushFutures");
        executionContexts.put(location, newContext);
        cancellationFutures.put(oldContext, oldCancellationFuture);
        cancellationFutures.put(newContext, newCancellationFuture);
        asyncFutures.put(oldContext, oldAsyncFutures);
        asyncFutures.put(newContext, newAsyncFutures);
        timerFlushFutures.put(oldContext, oldTimerFlushFutures);
        timerFlushFutures.put(newContext, newTimerFlushFutures);

        try {
            oldTracker.taskDone(oldTask);

            Assertions.assertSame(newContext, executionContexts.get(location));
            Assertions.assertFalse(finishedExecutionContexts.containsKey(location));
            assertEquals(1L, oldContext.getExecutionId());
            assertEquals(2L, newContext.getExecutionId());
            Assertions.assertNull(oldContext.getClassLoaders());
            Assertions.assertNotNull(newContext.getClassLoaders());
            Assertions.assertTrue(oldAsyncFuture.isCancelled());
            Mockito.verify(oldTimerFlushFuture).cancel(false);
            Assertions.assertFalse(newCancellationFuture.isCancelled());
            Assertions.assertFalse(cancellationFutures.containsKey(oldContext));
            Assertions.assertSame(newCancellationFuture, cancellationFutures.get(newContext));
            Assertions.assertFalse(newAsyncFuture.isCancelled());
            Mockito.verify(newTimerFlushFuture, Mockito.never()).cancel(false);
            assertEquals(FINISHED, oldResultFuture.get().getExecutionState());
        } finally {
            executionContexts.remove(location);
            cancellationFutures.remove(newContext);
            asyncFutures.remove(newContext);
            timerFlushFutures.remove(newContext);
            newAsyncFuture.cancel(true);
        }
    }

    @Test
    public void testTaskGroupContextEqualityUsesExecutionId() {
        TaskGroup taskGroup =
                new TaskGroupDefaultImpl(
                        newTaskGroupLocation(), "same-fields", Collections.emptyList());
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Collection<URL>> jars = new ConcurrentHashMap<>();
        TaskGroupContext first = new TaskGroupContext(1L, taskGroup, classLoaders, jars);
        TaskGroupContext sameExecution = new TaskGroupContext(1L, taskGroup, classLoaders, jars);
        // Long.hashCode(1L) and Long.hashCode(1L << 32) are both 1. Different executions must
        // remain distinct even when their hash codes collide.
        TaskGroupContext differentExecution =
                new TaskGroupContext(1L << 32, taskGroup, classLoaders, jars);
        Map<TaskGroupContext, String> contexts = new ConcurrentHashMap<>();
        contexts.put(first, "first");
        contexts.put(sameExecution, "same-execution");
        contexts.put(differentExecution, "different-execution");

        assertEquals(first, sameExecution);
        assertEquals(first.hashCode(), sameExecution.hashCode());
        Assertions.assertNotEquals(first, differentExecution);
        assertEquals(first.hashCode(), differentExecution.hashCode());
        assertEquals(2, contexts.size());
        assertEquals("same-execution", contexts.get(first));
        assertEquals("different-execution", contexts.get(differentExecution));
    }

    /**
     * Regression for the classloader-release race flagged in apache/seatunnel#12218: rollback and
     * normal completion must not both call {@code ClassLoaderService#releaseClassLoader} for the
     * same context, because the service's ref count is shared across task groups in the same job.
     */
    @Test
    public void testTaskGroupContextClassLoaderReleaseClaimIsAtomic() throws Exception {
        TaskGroupLocation location = newTaskGroupLocation();
        TaskGroup taskGroup =
                new TaskGroupDefaultImpl(
                        location,
                        "classloader-claim",
                        Lists.newArrayList(new TestTask(new AtomicBoolean(true), 0, true)));
        File testJar = File.createTempFile("classloader-claim", ".jar");
        testJar.deleteOnExit();
        Set<URL> testJars = Collections.singleton(testJar.toURI().toURL());
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Collection<URL>> jars = new ConcurrentHashMap<>();
        long taskId = taskGroup.getTasks().iterator().next().getTaskID();
        DefaultClassLoaderService classLoaderService =
                (DefaultClassLoaderService) server.getClassLoaderService();
        ClassLoader classLoader = classLoaderService.getClassLoader(location.getJobId(), testJars);
        classLoaders.put(taskId, classLoader);
        jars.put(taskId, testJars);
        // Sibling task group in the same job holding the same connector jars (shared ref count).
        classLoaderService.getClassLoader(location.getJobId(), testJars);
        assertEquals(
                2,
                classLoaderService.queryClassLoaderReferenceCount(location.getJobId(), testJars));

        TaskGroupContext context =
                new TaskGroupContext(FLAKE_ID_GENERATOR.newId(), taskGroup, classLoaders, jars);

        Map<Long, Collection<URL>> claimed = context.claimJarsForClassLoaderRelease();
        Assertions.assertNotNull(claimed);
        Assertions.assertNull(context.getClassLoaders());
        Assertions.assertNull(context.getJars());
        Assertions.assertNull(
                context.claimJarsForClassLoaderRelease(),
                "second claim must no-op so rollback and recycleClassLoader cannot double-release");

        for (Collection<URL> claimedJars : claimed.values()) {
            classLoaderService.releaseClassLoader(location.getJobId(), claimedJars);
        }
        // Only one decrement: sibling task group must still keep the classloader alive.
        assertEquals(
                1,
                classLoaderService.queryClassLoaderReferenceCount(location.getJobId(), testJars));
        Assertions.assertTrue(
                classLoaderService.queryClassLoaderById(location.getJobId(), testJars).isPresent());

        classLoaderService.releaseClassLoader(location.getJobId(), testJars);
        assertEquals(
                0,
                classLoaderService.queryClassLoaderReferenceCount(location.getJobId(), testJars));
    }

    @Test
    public void testRecycleClassLoaderAfterRollbackClaimDoesNotDoubleRelease() throws Exception {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation location = newTaskGroupLocation();
        Task task = new TestTask(new AtomicBoolean(true), 0, true);
        TaskGroup taskGroup =
                new TaskGroupDefaultImpl(
                        location, "rollback-then-recycle", Lists.newArrayList(task));
        File testJar = File.createTempFile("rollback-then-recycle", ".jar");
        testJar.deleteOnExit();
        Set<URL> testJars = Collections.singleton(testJar.toURI().toURL());
        DefaultClassLoaderService classLoaderService =
                (DefaultClassLoaderService) server.getClassLoaderService();
        ClassLoader classLoader = classLoaderService.getClassLoader(location.getJobId(), testJars);
        // Shared job-scoped ref as if another healthy task group still holds the jars.
        classLoaderService.getClassLoader(location.getJobId(), testJars);
        assertEquals(
                2,
                classLoaderService.queryClassLoaderReferenceCount(location.getJobId(), testJars));

        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Collection<URL>> jars = new ConcurrentHashMap<>();
        classLoaders.put(task.getTaskID(), classLoader);
        jars.put(task.getTaskID(), testJars);
        TaskGroupContext context =
                new TaskGroupContext(FLAKE_ID_GENERATOR.newId(), taskGroup, classLoaders, jars);
        CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();
        CompletableFuture<TaskExecutionState> resultFuture = new CompletableFuture<>();
        TaskExecutionService.TaskGroupExecutionTracker tracker =
                taskExecutionService
                .new TaskGroupExecutionTracker(cancellationFuture, context, resultFuture);

        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                getField(taskExecutionService, "executionContexts");
        ConcurrentMap<TaskGroupContext, CompletableFuture<Void>> cancellationFutures =
                getField(taskExecutionService, "cancellationFutures");
        executionContexts.put(location, context);
        cancellationFutures.put(context, cancellationFuture);

        try {
            // Simulate post-publish rollback claiming classloader release first.
            java.lang.reflect.Method releaseOnce =
                    TaskExecutionService.class.getDeclaredMethod(
                            "releaseClassLoadersOnce",
                            TaskGroupLocation.class,
                            TaskGroupContext.class);
            releaseOnce.setAccessible(true);
            releaseOnce.invoke(taskExecutionService, location, context);
            assertEquals(
                    1,
                    classLoaderService.queryClassLoaderReferenceCount(
                            location.getJobId(), testJars));

            // Normal completion path must observe the claim and must not decrement again.
            tracker.taskDone(task);
            assertEquals(
                    1,
                    classLoaderService.queryClassLoaderReferenceCount(
                            location.getJobId(), testJars));
            Assertions.assertTrue(
                    classLoaderService
                            .queryClassLoaderById(location.getJobId(), testJars)
                            .isPresent());
            assertEquals(FINISHED, resultFuture.get().getExecutionState());
        } finally {
            executionContexts.remove(location);
            cancellationFutures.remove(context);
            classLoaderService.releaseClassLoader(location.getJobId(), testJars);
        }
    }

    @Test
    public void testStaleFailedTaskDoneCleansOnlyOwnedGenerationResources() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation location = newTaskGroupLocation();
        Task oldTask1 = new TestTask(new AtomicBoolean(true), 0, true);
        Task oldTask2 = new TestTask(new AtomicBoolean(true), 0, true);
        TaskGroup oldTaskGroup =
                new TaskGroupDefaultImpl(
                        location, "old-generation", Lists.newArrayList(oldTask1, oldTask2));
        TaskGroup newTaskGroup =
                new TaskGroupDefaultImpl(
                        location,
                        "new-generation",
                        Lists.newArrayList(new TestTask(new AtomicBoolean(true), 0, true)));
        TaskGroupContext oldContext = newTaskGroupContext(1L, oldTaskGroup);
        TaskGroupContext newContext = newTaskGroupContext(2L, newTaskGroup);
        CompletableFuture<Void> oldCancellationFuture = new CompletableFuture<>();
        CompletableFuture<TaskExecutionState> oldResultFuture = new CompletableFuture<>();
        TaskExecutionService.TaskGroupExecutionTracker oldTracker =
                taskExecutionService
                .new TaskGroupExecutionTracker(oldCancellationFuture, oldContext, oldResultFuture);

        CompletableFuture<?> oldAsyncFuture = new CompletableFuture<>();
        CompletableFuture<?> newAsyncFuture = new CompletableFuture<>();
        Map<String, CompletableFuture<?>> oldAsyncFutures = new ConcurrentHashMap<>();
        Map<String, CompletableFuture<?>> newAsyncFutures = new ConcurrentHashMap<>();
        oldAsyncFutures.put("old-generation-async", oldAsyncFuture);
        newAsyncFutures.put("new-generation-async", newAsyncFuture);
        ScheduledFuture<?> oldTimerFlushFuture = newPendingScheduledFuture();
        ScheduledFuture<?> newTimerFlushFuture = newPendingScheduledFuture();
        ConcurrentMap<TaskLocation, ScheduledFuture<?>> oldTimerFlushFutures =
                new ConcurrentHashMap<>();
        ConcurrentMap<TaskLocation, ScheduledFuture<?>> newTimerFlushFutures =
                new ConcurrentHashMap<>();
        oldTimerFlushFutures.put(
                new TaskLocation(location, oldTask1.getTaskID(), 0), oldTimerFlushFuture);
        newTimerFlushFutures.put(
                new TaskLocation(
                        location, newTaskGroup.getTasks().iterator().next().getTaskID(), 0),
                newTimerFlushFuture);

        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                getField(taskExecutionService, "executionContexts");
        ConcurrentMap<TaskGroupContext, Map<String, CompletableFuture<?>>> asyncFutures =
                getField(taskExecutionService, "taskAsyncFunctionFuture");
        ConcurrentMap<TaskGroupContext, ConcurrentMap<TaskLocation, ScheduledFuture<?>>>
                timerFlushFutures = getField(taskExecutionService, "timerFlushFutures");
        executionContexts.put(location, newContext);
        asyncFutures.put(oldContext, oldAsyncFutures);
        asyncFutures.put(newContext, newAsyncFutures);
        timerFlushFutures.put(oldContext, oldTimerFlushFutures);
        timerFlushFutures.put(newContext, newTimerFlushFutures);

        try {
            oldTracker.exception(new RuntimeException("stale generation task failed"));
            oldTracker.taskDone(oldTask1);

            Assertions.assertSame(newContext, executionContexts.get(location));
            Assertions.assertTrue(oldAsyncFuture.isCancelled());
            Mockito.verify(oldTimerFlushFuture).cancel(false);
            Assertions.assertFalse(newAsyncFuture.isCancelled());
            Mockito.verify(newTimerFlushFuture, Mockito.never()).cancel(false);
            Assertions.assertFalse(oldResultFuture.isDone());
        } finally {
            executionContexts.remove(location);
            oldTracker.taskDone(oldTask2);
            asyncFutures.remove(newContext);
            timerFlushFutures.remove(newContext);
            newAsyncFuture.cancel(true);
        }
    }

    public List<Task> buildFixedTestTask(
            long callTime, long count, AtomicBoolean stopMart, CopyOnWriteArrayList<Long> lagList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(
                    new FixedCallTestTimeTask(callTime, callTime + "t" + i, stopMart, lagList));
        }
        return taskQueue;
    }

    private TaskGroupLocation newTaskGroupLocation() {
        return new TaskGroupLocation(
                System.currentTimeMillis(), pipeLineId, FLAKE_ID_GENERATOR.newId());
    }

    private static TaskGroupContext newTaskGroupContext(long executionId, TaskGroup taskGroup) {
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        ConcurrentHashMap<Long, Collection<URL>> jars = new ConcurrentHashMap<>();
        taskGroup
                .getTasks()
                .forEach(
                        task -> {
                            classLoaders.put(
                                    task.getTaskID(),
                                    Thread.currentThread().getContextClassLoader());
                            jars.put(task.getTaskID(), Collections.emptyList());
                        });
        return new TaskGroupContext(executionId, taskGroup, classLoaders, jars);
    }

    private static ScheduledFuture<?> newPendingScheduledFuture() {
        ScheduledFuture<?> future = Mockito.mock(ScheduledFuture.class);
        // Prefer doReturn(...) so stubbing does not invoke the mocked method.
        Mockito.doReturn(false).when(future).isDone();
        return future;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String fieldName) {
        return (T)
                ReflectionUtils.getField(target, fieldName)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Field " + fieldName + " not found on " + target));
    }

    public List<Task> buildStopTestTask(
            long callTime,
            long count,
            AtomicBoolean stopMart,
            CopyOnWriteArrayList<Long> stopList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(new StopTimeTestTask(callTime, stopList, stopMart));
        }
        return taskQueue;
    }

    @Test
    public void testRegisterTimerFlushRejectsNonPositiveInterval() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation groupLocation = new TaskGroupLocation(jobId, pipeLineId, 200L);
        TaskLocation taskLocation = new TaskLocation(groupLocation, 1L, 1);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> taskExecutionService.registerTimerFlushTask(taskLocation, () -> {}, 0L));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> taskExecutionService.registerTimerFlushTask(taskLocation, () -> {}, -1L));
    }

    @Test
    public void testRegisterAndCloseTimerFlushTask() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation groupLocation = new TaskGroupLocation(jobId, pipeLineId, 201L);
        TaskLocation taskLocation = new TaskLocation(groupLocation, 1L, 1);
        TaskGroupContext context = installTestContext(taskExecutionService, groupLocation);

        try {
            ScheduledFuture<?> future =
                    taskExecutionService.registerTimerFlushTask(taskLocation, () -> {}, 1_000L);
            Assertions.assertNotNull(future);
            Assertions.assertFalse(future.isCancelled());

            taskExecutionService.closeTimerFlushTask(taskLocation);
            Assertions.assertTrue(future.isCancelled());

            // Closing the last timer must not detach the deployment-level bucket. The owning
            // tracker is the only component allowed to remove that bucket during final cleanup.
            ConcurrentMap<TaskGroupContext, ConcurrentMap<TaskLocation, ScheduledFuture<?>>>
                    timerFlushFutures = getField(taskExecutionService, "timerFlushFutures");
            Assertions.assertTrue(timerFlushFutures.containsKey(context));
            Assertions.assertTrue(timerFlushFutures.get(context).isEmpty());

            // closing again is idempotent
            Assertions.assertDoesNotThrow(
                    () -> taskExecutionService.closeTimerFlushTask(taskLocation));
        } finally {
            removeTestContext(taskExecutionService, groupLocation, context);
        }
    }

    @Test
    public void testReRegisterTimerFlushCancelsPreviousFuture() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation groupLocation = new TaskGroupLocation(jobId, pipeLineId, 202L);
        TaskLocation taskLocation = new TaskLocation(groupLocation, 1L, 1);
        TaskGroupContext context = installTestContext(taskExecutionService, groupLocation);

        try {
            ScheduledFuture<?> first =
                    taskExecutionService.registerTimerFlushTask(taskLocation, () -> {}, 1_000L);
            ScheduledFuture<?> second =
                    taskExecutionService.registerTimerFlushTask(taskLocation, () -> {}, 2_000L);

            Assertions.assertNotSame(first, second);
            Assertions.assertTrue(
                    first.isCancelled(), "previous future must be cancelled on re-register");
            Assertions.assertFalse(second.isCancelled(), "new future must remain active");

            taskExecutionService.closeTimerFlushTask(taskLocation);
        } finally {
            removeTestContext(taskExecutionService, groupLocation, context);
        }
    }

    @Test
    public void testCloseTimerFlushOnUnknownLocationIsNoop() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        TaskGroupLocation groupLocation = new TaskGroupLocation(jobId, pipeLineId, 203L);
        TaskLocation unknown = new TaskLocation(groupLocation, 1L, 99);

        Assertions.assertDoesNotThrow(() -> taskExecutionService.closeTimerFlushTask(unknown));
    }

    private static TaskGroupContext installTestContext(
            TaskExecutionService taskExecutionService, TaskGroupLocation location) {
        TaskGroup taskGroup =
                new TaskGroupDefaultImpl(location, "timer-test", Collections.emptyList());
        TaskGroupContext context =
                new TaskGroupContext(
                        FLAKE_ID_GENERATOR.newId(),
                        taskGroup,
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>());
        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                getField(taskExecutionService, "executionContexts");
        ConcurrentMap<TaskGroupContext, ConcurrentMap<TaskLocation, ScheduledFuture<?>>>
                timerFlushFutures = getField(taskExecutionService, "timerFlushFutures");
        timerFlushFutures.put(context, new ConcurrentHashMap<>());
        executionContexts.put(location, context);
        return context;
    }

    private static void removeTestContext(
            TaskExecutionService taskExecutionService,
            TaskGroupLocation location,
            TaskGroupContext context) {
        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                getField(taskExecutionService, "executionContexts");
        ConcurrentMap<TaskGroupContext, ConcurrentMap<TaskLocation, ScheduledFuture<?>>>
                timerFlushFutures = getField(taskExecutionService, "timerFlushFutures");
        executionContexts.remove(location, context);
        timerFlushFutures.remove(context);
    }

    private static class ContextInitializationFailureTask implements Task {

        @Override
        public void setTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
            throw new IllegalStateException("context initialization failed");
        }

        @Override
        public ProgressState call() {
            return ProgressState.DONE;
        }

        @Override
        public Long getTaskID() {
            return 1L;
        }
    }

    /**
     * Marks that {@link #call()} ran so tests can prove a redeploy actually started execution.
     *
     * <p>Uses a static execution flag because Hazelcast serialization creates a new task instance;
     * the flag must remain visible to the test thread after deserialize.
     */
    private static class ExecutionMarkerTask implements Task, java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private static final AtomicBoolean EXECUTED = new AtomicBoolean(false);

        private final AtomicBoolean stop;

        private ExecutionMarkerTask(AtomicBoolean stop) {
            this.stop = stop;
        }

        private static void reset() {
            EXECUTED.set(false);
        }

        private static boolean wasExecuted() {
            return EXECUTED.get();
        }

        @Override
        public ProgressState call() {
            EXECUTED.set(true);
            if (stop.get()) {
                return ProgressState.DONE;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ProgressState.DONE;
            }
            return ProgressState.MADE_PROGRESS;
        }

        @Override
        public Long getTaskID() {
            return 2L;
        }

        @Override
        public boolean isThreadsShare() {
            return true;
        }
    }

    /**
     * Probe task used to exercise mixed thread-share / blocking submission under PART mode. Static
     * call counter survives Hazelcast deserialize so the test can detect orphaned execution.
     */
    private static class PartialSubmitProbeTask implements Task, java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private static final AtomicInteger CALL_COUNT = new AtomicInteger();
        private static final AtomicBoolean STOP = new AtomicBoolean();

        private final long taskId;
        private final boolean threadsShare;

        private PartialSubmitProbeTask(long taskId, boolean threadsShare) {
            this.taskId = taskId;
            this.threadsShare = threadsShare;
        }

        private static void reset() {
            CALL_COUNT.set(0);
            STOP.set(false);
        }

        private static int callCount() {
            return CALL_COUNT.get();
        }

        @Override
        public ProgressState call() {
            CALL_COUNT.incrementAndGet();
            if (STOP.get()) {
                return ProgressState.DONE;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ProgressState.DONE;
            }
            return ProgressState.MADE_PROGRESS;
        }

        @Override
        public Long getTaskID() {
            return taskId;
        }

        @Override
        public boolean isThreadsShare() {
            return threadsShare;
        }
    }
}
