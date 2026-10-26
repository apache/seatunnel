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

package org.apache.seatunnel.engine.server.master.dirty;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.DirtyJobEvaluationStatus;
import org.apache.seatunnel.engine.common.job.DirtyJobMemberEvent;
import org.apache.seatunnel.engine.common.job.DirtyJobState;
import org.apache.seatunnel.engine.common.job.MemberLeaveClassification;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.ExtendedMapEntry;
import com.hazelcast.map.IMap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

/**
 * Protects execution-owner isolation and fail-closed tracking under delayed distributed writes.
 * Service tests run actual asynchronous timeout, acknowledgement retry and demotion paths.
 */
class DirtyJobServiceTest {

    @Test
    void disabledJobsNeverCreateTrackingStateOrRecoveryAttempts() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            fixture.service.initialize(jobInformation(1L, 100L, -1), false);
            Assertions.assertFalse(fixture.service.isEnabled(jobInformation(1L, 100L, -1)));
            Assertions.assertNull(fixture.service.prepareRecoveryAttempt(1L, 1));
            Assertions.assertNull(fixture.service.reconcileRecoveryPipeline(1L, 1, true, true));
            fixture.service.confirmRecoveryAttempt(1L, 1, "unconfigured");
            fixture.service.completeRecoveryPipeline(1L, 1, "unconfigured");
            Assertions.assertTrue(fixture.states.isEmpty());
            Assertions.assertTrue(fixture.enabled.isEmpty());
            // Disabled jobs must not send feature-specific processor classes to older members.
            Assertions.assertTrue(fixture.submittedStateProcessors.isEmpty());
            Assertions.assertFalse(fixture.service.shouldTrackMemberEvents());
        }
    }

    @Test
    void failedEnabledMarkerReadRetainsLocalGapWithoutOptingInDistributedTracking() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            fixture.failEnabledMarkerRead.set(true);
            Assertions.assertFalse(fixture.service.shouldTrackMemberEvents());
            Assertions.assertTrue(fixture.submittedStateProcessors.isEmpty());
            Assertions.assertTrue(fixture.states.isEmpty());
            Assertions.assertTrue(fixture.events.isEmpty());
            Assertions.assertTrue(
                    fixture.service.getPendingMemberEvents().stream()
                            .anyMatch(
                                    event ->
                                            event.getSequence() < 0
                                                    && "local-tracking-gap"
                                                            .equals(event.getMemberUuid())));
        }
    }

    @Test
    void lateInitializationAfterObservationTimeoutPersistsUnknown() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            fixture.delayNextStateWrite.set(true);
            fixture.service.initialize(jobInformation(1L, 100L, 1), false);
            // initialize has returned through its real 100 ms timeout, not a simulated exception.
            Assertions.assertFalse(fixture.delayedCompletion.get().isDone());
            fixture.completeDelayedWrite();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(fixture.states.get(1L).isTrackingComplete());
                                Assertions.assertEquals(
                                        DirtyJobEvaluationStatus.UNKNOWN,
                                        fixture.service
                                                .getEvaluatedState(1L)
                                                .getEvaluationStatus());
                            });
        }
    }

    @Test
    void timedOutInitializationKeepsDurableUnknownRetryAfterDemotion() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            fixture.delayNextStateWrite.set(true);
            fixture.service.initialize(jobInformation(1L, 100L, 1), false);
            fixture.service.clearLocalCoordinatorState();
            fixture.completeDelayedWrite();
            // Demotion cancels local waiting, but cannot cancel an already dispatched map write.
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            fixture.states.get(1L).isTrackingComplete()));
        }
    }

    @Test
    void sameExecutionInitializationCannotEraseConfirmedRecovery() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            JobImmutableInformation job = jobInformation(1L, 100L, 1);
            fixture.service.initialize(job, false);
            DirtyJobState state = fixture.states.get(1L);
            state.processMemberEvent(
                    8L,
                    "worker",
                    "localhost:5801",
                    MemberLeaveClassification.UNCLASSIFIED,
                    Collections.singleton(1),
                    1_000L);
            String attempt = state.prepareRecoveryAttempt(1, 1_001L);
            state.confirmRecoveryAttempt(1, attempt, 1_002L);
            fixture.service.initialize(job, false);
            Assertions.assertTrue(fixture.states.get(1L).isDirty());
            Assertions.assertEquals(1, fixture.states.get(1L).getRecoveryAttemptCount());
            Assertions.assertEquals(
                    8L, fixture.states.get(1L).getLastProcessedMemberEventSequence());
        }
    }

    @Test
    void demotionPreventsLateTrackingFailureFromMarkingSuccessorExecution() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            fixture.delayNextStateWrite.set(true);
            fixture.service.initialize(jobInformation(1L, 100L, 1), false);
            fixture.service.clearLocalCoordinatorState();
            fixture.service.initialize(jobInformation(1L, 200L, 1), false);
            // A delayed distributed processor may still execute after local cancellation.
            fixture.completeDelayedWrite();
            DirtyJobState successor = fixture.states.get(1L);
            Assertions.assertEquals(200L, successor.getJobCreateTime());
            Assertions.assertEquals(0, successor.getRecoveryAttemptCount());
            Assertions.assertTrue(successor.isTrackingComplete());
            Assertions.assertEquals(
                    DirtyJobEvaluationStatus.CLEAN,
                    fixture.service.getEvaluatedState(1L).getEvaluationStatus());
        }
    }

    @Test
    void acknowledgementRetriesAfterFailureAndSurvivesCoordinatorDemotion() {
        try (ServiceFixture fixture = new ServiceFixture()) {
            DirtyJobMemberEvent event =
                    new DirtyJobMemberEvent(7L, "member-1", "localhost", 5801, 100L);
            fixture.events.put(event.getMemberUuid(), event);
            fixture.failNextAcknowledgement.set(true);
            fixture.service.removePendingMemberEvent(event);
            await().atMost(5, TimeUnit.SECONDS).until(() -> !fixture.failNextAcknowledgement.get());
            fixture.service.clearLocalCoordinatorState();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            600_000L,
                                            fixture.acknowledgedTtl.get(event.getMemberUuid())));
            Assertions.assertSame(event, fixture.events.get(event.getMemberUuid()));
        }
    }

    private static JobImmutableInformation jobInformation(long jobId, long owner, int threshold) {
        JobConfig config = new JobConfig();
        config.getEnvOptions().put(EnvCommonOptions.DIRTY_JOB_RESTORE_THRESHOLD.key(), threshold);
        JobImmutableInformation information = Mockito.mock(JobImmutableInformation.class);
        Mockito.when(information.getJobId()).thenReturn(jobId);
        Mockito.when(information.getCreateTime()).thenReturn(owner);
        Mockito.when(information.getJobConfig()).thenReturn(config);
        return information;
    }

    /**
     * Executes real entry processors over map-backed entries, with explicit delayed completions.
     * Only the distributed transport is controlled; service timers and retry executors are real.
     */
    private static class ServiceFixture implements AutoCloseable {
        private final Map<Long, DirtyJobState> states = new ConcurrentHashMap<>();
        private final List<String> submittedStateProcessors =
                Collections.synchronizedList(new ArrayList<>());
        private final Map<Long, Integer> enabled = new ConcurrentHashMap<>();
        private final Map<String, DirtyJobMemberEvent> events = new ConcurrentHashMap<>();
        private final Map<String, Long> acknowledgedTtl = new ConcurrentHashMap<>();
        private final AtomicBoolean delayNextStateWrite = new AtomicBoolean();
        private final AtomicBoolean failEnabledMarkerRead = new AtomicBoolean();
        private final AtomicBoolean failNextAcknowledgement = new AtomicBoolean();
        private final AtomicReference<Runnable> delayedWrite = new AtomicReference<>();
        private final AtomicReference<CompletableFuture<Object>> delayedCompletion =
                new AtomicReference<>();
        private final DirtyJobService service;

        @SuppressWarnings({"unchecked", "rawtypes"})
        private ServiceFixture() {
            IMap<Long, DirtyJobState> stateMap = map(states);
            Mockito.doAnswer(
                            invocation -> {
                                Long key = invocation.getArgument(0);
                                EntryProcessor processor = invocation.getArgument(1);
                                submittedStateProcessors.add(processor.getClass().getName());
                                if (delayNextStateWrite.compareAndSet(true, false)) {
                                    CompletableFuture<Object> future = new CompletableFuture<>();
                                    delayedCompletion.set(future);
                                    delayedWrite.set(
                                            () -> future.complete(process(states, key, processor)));
                                    return future;
                                }
                                return CompletableFuture.completedFuture(
                                        process(states, key, processor));
                            })
                    .when(stateMap)
                    .submitToKey(Mockito.anyLong(), Mockito.any());
            IMap<String, DirtyJobMemberEvent> eventMap = map(events);
            Mockito.doAnswer(
                            invocation -> {
                                String key = invocation.getArgument(0);
                                EntryProcessor processor = invocation.getArgument(1);
                                if (processor
                                                instanceof
                                                DirtyJobService.AcknowledgeJournalEventProcessor
                                        && failNextAcknowledgement.compareAndSet(true, false)) {
                                    CompletableFuture<Object> failure = new CompletableFuture<>();
                                    failure.completeExceptionally(
                                            new IllegalStateException("partition unavailable"));
                                    return failure;
                                }
                                ExtendedMapEntry entry = Mockito.mock(ExtendedMapEntry.class);
                                Mockito.when(entry.getKey()).thenReturn(key);
                                Mockito.when(entry.getValue())
                                        .thenAnswer(ignored -> events.get(key));
                                Mockito.when(entry.setValue(Mockito.any()))
                                        .thenAnswer(call -> events.put(key, call.getArgument(0)));
                                Mockito.when(
                                                entry.setValue(
                                                        Mockito.any(),
                                                        Mockito.anyLong(),
                                                        Mockito.any(TimeUnit.class)))
                                        .thenAnswer(
                                                call -> {
                                                    acknowledgedTtl.put(key, call.getArgument(1));
                                                    return events.put(key, call.getArgument(0));
                                                });
                                return CompletableFuture.completedFuture(processor.process(entry));
                            })
                    .when(eventMap)
                    .submitToKey(Mockito.anyString(), Mockito.any());
            IMap<Long, Integer> enabledMap = map(enabled);
            Mockito.doAnswer(
                            invocation -> {
                                if (failEnabledMarkerRead.get()) {
                                    throw new IllegalStateException(
                                            "enabled marker read unavailable");
                                }
                                return enabled.isEmpty();
                            })
                    .when(enabledMap)
                    .isEmpty();
            service =
                    new DirtyJobService(
                            stateMap,
                            map(new ConcurrentHashMap<Long, Long>()),
                            eventMap,
                            enabledMap,
                            Mockito.mock(ILogger.class),
                            10,
                            600_000L);
        }

        private void completeDelayedWrite() {
            delayedWrite.getAndSet(null).run();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <K, V> IMap<K, V> map(Map<K, V> values) {
            return Mockito.mock(
                    IMap.class,
                    invocation -> {
                        Object[] arguments = invocation.getArguments();
                        switch (invocation.getMethod().getName()) {
                            case "get":
                                return values.get(arguments[0]);
                            case "getAsync":
                                return CompletableFuture.completedFuture(values.get(arguments[0]));
                            case "putAsync":
                                return CompletableFuture.completedFuture(
                                        values.put((K) arguments[0], (V) arguments[1]));
                            case "keySet":
                                return new HashSet<>(values.keySet());
                            case "values":
                                return new ArrayList<>(values.values());
                            case "isEmpty":
                                return values.isEmpty();
                            case "getAll":
                                Map<K, V> found = new HashMap<>();
                                for (Object key : (Iterable<?>) arguments[0]) {
                                    if (values.containsKey(key)) {
                                        found.put((K) key, values.get(key));
                                    }
                                }
                                return found;
                            case "submitToKey":
                                return CompletableFuture.completedFuture(
                                        process(
                                                values,
                                                (K) arguments[0],
                                                (EntryProcessor) arguments[1]));
                            default:
                                return Mockito.RETURNS_DEFAULTS.answer(invocation);
                        }
                    });
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static <K, V> Object process(Map<K, V> values, K key, EntryProcessor processor) {
            Map.Entry<K, V> entry = new AbstractMap.SimpleEntry<>(key, values.get(key));
            Object result = processor.process(entry);
            if (entry.getValue() == null) {
                values.remove(key);
            } else {
                values.put(key, entry.getValue());
            }
            return result;
        }

        @Override
        public void close() {
            service.shutdown();
        }
    }

    @Test
    void staleJournalCallbacksCannotReplaceOrAcknowledgeNewerMasterEvent() {
        DirtyJobMemberEvent previous =
                new DirtyJobMemberEvent(7L, "member-1", "127.0.0.1", 5801, 1_000L);
        DirtyJobMemberEvent current =
                new DirtyJobMemberEvent(8L, "member-1", "127.0.0.1", 5801, 2_000L);
        Map.Entry<String, DirtyJobMemberEvent> entry =
                new AbstractMap.SimpleEntry<>("member-1", current);

        Assertions.assertSame(
                current, new DirtyJobService.JournalMemberEventProcessor(previous).process(entry));
        Assertions.assertSame(current, entry.getValue());
        Assertions.assertFalse(
                new DirtyJobService.AcknowledgeJournalEventProcessor(7L, 600_000L).process(entry));
        Assertions.assertSame(current, entry.getValue());
    }

    @Test
    void shouldRejectUnknownOrPreviousExecutionOwner() {
        DirtyJobState state = DirtyJobState.create(1L, 200L, 2, 10, 600_000L, 0L);

        Assertions.assertTrue(DirtyJobService.isOwnedBy(state, 200L));
        Assertions.assertFalse(DirtyJobService.isOwnedBy(state, 100L));
        Assertions.assertFalse(DirtyJobService.isOwnedBy(state, 0L));
    }

    @Test
    void shouldNotReplaceCurrentIncompleteOwnerWithStaleRetry() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L);
        Assertions.assertTrue(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 200L));

        Assertions.assertFalse(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 100L));
        Assertions.assertEquals(200L, incompleteOwners.get(1L));
    }

    @Test
    void shouldRetainEarlyIncompleteEvidenceForSameRestoredExecution() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        Assertions.assertTrue(
                DirtyJobService.registerLocalIncompleteOwner(owners, incompleteOwners, 1L, 200L));

        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L);

        Assertions.assertEquals(200L, incompleteOwners.get(1L));
    }

    @Test
    void shouldRemoveIncompleteEvidenceFromPreviousExecution() {
        Map<Long, Long> owners = new ConcurrentHashMap<>();
        Map<Long, Long> incompleteOwners = new ConcurrentHashMap<>();
        incompleteOwners.put(1L, 100L);

        DirtyJobService.registerLocalOwner(owners, incompleteOwners, 1L, 200L);

        Assertions.assertFalse(incompleteOwners.containsKey(1L));
    }

    @Test
    void shouldInjectGapEventWhenPendingJournalReadFailsWithoutLocalFallback() {
        List<DirtyJobMemberEvent> events =
                DirtyJobService.buildFailClosedPendingMemberEvents(Collections.emptyList());

        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals(-1L, events.get(0).getSequence());
        Assertions.assertEquals("local-tracking-gap", events.get(0).getMemberUuid());
    }

    @Test
    void shouldPreserveLocalFallbackWhileFailingClosedAfterJournalReadFailure() {
        DirtyJobMemberEvent localEvent =
                new DirtyJobMemberEvent(7L, "member-1", "127.0.0.1", 5801, 1_000L);

        List<DirtyJobMemberEvent> events =
                DirtyJobService.buildFailClosedPendingMemberEvents(
                        Collections.singletonList(localEvent));

        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals("local-tracking-gap", events.get(0).getMemberUuid());
        Assertions.assertEquals(localEvent, events.get(1));
    }
}
