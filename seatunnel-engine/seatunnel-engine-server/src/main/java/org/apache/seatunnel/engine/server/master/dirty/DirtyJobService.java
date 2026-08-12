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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.DirtyJobMemberEvent;
import org.apache.seatunnel.engine.common.job.DirtyJobState;
import org.apache.seatunnel.engine.common.job.MemberLeaveClassification;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.ExtendedMapEntry;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Coordinates backup-aware atomic dirty-job updates without changing the existing recovery result.
 *
 * <p>Tracking failures are converted to UNKNOWN whenever state is still reachable. Member events
 * are journaled before coordinator readiness checks so a new master can replay the event before a
 * restored job becomes schedulable.
 */
public class DirtyJobService {
    private static final long MEMBER_EVENT_SEQUENCE_KEY = 0L;
    private static final long TRACKING_OPERATION_TIMEOUT_MILLIS = 100L;
    private static final long TRACKING_RETRY_DELAY_MILLIS = 1000L;
    private static final String LOCAL_TRACKING_GAP_MEMBER_UUID = "local-tracking-gap";

    // Job-scoped source of truth updated atomically with synchronous backups.
    private final IMap<Long, DirtyJobState> dirtyJobStateMap;
    // Cluster watermark used to detect member events missed by one job state.
    private final IMap<Long, Long> memberEventSequenceMap;
    // Durable event journal and short-lived acknowledgement tombstones.
    private final IMap<String, DirtyJobMemberEvent> pendingMemberEventMap;
    // Compact marker distinguishing disabled jobs from missing enabled state.
    private final IMap<Long, Integer> enabledThresholdMap;
    // Hazelcast logger keeps tracking diagnostics in the engine log path.
    private final ILogger logger;
    // Bound applied independently to episode and incident history.
    private final int eventHistorySize;
    // Shared TTL for incomplete episodes and acknowledgement tombstones.
    private final long pendingIncidentTtlMillis;
    // Serializes asynchronous state updates by job without blocking member removal.
    private final Map<Long, CompletionStage<?>> pendingStateUpdates = new HashMap<>();
    // Local fail-closed guard scoped to one submission owner.
    private final Map<Long, Long> locallyIncompleteJobOwners = new ConcurrentHashMap<>();
    // Covers the active master's marker-write window without a distributed custom type.
    private final Set<Long> locallyEnabledJobIds = ConcurrentHashMap.newKeySet();
    // Captures the submission owner used by delayed fail-closed marker processors.
    private final Map<Long, Long> locallyOwnedJobCreateTimes = new ConcurrentHashMap<>();
    // Survives sequence or journal outages on each master-capable member.
    private final Map<String, DirtyJobMemberEvent> localPendingMemberEvents =
            new ConcurrentHashMap<>();
    // Prevents duplicate reconciliation loops for the same fallback event.
    private final Set<String> localMemberEventReconciliations = ConcurrentHashMap.newKeySet();
    // Retains the highest event requested for acknowledgement per member UUID.
    private final Map<String, DirtyJobMemberEvent> pendingMemberEventAcknowledgements =
            new ConcurrentHashMap<>();
    // Limits acknowledgement execution to one loop per member UUID.
    private final Set<String> runningMemberEventAcknowledgements = ConcurrentHashMap.newKeySet();
    // Limits HA incomplete-marker retry scheduling to one loop per job execution owner.
    private final Set<String> incompleteMarkerRetries = ConcurrentHashMap.newKeySet();
    // Invalidates role-local tracking work after this node loses active-master ownership.
    private final AtomicLong coordinatorEpoch = new AtomicLong();
    // Collapses overflow into one permanent fail-closed signal instead of dropping evidence.
    private volatile boolean localTrackingGap;
    // Isolates journal reads, acknowledgement, and fallback reconciliation from recovery threads.
    private final ScheduledExecutorService journalExecutor =
            Executors.newScheduledThreadPool(
                    2,
                    runnable -> {
                        Thread thread = new Thread(runnable, "dirty-job-journal");
                        thread.setDaemon(true);
                        return thread;
                    });
    // Keeps UNKNOWN watchdogs independent from potentially stalled journal operations.
    private final ScheduledExecutorService trackingRetryExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "dirty-job-tracking-retry");
                        thread.setDaemon(true);
                        return thread;
                    });

    public DirtyJobService(
            NodeEngineImpl nodeEngine,
            ILogger logger,
            int eventHistorySize,
            long pendingIncidentTtlMillis) {
        this.dirtyJobStateMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_DIRTY_JOB_STATE);
        this.memberEventSequenceMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_DIRTY_JOB_MEMBER_EVENT_SEQUENCE);
        this.pendingMemberEventMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_DIRTY_JOB_PENDING_MEMBER_EVENTS);
        this.enabledThresholdMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_DIRTY_JOB_ENABLED_THRESHOLDS);
        this.logger = logger;
        this.eventHistorySize = eventHistorySize;
        this.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
    }

    /**
     * Creates the job state before scheduling, or validates retained state during master recovery.
     */
    public void initialize(JobImmutableInformation jobInformation, boolean restart) {
        int threshold = getThreshold(jobInformation);
        if (threshold <= 0) {
            return;
        }
        long jobId = jobInformation.getJobId();
        locallyEnabledJobIds.add(jobId);
        registerLocalOwner(
                locallyOwnedJobCreateTimes,
                locallyIncompleteJobOwners,
                jobId,
                jobInformation.getCreateTime(),
                restart);
        boolean enabledMarkerIncomplete = false;
        try {
            enabledThresholdMap
                    .putAsync(jobId, threshold)
                    .toCompletableFuture()
                    .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            locallyIncompleteJobOwners.put(jobId, jobInformation.getCreateTime());
            enabledMarkerIncomplete = true;
            logger.warning(
                    String.format("Failed to persist dirty-job enabled marker for job %s", jobId),
                    error);
        }
        try {
            long sequence = getCurrentMemberEventSequence();
            CompletableFuture<DirtyJobState> update =
                    submitStateUpdate(
                            jobId,
                            new InitializeProcessor(
                                    jobId,
                                    jobInformation.getCreateTime(),
                                    threshold,
                                    eventHistorySize,
                                    pendingIncidentTtlMillis,
                                    sequence,
                                    restart,
                                    !restart,
                                    enabledMarkerIncomplete),
                            "initialize dirty-job state");
            awaitStateUpdate(jobId, update, "initialize dirty-job state");
        } catch (Throwable error) {
            handleTrackingFailure(jobId, "initialize dirty-job state", error);
        }
    }

    /**
     * Returns whether the job explicitly enabled member-loss dirty tracking.
     *
     * @param jobMaster active or pending job master
     * @return true only for a positive job-scoped threshold
     */
    public boolean isEnabled(JobMaster jobMaster) {
        return jobMaster != null && getThreshold(jobMaster.getJobImmutableInformation()) > 0;
    }

    /**
     * Avoids sending new tracking types during a disabled rolling upgrade.
     *
     * @return true when tracking is enabled locally or in HA, or when the HA check is uncertain
     */
    public boolean shouldTrackMemberEvents() {
        if (!locallyEnabledJobIds.isEmpty()) {
            return true;
        }
        try {
            return CompletableFuture.supplyAsync(
                            () -> !enabledThresholdMap.isEmpty() || !dirtyJobStateMap.isEmpty(),
                            journalExecutor)
                    .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warning(
                    "Failed to determine whether dirty-job member tracking is enabled; tracking conservatively",
                    error);
            return true;
        }
    }

    /**
     * Runs an observation-only tracking read without allowing it to stall active-master recovery.
     *
     * @param <T> observation result type
     * @param observation tracking read which must not mutate the original recovery state
     * @return observation result within the shared tracking budget
     */
    public <T> T runBoundedObservation(Supplier<T> observation) {
        try {
            return CompletableFuture.supplyAsync(observation, journalExecutor)
                    .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException(
                    "Dirty-job tracking observation failed or timed out", error);
        }
    }

    /**
     * Retains fallback evidence on every master-capable member before active-master selection.
     *
     * @param memberUuid removed Hazelcast member identity
     * @param memberHost removed member host
     * @param memberPort removed member port
     */
    public void rememberMemberEventLocally(String memberUuid, String memberHost, int memberPort) {
        DirtyJobMemberEvent previous =
                localPendingMemberEvents.putIfAbsent(
                        memberUuid,
                        new DirtyJobMemberEvent(
                                -1L,
                                memberUuid,
                                memberHost,
                                memberPort,
                                System.currentTimeMillis()));
        if (previous == null) {
            boundLocalMemberEvents();
            scheduleLocalMemberEventReconciliation(memberUuid);
        }
    }

    /**
     * Journals a member removal before coordinator readiness is checked. The entry remains until an
     * active coordinator has applied it to every restored job.
     */
    public DirtyJobMemberEvent recordMemberEvent(
            String memberUuid, String memberHost, int memberPort) {
        long eventTime = System.currentTimeMillis();
        long sequence = nextMemberEventSequence();
        DirtyJobMemberEvent event =
                new DirtyJobMemberEvent(sequence, memberUuid, memberHost, memberPort, eventTime);
        if (sequence < 0) {
            localPendingMemberEvents.put(memberUuid, event);
            boundLocalMemberEvents();
            return event;
        }
        try {
            pendingMemberEventMap
                    .submitToKey(memberUuid, new JournalMemberEventProcessor(event))
                    .toCompletableFuture()
                    .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            localPendingMemberEvents.remove(memberUuid);
            return event;
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warning(String.format("Failed to journal removed member %s", memberUuid), error);
            localPendingMemberEvents.put(memberUuid, event);
            boundLocalMemberEvents();
            return event;
        }
    }

    /**
     * Returns pending events in cluster sequence order so active-master recovery is deterministic.
     */
    public List<DirtyJobMemberEvent> getPendingMemberEvents() {
        try {
            Map<String, DirtyJobMemberEvent> eventsByMember = new HashMap<>();
            Collection<DirtyJobMemberEvent> persistedEvents =
                    CompletableFuture.supplyAsync(
                                    () -> new ArrayList<>(pendingMemberEventMap.values()),
                                    journalExecutor)
                            .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            persistedEvents.forEach(event -> eventsByMember.put(event.getMemberUuid(), event));
            localPendingMemberEvents.forEach(
                    (memberUuid, event) ->
                            eventsByMember.merge(
                                    memberUuid,
                                    event,
                                    (left, right) ->
                                            left.getSequence() >= right.getSequence()
                                                    ? left
                                                    : right));
            addLocalTrackingGap(eventsByMember);
            List<DirtyJobMemberEvent> events = new ArrayList<>(eventsByMember.values());
            events.sort((left, right) -> Long.compare(left.getSequence(), right.getSequence()));
            return events;
        } catch (Throwable error) {
            logger.warning("Failed to load pending dirty-job member events", error);
            return buildFailClosedPendingMemberEvents(localPendingMemberEvents.values());
        }
    }

    /**
     * Removes one journal entry only after the active coordinator has processed all current jobs.
     */
    public void removePendingMemberEvent(DirtyJobMemberEvent event) {
        if (event == null) {
            return;
        }
        scheduleMemberEventAcknowledgement(event, 0L);
    }

    /**
     * Removes journal entries replayed while all running jobs were reconstructed on a new master.
     */
    public void removePendingMemberEvents(Collection<DirtyJobMemberEvent> events) {
        if (events == null) {
            return;
        }
        events.forEach(this::removePendingMemberEvent);
    }

    /**
     * Creates a non-persisted UNKNOWN snapshot when enabled state cannot be read.
     *
     * @param jobInformation immutable job configuration
     * @param reason evidence explaining why CLEAN cannot be asserted
     * @return UNKNOWN state, or null when tracking is disabled
     */
    public DirtyJobState createUnknownState(JobImmutableInformation jobInformation, String reason) {
        int threshold = getThreshold(jobInformation);
        if (threshold <= 0) {
            return null;
        }
        long sequence;
        try {
            sequence = getCurrentMemberEventSequence();
        } catch (Throwable ignored) {
            sequence = 0L;
        }
        DirtyJobState state =
                DirtyJobState.create(
                        jobInformation.getJobId(),
                        jobInformation.getCreateTime(),
                        threshold,
                        eventHistorySize,
                        pendingIncidentTtlMillis,
                        sequence);
        state.markTrackingIncomplete(reason);
        return state;
    }

    /**
     * Advances a newly submitted job to the current watermark immediately before scheduling.
     *
     * @param jobMaster job which is about to become schedulable
     */
    public void synchronizeMemberEventSequence(JobMaster jobMaster) {
        if (!isEnabled(jobMaster)) {
            return;
        }
        long jobId = jobMaster.getJobId();
        try {
            long sequence = getCurrentMemberEventSequence();
            CompletableFuture<Void> update =
                    submitStateUpdate(
                            jobId,
                            new AdvanceSequenceProcessor(
                                    jobId,
                                    jobMaster.getJobImmutableInformation().getCreateTime(),
                                    getThreshold(jobMaster.getJobImmutableInformation()),
                                    eventHistorySize,
                                    pendingIncidentTtlMillis,
                                    sequence),
                            "synchronize member-event sequence");
            awaitStateUpdate(jobId, update, "synchronize member-event sequence");
        } catch (Throwable error) {
            handleTrackingFailure(jobId, "synchronize member-event sequence", error);
        }
    }

    /**
     * Atomically advances the watermark and records an incident when this job has affected
     * pipelines.
     */
    public CompletableFuture<Void> processMemberEvent(
            JobMaster jobMaster,
            long memberEventSequence,
            String lostMemberUuid,
            String lostAddress,
            Collection<Integer> affectedPipelineIds) {
        return processMemberEvent(
                jobMaster,
                memberEventSequence,
                lostMemberUuid,
                lostAddress,
                affectedPipelineIds,
                System.currentTimeMillis());
    }

    /**
     * Replays a journaled member event with its original timestamp so stale episodes stay stale.
     *
     * @param eventTime original Hazelcast member-removal observation time
     */
    public CompletableFuture<Void> processMemberEvent(
            JobMaster jobMaster,
            long memberEventSequence,
            String lostMemberUuid,
            String lostAddress,
            Collection<Integer> affectedPipelineIds,
            long eventTime) {
        if (!isEnabled(jobMaster)) {
            return CompletableFuture.completedFuture(null);
        }
        long jobId = jobMaster.getJobId();
        if (memberEventSequence < 0) {
            return markTrackingIncomplete(jobId, "Member-event sequence allocation failed");
        }
        return submitStateUpdate(
                jobId,
                new MemberEventProcessor(
                        jobId,
                        jobMaster.getJobImmutableInformation().getCreateTime(),
                        getThreshold(jobMaster.getJobImmutableInformation()),
                        eventHistorySize,
                        pendingIncidentTtlMillis,
                        memberEventSequence,
                        lostMemberUuid,
                        lostAddress,
                        affectedPipelineIds,
                        eventTime),
                "record member-loss event");
    }

    /**
     * Waits only for the bounded tracking budget used before a restored job becomes schedulable.
     *
     * @param jobId job whose replay update was submitted
     * @param update asynchronous member-event update
     */
    public void awaitMemberEventReplay(long jobId, CompletableFuture<Void> update) {
        awaitStateUpdate(jobId, update, "replay member-loss event");
    }

    /**
     * Allocates the stable PREPARED attempt consumed by the pipeline state transition.
     *
     * @return stable attempt ID, or null when no member-loss episode owns the pipeline
     */
    public String prepareRecoveryAttempt(long jobId, int pipelineId) {
        CompletableFuture<String> update =
                submitStateUpdate(
                        jobId,
                        new PrepareAttemptProcessor(
                                getLocallyOwnedJobCreateTime(jobId),
                                pipelineId,
                                System.currentTimeMillis()),
                        "prepare recovery attempt");
        return awaitStateUpdate(jobId, update, "prepare recovery attempt");
    }

    /**
     * Confirms one attempt after the pipeline state is persisted as DEPLOYING.
     *
     * @param attemptId stable ID returned during restore preparation
     */
    public void confirmRecoveryAttempt(long jobId, int pipelineId, String attemptId) {
        if (attemptId == null) {
            return;
        }
        submitStateUpdate(
                jobId,
                new ConfirmAttemptProcessor(
                        getLocallyOwnedJobCreateTime(jobId),
                        pipelineId,
                        attemptId,
                        System.currentTimeMillis()),
                "confirm recovery attempt");
    }

    /**
     * Completes one pipeline only after all of its task groups report RUNNING.
     *
     * @param attemptId stable confirmed attempt which reached RUNNING
     */
    public void completeRecoveryPipeline(long jobId, int pipelineId, String attemptId) {
        if (attemptId == null) {
            return;
        }
        submitStateUpdate(
                jobId,
                new CompletePipelineProcessor(
                        getLocallyOwnedJobCreateTime(jobId),
                        pipelineId,
                        attemptId,
                        System.currentTimeMillis()),
                "complete recovery pipeline");
    }

    /**
     * Replays persisted DEPLOYING or RUNNING state idempotently after an active-master switch.
     *
     * @param deployed whether persisted pipeline state proves deployment started
     * @param recovered whether persisted task state proves deployment completed
     */
    public String reconcileRecoveryPipeline(
            long jobId, int pipelineId, boolean deployed, boolean recovered) {
        if (!deployed) {
            return null;
        }
        CompletableFuture<String> update =
                submitStateUpdate(
                        jobId,
                        new ReconcilePipelineProcessor(
                                getLocallyOwnedJobCreateTime(jobId),
                                pipelineId,
                                recovered,
                                System.currentTimeMillis()),
                        "reconcile recovery pipeline");
        return awaitStateUpdate(jobId, update, "reconcile recovery pipeline");
    }

    /**
     * Reads one state and applies current watermark and TTL completeness checks.
     *
     * @return evaluated copy, or null when no enabled state exists
     */
    public DirtyJobState getEvaluatedState(long jobId) {
        DirtyJobState state;
        try {
            state = dirtyJobStateMap.get(jobId);
            if (state == null) {
                return null;
            }
            state = state.evaluatedCopy(getCurrentMemberEventSequence());
            markLocallyIncomplete(jobId, state);
            return state;
        } catch (Throwable error) {
            logger.warning(
                    String.format("Failed to evaluate dirty-job state for job %s", jobId), error);
            try {
                state = dirtyJobStateMap.get(jobId);
                if (state != null) {
                    state = state.evaluatedCopy(state.getLastProcessedMemberEventSequence());
                    state.markTrackingIncomplete("Failed to evaluate dirty-job state");
                }
                return state;
            } catch (Throwable ignored) {
                return null;
            }
        }
    }

    /**
     * Bulk-reads list-page state to avoid one distributed lookup per active job.
     *
     * @return evaluated states keyed by enabled job ID
     */
    public Map<Long, DirtyJobState> getEvaluatedStates(Collection<Long> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Long, DirtyJobState> states = new HashMap<>();
        try {
            states.putAll(dirtyJobStateMap.getAll(new HashSet<>(jobIds)));
            long sequence = getCurrentMemberEventSequence();
            states.replaceAll(
                    (jobId, state) -> {
                        DirtyJobState evaluated = state.evaluatedCopy(sequence);
                        markLocallyIncomplete(jobId, evaluated);
                        return evaluated;
                    });
        } catch (Throwable error) {
            logger.warning("Failed to evaluate dirty-job states", error);
            states.replaceAll(
                    (jobId, state) -> {
                        DirtyJobState evaluated =
                                state.evaluatedCopy(state.getLastProcessedMemberEventSequence());
                        evaluated.markTrackingIncomplete("Failed to evaluate dirty-job state list");
                        return evaluated;
                    });
        }
        return states;
    }

    /**
     * Aborts any incomplete episode and returns the final history snapshot.
     *
     * @return evaluated terminal snapshot, or null when tracking was disabled
     */
    public DirtyJobState finishAndGetState(long jobId) {
        try {
            CompletableFuture<DirtyJobState> update =
                    submitStateUpdate(
                            jobId,
                            new FinishEpisodeProcessor(
                                    getLocallyOwnedJobCreateTime(jobId),
                                    System.currentTimeMillis()),
                            "finalize dirty-job state");
            DirtyJobState state = awaitStateUpdate(jobId, update, "finalize dirty-job state");
            if (state == null) {
                return null;
            }
            state = state.evaluatedCopy(getCurrentMemberEventSequence());
            markLocallyIncomplete(jobId, state);
            return state;
        } catch (Throwable error) {
            handleTrackingFailure(jobId, "finalize dirty-job state", error);
            return getEvaluatedState(jobId);
        }
    }

    /**
     * Removes running tracking state only after the delayed terminal-history cleanup boundary.
     *
     * @param jobId terminal job whose history snapshot has already been stored
     * @return true when both state and enabled marker were removed or already absent
     */
    public boolean removeRunningState(long jobId) {
        boolean stateRemoved = removeWithBudget(dirtyJobStateMap, jobId, "state");
        boolean markerRemoved = removeWithBudget(enabledThresholdMap, jobId, "enabled marker");
        boolean removed = stateRemoved && markerRemoved;
        if (removed) {
            locallyEnabledJobIds.remove(jobId);
            synchronized (locallyOwnedJobCreateTimes) {
                locallyIncompleteJobOwners.remove(jobId);
                locallyOwnedJobCreateTimes.remove(jobId);
            }
        }
        return removed;
    }

    /**
     * Applies the local fail-closed guard to a query copy built outside this service.
     *
     * @param jobId queried job
     * @param state query copy loaded by REST
     * @return the same state, marked UNKNOWN when a bounded local update timed out
     */
    public DirtyJobState markUnknownIfLocallyIncomplete(long jobId, DirtyJobState state) {
        markLocallyIncomplete(jobId, state);
        return state;
    }

    /**
     * Converts an exception caught by a recovery caller into local and HA UNKNOWN evidence.
     *
     * @param jobId affected job
     * @param operation tracking operation which failed
     * @param error failure isolated from the original recovery path
     */
    public void reportTrackingFailure(long jobId, String operation, Throwable error) {
        handleTrackingFailure(jobId, operation, error);
    }

    /**
     * Converts an early recovery exception using the owner read from persisted job metadata.
     *
     * @param jobId affected job
     * @param jobCreateTime immutable execution owner
     * @param operation tracking operation which failed
     * @param error failure isolated from the original recovery path
     */
    public void reportTrackingFailure(
            long jobId, long jobCreateTime, String operation, Throwable error) {
        logger.warning(String.format("Failed to %s for job %s", operation, jobId), error);
        markTrackingIncomplete(
                jobId, jobCreateTime, operation + " failed: " + error.getClass().getSimpleName());
    }

    // Clears active-coordinator state while retaining member-event fallback evidence.
    public void clearLocalCoordinatorState() {
        coordinatorEpoch.incrementAndGet();
        locallyEnabledJobIds.clear();
        synchronized (locallyOwnedJobCreateTimes) {
            locallyIncompleteJobOwners.clear();
            locallyOwnedJobCreateTimes.clear();
        }
        synchronized (pendingStateUpdates) {
            pendingStateUpdates.values().stream()
                    .map(CompletionStage::toCompletableFuture)
                    .forEach(future -> future.cancel(false));
            pendingStateUpdates.clear();
        }
    }

    // Stops bounded journal and incomplete-marker retries during server shutdown.
    public void shutdown() {
        journalExecutor.shutdownNow();
        trackingRetryExecutor.shutdownNow();
    }

    private long nextMemberEventSequence() {
        try {
            CompletableFuture<Long> update =
                    memberEventSequenceMap
                            .submitToKey(
                                    MEMBER_EVENT_SEQUENCE_KEY, new IncrementSequenceProcessor())
                            .toCompletableFuture();
            return update.get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warning("Failed to allocate dirty-job member-event sequence", error);
            return -1L;
        }
    }

    private int getThreshold(JobImmutableInformation jobInformation) {
        if (jobInformation == null || jobInformation.getJobConfig() == null) {
            return EnvCommonOptions.DIRTY_JOB_RESTORE_THRESHOLD.defaultValue();
        }
        Map<String, Object> envOptions = jobInformation.getJobConfig().getEnvOptions();
        Object value = envOptions.get(EnvCommonOptions.DIRTY_JOB_RESTORE_THRESHOLD.key());
        return value == null
                ? EnvCommonOptions.DIRTY_JOB_RESTORE_THRESHOLD.defaultValue()
                : Integer.parseInt(value.toString());
    }

    private long getLocallyOwnedJobCreateTime(long jobId) {
        return locallyOwnedJobCreateTimes.getOrDefault(jobId, 0L);
    }

    private boolean acknowledgeMemberEvent(DirtyJobMemberEvent event) {
        try {
            DirtyJobMemberEvent acknowledgementCandidate = event;
            if (event.getSequence() >= 0) {
                acknowledgementCandidate =
                        pendingMemberEventMap
                                .submitToKey(
                                        event.getMemberUuid(),
                                        new JournalMemberEventProcessor(event))
                                .toCompletableFuture()
                                .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            }
            final DirtyJobMemberEvent eventToAcknowledge = acknowledgementCandidate;
            Set<Long> enabledJobIds = new HashSet<>(enabledThresholdMap.keySet());
            if (!enabledJobIds.isEmpty()) {
                Map<Long, DirtyJobState> states = dirtyJobStateMap.getAll(enabledJobIds);
                boolean processedByEveryEnabledJob =
                        enabledJobIds.stream()
                                .allMatch(
                                        jobId -> {
                                            DirtyJobState state = states.get(jobId);
                                            if (state == null) {
                                                return false;
                                            }
                                            return eventToAcknowledge.getSequence() < 0
                                                    ? !state.isTrackingComplete()
                                                    : state.getLastProcessedMemberEventSequence()
                                                            >= eventToAcknowledge.getSequence();
                                        });
                if (!processedByEveryEnabledJob) {
                    return false;
                }
            }
            if (eventToAcknowledge.getSequence() >= 0) {
                boolean acknowledged =
                        pendingMemberEventMap
                                .submitToKey(
                                        eventToAcknowledge.getMemberUuid(),
                                        new AcknowledgeJournalEventProcessor(
                                                eventToAcknowledge.getSequence(),
                                                pendingIncidentTtlMillis))
                                .toCompletableFuture()
                                .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (!acknowledged) {
                    return false;
                }
            }
            localPendingMemberEvents.remove(eventToAcknowledge.getMemberUuid());
            return true;
        } catch (Throwable error) {
            logger.warning(
                    String.format(
                            "Failed to acknowledge dirty-job member event %s",
                            event.getMemberUuid()),
                    error);
            return false;
        }
    }

    private void scheduleMemberEventAcknowledgement(DirtyJobMemberEvent event, long delayMillis) {
        pendingMemberEventAcknowledgements.merge(
                event.getMemberUuid(),
                event,
                (current, requested) ->
                        current.getSequence() >= requested.getSequence() ? current : requested);
        if (journalExecutor.isShutdown()
                || !runningMemberEventAcknowledgements.add(event.getMemberUuid())) {
            return;
        }
        try {
            journalExecutor.schedule(
                    () -> {
                        DirtyJobMemberEvent requested =
                                pendingMemberEventAcknowledgements.get(event.getMemberUuid());
                        boolean acknowledged = acknowledgeMemberEvent(requested);
                        if (acknowledged) {
                            pendingMemberEventAcknowledgements.remove(
                                    event.getMemberUuid(), requested);
                        }
                        runningMemberEventAcknowledgements.remove(event.getMemberUuid());
                        DirtyJobMemberEvent remaining =
                                pendingMemberEventAcknowledgements.get(event.getMemberUuid());
                        if (remaining != null) {
                            scheduleMemberEventAcknowledgement(
                                    remaining, acknowledged ? 0L : TRACKING_RETRY_DELAY_MILLIS);
                        }
                    },
                    delayMillis,
                    TimeUnit.MILLISECONDS);
        } catch (Throwable scheduleError) {
            runningMemberEventAcknowledgements.remove(event.getMemberUuid());
            logger.warning(
                    String.format(
                            "Failed to schedule acknowledgement for dirty-job member event %s",
                            event.getMemberUuid()),
                    scheduleError);
        }
    }

    private void boundLocalMemberEvents() {
        int limit = Math.max(1, eventHistorySize * 4);
        while (localPendingMemberEvents.size() > limit) {
            String oldestMemberUuid =
                    localPendingMemberEvents.entrySet().stream()
                            .min(
                                    (left, right) ->
                                            Long.compare(
                                                    left.getValue().getEventTime(),
                                                    right.getValue().getEventTime()))
                            .map(Map.Entry::getKey)
                            .orElse(null);
            if (oldestMemberUuid == null) {
                return;
            }
            localPendingMemberEvents.remove(oldestMemberUuid);
            localMemberEventReconciliations.remove(oldestMemberUuid);
            localTrackingGap = true;
        }
    }

    private void addLocalTrackingGap(Map<String, DirtyJobMemberEvent> eventsByMember) {
        if (localTrackingGap) {
            eventsByMember.put(LOCAL_TRACKING_GAP_MEMBER_UUID, createLocalTrackingGapEvent());
        }
    }

    /** Forces the current replay round to fail closed when the shared journal cannot be read. */
    static List<DirtyJobMemberEvent> buildFailClosedPendingMemberEvents(
            Collection<DirtyJobMemberEvent> localEvents) {
        Map<String, DirtyJobMemberEvent> eventsByMember = new HashMap<>();
        if (localEvents != null) {
            localEvents.forEach(event -> eventsByMember.put(event.getMemberUuid(), event));
        }
        eventsByMember.put(LOCAL_TRACKING_GAP_MEMBER_UUID, createLocalTrackingGapEvent());
        List<DirtyJobMemberEvent> events = new ArrayList<>(eventsByMember.values());
        events.sort((left, right) -> Long.compare(left.getSequence(), right.getSequence()));
        return events;
    }

    private static DirtyJobMemberEvent createLocalTrackingGapEvent() {
        return new DirtyJobMemberEvent(
                -1L, LOCAL_TRACKING_GAP_MEMBER_UUID, "", 0, System.currentTimeMillis());
    }

    private <V> boolean removeWithBudget(IMap<Long, V> map, long jobId, String stateName) {
        try {
            V current =
                    map.getAsync(jobId)
                            .toCompletableFuture()
                            .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return current == null
                    || map.tryRemove(
                            jobId, TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warning(
                    String.format("Failed to remove dirty-job %s for job %s", stateName, jobId),
                    error);
            return false;
        }
    }

    private long getCurrentMemberEventSequence() {
        try {
            Long sequence =
                    memberEventSequenceMap
                            .getAsync(MEMBER_EVENT_SEQUENCE_KEY)
                            .toCompletableFuture()
                            .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return sequence == null ? 0L : sequence;
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException(
                    "Failed to read dirty-job member-event sequence", error);
        }
    }

    private void handleTrackingFailure(long jobId, String operation, Throwable error) {
        logger.warning(String.format("Failed to %s for job %s", operation, jobId), error);
        markTrackingIncomplete(jobId, operation + " failed: " + error.getClass().getSimpleName());
    }

    private CompletableFuture<Void> markTrackingIncomplete(long jobId, String reason) {
        long jobCreateTime = locallyOwnedJobCreateTimes.getOrDefault(jobId, 0L);
        return markTrackingIncomplete(jobId, jobCreateTime, reason);
    }

    private CompletableFuture<Void> markTrackingIncomplete(
            long jobId, long jobCreateTime, String reason) {
        if (!registerLocalIncompleteOwner(
                locallyOwnedJobCreateTimes, locallyIncompleteJobOwners, jobId, jobCreateTime)) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            CompletableFuture<Void> update =
                    dirtyJobStateMap
                            .submitToKey(jobId, new MarkIncompleteProcessor(jobCreateTime, reason))
                            .toCompletableFuture();
            update.whenComplete(
                    (ignored, markError) -> {
                        if (markError != null) {
                            logger.warning(
                                    String.format(
                                            "Failed to mark dirty-job tracking incomplete for job %s",
                                            jobId),
                                    markError);
                            scheduleIncompleteMarkerRetry(jobId, jobCreateTime, reason);
                        }
                    });
            scheduleIncompleteMarkerWatchdog(jobId, jobCreateTime, reason, update);
            return update;
        } catch (Throwable markError) {
            logger.warning(
                    String.format("Failed to submit dirty-job incomplete marker for job %s", jobId),
                    markError);
            scheduleIncompleteMarkerRetry(jobId, jobCreateTime, reason);
            return CompletableFuture.completedFuture(null);
        }
    }

    private void scheduleIncompleteMarkerWatchdog(
            long jobId, long jobCreateTime, String reason, CompletableFuture<Void> update) {
        if (trackingRetryExecutor.isShutdown()) {
            return;
        }
        try {
            trackingRetryExecutor.schedule(
                    () -> {
                        if (!update.isDone()) {
                            scheduleIncompleteMarkerRetry(jobId, jobCreateTime, reason);
                        }
                    },
                    TRACKING_OPERATION_TIMEOUT_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (Throwable scheduleError) {
            logger.warning(
                    String.format(
                            "Failed to schedule dirty-job incomplete marker watchdog for job %s",
                            jobId),
                    scheduleError);
        }
    }

    private void scheduleIncompleteMarkerRetry(long jobId, long jobCreateTime, String reason) {
        String retryKey = jobId + ":" + jobCreateTime;
        if (trackingRetryExecutor.isShutdown() || !incompleteMarkerRetries.add(retryKey)) {
            return;
        }
        try {
            trackingRetryExecutor.schedule(
                    () -> {
                        incompleteMarkerRetries.remove(retryKey);
                        markTrackingIncomplete(jobId, jobCreateTime, reason);
                    },
                    TRACKING_RETRY_DELAY_MILLIS,
                    TimeUnit.MILLISECONDS);
        } catch (Throwable scheduleError) {
            incompleteMarkerRetries.remove(retryKey);
            logger.warning(
                    String.format(
                            "Failed to schedule dirty-job incomplete marker for job %s", jobId),
                    scheduleError);
        }
    }

    private void scheduleLocalMemberEventReconciliation(String memberUuid) {
        if (journalExecutor.isShutdown() || !localMemberEventReconciliations.add(memberUuid)) {
            return;
        }
        journalExecutor.schedule(
                () -> reconcileLocalMemberEvent(memberUuid),
                TRACKING_RETRY_DELAY_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void reconcileLocalMemberEvent(String memberUuid) {
        if (!localPendingMemberEvents.containsKey(memberUuid)) {
            localMemberEventReconciliations.remove(memberUuid);
            return;
        }
        try {
            DirtyJobMemberEvent persisted =
                    pendingMemberEventMap
                            .getAsync(memberUuid)
                            .toCompletableFuture()
                            .get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (persisted != null && persisted.getSequence() >= 0) {
                localPendingMemberEvents.remove(memberUuid);
                localMemberEventReconciliations.remove(memberUuid);
                return;
            }
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.warning(
                    String.format("Failed to reconcile local dirty-job event %s", memberUuid),
                    error);
        }
        localMemberEventReconciliations.remove(memberUuid);
        scheduleLocalMemberEventReconciliation(memberUuid);
    }

    private <R> CompletableFuture<R> submitStateUpdate(
            long jobId, DirtyJobProcessor<R> processor, String operation) {
        CompletableFuture<R> update;
        long epoch = coordinatorEpoch.get();
        try {
            synchronized (pendingStateUpdates) {
                CompletionStage<?> previous = pendingStateUpdates.get(jobId);
                CompletionStage<R> submitted =
                        previous == null
                                ? dirtyJobStateMap.submitToKey(jobId, processor)
                                : previous.handle((ignored, error) -> null)
                                        .thenCompose(
                                                ignored ->
                                                        dirtyJobStateMap.submitToKey(
                                                                jobId, processor));
                update = submitted.toCompletableFuture();
                pendingStateUpdates.put(jobId, update);
            }
        } catch (Throwable error) {
            if (coordinatorEpoch.get() == epoch) {
                handleTrackingFailure(jobId, operation, error);
            }
            return CompletableFuture.completedFuture(null);
        }
        update.whenComplete(
                (ignored, error) -> {
                    synchronized (pendingStateUpdates) {
                        pendingStateUpdates.remove(jobId, update);
                    }
                    if (error != null && coordinatorEpoch.get() == epoch) {
                        handleTrackingFailure(jobId, operation, error);
                    }
                });
        return update;
    }

    private <R> R awaitStateUpdate(long jobId, CompletableFuture<R> update, String operation) {
        long epoch = coordinatorEpoch.get();
        try {
            return update.get(TRACKING_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (Throwable error) {
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (coordinatorEpoch.get() == epoch) {
                handleTrackingFailure(jobId, operation, error);
            }
            return null;
        }
    }

    private void markLocallyIncomplete(long jobId, DirtyJobState state) {
        Long expectedJobCreateTime = locallyIncompleteJobOwners.get(jobId);
        if (state != null
                && expectedJobCreateTime != null
                && expectedJobCreateTime == state.getJobCreateTime()) {
            state.markTrackingIncomplete("A local dirty-job tracking operation did not complete");
        }
    }

    /**
     * Registers the current execution and removes local UNKNOWN evidence from another execution.
     */
    static void registerLocalOwner(
            Map<Long, Long> owners,
            Map<Long, Long> incompleteOwners,
            long jobId,
            long jobCreateTime,
            boolean restart) {
        synchronized (owners) {
            owners.put(jobId, jobCreateTime);
            Long incompleteOwner = incompleteOwners.get(jobId);
            if (!restart || (incompleteOwner != null && incompleteOwner != jobCreateTime)) {
                incompleteOwners.remove(jobId);
            }
        }
    }

    /** Retains local UNKNOWN evidence only while no different execution owns the same job ID. */
    static boolean registerLocalIncompleteOwner(
            Map<Long, Long> owners,
            Map<Long, Long> incompleteOwners,
            long jobId,
            long jobCreateTime) {
        synchronized (owners) {
            Long currentOwner = owners.get(jobId);
            if (currentOwner != null && currentOwner != jobCreateTime) {
                return false;
            }
            incompleteOwners.put(jobId, jobCreateTime);
            return true;
        }
    }

    static boolean isOwnedBy(DirtyJobState state, long expectedJobCreateTime) {
        return state != null && state.getJobCreateTime() == expectedJobCreateTime;
    }

    /**
     * Base processor applying the same deterministic mutation to primary and synchronous backups.
     */
    private abstract static class DirtyJobProcessor<R>
            implements EntryProcessor<Long, DirtyJobState, R>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public EntryProcessor<Long, DirtyJobState, R> getBackupProcessor() {
            return this;
        }
    }

    /** Initializes or validates state for one immutable job execution. */
    private static class InitializeProcessor extends DirtyJobProcessor<DirtyJobState> {
        private static final long serialVersionUID = 1L;

        private final long jobId;
        private final long jobCreateTime;
        private final int threshold;
        private final int eventHistorySize;
        private final long pendingIncidentTtlMillis;
        private final long memberEventSequence;
        private final boolean missingIsIncomplete;
        private final boolean resetExisting;
        private final boolean enabledMarkerIncomplete;

        private InitializeProcessor(
                long jobId,
                long jobCreateTime,
                int threshold,
                int eventHistorySize,
                long pendingIncidentTtlMillis,
                long memberEventSequence,
                boolean missingIsIncomplete,
                boolean resetExisting,
                boolean enabledMarkerIncomplete) {
            this.jobId = jobId;
            this.jobCreateTime = jobCreateTime;
            this.threshold = threshold;
            this.eventHistorySize = eventHistorySize;
            this.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
            this.memberEventSequence = memberEventSequence;
            this.missingIsIncomplete = missingIsIncomplete;
            this.resetExisting = resetExisting;
            this.enabledMarkerIncomplete = enabledMarkerIncomplete;
        }

        @Override
        public DirtyJobState process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            boolean ownerMismatch = state != null && state.getJobCreateTime() != jobCreateTime;
            if (state == null || resetExisting || ownerMismatch) {
                state =
                        DirtyJobState.create(
                                jobId,
                                jobCreateTime,
                                threshold,
                                eventHistorySize,
                                pendingIncidentTtlMillis,
                                memberEventSequence);
                if (missingIsIncomplete) {
                    state.markTrackingIncomplete(
                            "Dirty-job state was missing during active-master recovery");
                }
                if (ownerMismatch) {
                    state.markTrackingIncomplete(
                            "Dirty-job state belonged to a different job execution");
                }
            } else {
                state.validateConfiguration(threshold, eventHistorySize, pendingIncidentTtlMillis);
            }
            if (enabledMarkerIncomplete) {
                state.markTrackingIncomplete(
                        "Dirty-job enabled marker was not persisted within the tracking budget");
            }
            entry.setValue(state);
            return state;
        }
    }

    /** Allocates the next cluster-wide member-event sequence. */
    private static class IncrementSequenceProcessor extends SequenceProcessor<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long process(Map.Entry<Long, Long> entry) {
            long next = entry.getValue() == null ? 1L : entry.getValue() + 1L;
            entry.setValue(next);
            return next;
        }
    }

    /** Base processor for the synchronously backed member-event sequence map. */
    private abstract static class SequenceProcessor<R>
            implements EntryProcessor<Long, Long, R>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public EntryProcessor<Long, Long, R> getBackupProcessor() {
            return this;
        }
    }

    /** Retains only the highest observed event sequence for one member UUID. */
    private static class JournalMemberEventProcessor
            implements EntryProcessor<String, DirtyJobMemberEvent, DirtyJobMemberEvent>,
                    Serializable {
        private static final long serialVersionUID = 1L;

        private final DirtyJobMemberEvent event;

        private JournalMemberEventProcessor(DirtyJobMemberEvent event) {
            this.event = event;
        }

        @Override
        public DirtyJobMemberEvent process(Map.Entry<String, DirtyJobMemberEvent> entry) {
            DirtyJobMemberEvent current = entry.getValue();
            if (current == null || current.getSequence() < event.getSequence()) {
                entry.setValue(event);
                return event;
            }
            return current;
        }

        @Override
        public EntryProcessor<String, DirtyJobMemberEvent, DirtyJobMemberEvent>
                getBackupProcessor() {
            return this;
        }
    }

    /** Atomically converts one exact journal event into a short-lived acknowledgement tombstone. */
    private static class AcknowledgeJournalEventProcessor
            implements EntryProcessor<String, DirtyJobMemberEvent, Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        private final long expectedSequence;
        private final long tombstoneTtlMillis;

        private AcknowledgeJournalEventProcessor(long expectedSequence, long tombstoneTtlMillis) {
            this.expectedSequence = expectedSequence;
            this.tombstoneTtlMillis = tombstoneTtlMillis;
        }

        @Override
        public Boolean process(Map.Entry<String, DirtyJobMemberEvent> entry) {
            DirtyJobMemberEvent current = entry.getValue();
            if (current == null || current.getSequence() != expectedSequence) {
                return false;
            }
            ((ExtendedMapEntry<String, DirtyJobMemberEvent>) entry)
                    .setValue(current, tombstoneTtlMillis, TimeUnit.MILLISECONDS);
            return true;
        }

        @Override
        public EntryProcessor<String, DirtyJobMemberEvent, Boolean> getBackupProcessor() {
            return this;
        }
    }

    /** Advances a new job to the current watermark before scheduling can begin. */
    private static class AdvanceSequenceProcessor extends DirtyJobProcessor<Void> {
        private static final long serialVersionUID = 1L;
        private final long jobId;
        private final long jobCreateTime;
        private final int threshold;
        private final int eventHistorySize;
        private final long pendingIncidentTtlMillis;
        private final long memberEventSequence;

        private AdvanceSequenceProcessor(
                long jobId,
                long jobCreateTime,
                int threshold,
                int eventHistorySize,
                long pendingIncidentTtlMillis,
                long memberEventSequence) {
            this.jobId = jobId;
            this.jobCreateTime = jobCreateTime;
            this.threshold = threshold;
            this.eventHistorySize = eventHistorySize;
            this.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
            this.memberEventSequence = memberEventSequence;
        }

        @Override
        public Void process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (state != null && !isOwnedBy(state, jobCreateTime)) {
                return null;
            }
            if (state == null) {
                state =
                        DirtyJobState.create(
                                jobId,
                                jobCreateTime,
                                threshold,
                                eventHistorySize,
                                pendingIncidentTtlMillis,
                                memberEventSequence);
                state.markTrackingIncomplete(
                        "Dirty-job state was missing before the job became schedulable");
            }
            state.advanceMemberEventSequence(memberEventSequence);
            entry.setValue(state);
            return null;
        }
    }

    /** Records one member-loss incident and the pipelines assigned to that member. */
    private static class MemberEventProcessor extends DirtyJobProcessor<Void> {
        private static final long serialVersionUID = 1L;

        private final long jobId;
        private final long jobCreateTime;
        private final int threshold;
        private final int eventHistorySize;
        private final long pendingIncidentTtlMillis;
        private final long memberEventSequence;
        private final String lostMemberUuid;
        private final String lostAddress;
        private final Collection<Integer> affectedPipelineIds;
        private final long timestamp;

        private MemberEventProcessor(
                long jobId,
                long jobCreateTime,
                int threshold,
                int eventHistorySize,
                long pendingIncidentTtlMillis,
                long memberEventSequence,
                String lostMemberUuid,
                String lostAddress,
                Collection<Integer> affectedPipelineIds,
                long timestamp) {
            this.jobId = jobId;
            this.jobCreateTime = jobCreateTime;
            this.threshold = threshold;
            this.eventHistorySize = eventHistorySize;
            this.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
            this.memberEventSequence = memberEventSequence;
            this.lostMemberUuid = lostMemberUuid;
            this.lostAddress = lostAddress;
            this.affectedPipelineIds =
                    affectedPipelineIds == null ? Collections.emptyList() : affectedPipelineIds;
            this.timestamp = timestamp;
        }

        @Override
        public Void process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (state != null && !isOwnedBy(state, jobCreateTime)) {
                return null;
            }
            if (state == null) {
                state =
                        DirtyJobState.create(
                                jobId,
                                jobCreateTime,
                                threshold,
                                eventHistorySize,
                                pendingIncidentTtlMillis,
                                Math.max(0L, memberEventSequence - 1L));
                state.markTrackingIncomplete(
                        "Dirty-job state was missing while processing member loss");
            }
            state.processMemberEvent(
                    memberEventSequence,
                    lostMemberUuid,
                    lostAddress,
                    MemberLeaveClassification.UNCLASSIFIED,
                    affectedPipelineIds,
                    timestamp);
            entry.setValue(state);
            return null;
        }
    }

    /** Allocates or reuses a stable attempt identity before deployment starts. */
    private static class PrepareAttemptProcessor extends DirtyJobProcessor<String> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final int pipelineId;
        private final long timestamp;

        private PrepareAttemptProcessor(
                long expectedJobCreateTime, int pipelineId, long timestamp) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.pipelineId = pipelineId;
            this.timestamp = timestamp;
        }

        @Override
        public String process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (!isOwnedBy(state, expectedJobCreateTime)) {
                return null;
            }
            String attemptId = state.prepareRecoveryAttempt(pipelineId, timestamp);
            entry.setValue(state);
            return attemptId;
        }
    }

    /** Counts an attempt only after persisted pipeline state proves deployment started. */
    private static class ConfirmAttemptProcessor extends DirtyJobProcessor<Void> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final int pipelineId;
        private final String attemptId;
        private final long timestamp;

        private ConfirmAttemptProcessor(
                long expectedJobCreateTime, int pipelineId, String attemptId, long timestamp) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.pipelineId = pipelineId;
            this.attemptId = attemptId;
            this.timestamp = timestamp;
        }

        @Override
        public Void process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (isOwnedBy(state, expectedJobCreateTime)) {
                state.confirmRecoveryAttempt(pipelineId, attemptId, timestamp);
                entry.setValue(state);
            }
            return null;
        }
    }

    /** Completes one affected pipeline after all task groups report RUNNING. */
    private static class CompletePipelineProcessor extends DirtyJobProcessor<Void> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final int pipelineId;
        private final String attemptId;
        private final long timestamp;

        private CompletePipelineProcessor(
                long expectedJobCreateTime, int pipelineId, String attemptId, long timestamp) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.pipelineId = pipelineId;
            this.attemptId = attemptId;
            this.timestamp = timestamp;
        }

        @Override
        public Void process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (isOwnedBy(state, expectedJobCreateTime)) {
                state.completeRecoveryPipeline(pipelineId, attemptId, timestamp);
                entry.setValue(state);
            }
            return null;
        }
    }

    /** Rebinds persisted deployment state to its stable attempt after an active-master switch. */
    private static class ReconcilePipelineProcessor extends DirtyJobProcessor<String> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final int pipelineId;
        private final boolean recovered;
        private final long timestamp;

        private ReconcilePipelineProcessor(
                long expectedJobCreateTime, int pipelineId, boolean recovered, long timestamp) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.pipelineId = pipelineId;
            this.recovered = recovered;
            this.timestamp = timestamp;
        }

        @Override
        public String process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (isOwnedBy(state, expectedJobCreateTime)) {
                String attemptId = state.confirmCurrentRecoveryAttempt(pipelineId, timestamp);
                if (recovered) {
                    state.completeCurrentRecoveryPipeline(pipelineId, timestamp);
                }
                entry.setValue(state);
                return attemptId;
            }
            return null;
        }
    }

    /** Closes any unfinished episode before terminal history is persisted. */
    private static class FinishEpisodeProcessor extends DirtyJobProcessor<DirtyJobState> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final long timestamp;

        private FinishEpisodeProcessor(long expectedJobCreateTime, long timestamp) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.timestamp = timestamp;
        }

        @Override
        public DirtyJobState process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (isOwnedBy(state, expectedJobCreateTime)) {
                state.finishActiveEpisode(timestamp);
                entry.setValue(state);
                return state;
            }
            return null;
        }
    }

    /** Permanently changes the matching execution to UNKNOWN after tracking evidence is lost. */
    private static class MarkIncompleteProcessor extends DirtyJobProcessor<Void> {
        private static final long serialVersionUID = 1L;
        private final long expectedJobCreateTime;
        private final String reason;

        private MarkIncompleteProcessor(long expectedJobCreateTime, String reason) {
            this.expectedJobCreateTime = expectedJobCreateTime;
            this.reason = reason;
        }

        @Override
        public Void process(Map.Entry<Long, DirtyJobState> entry) {
            DirtyJobState state = entry.getValue();
            if (isOwnedBy(state, expectedJobCreateTime)) {
                state.markTrackingIncomplete(reason);
                entry.setValue(state);
            }
            return null;
        }
    }
}
