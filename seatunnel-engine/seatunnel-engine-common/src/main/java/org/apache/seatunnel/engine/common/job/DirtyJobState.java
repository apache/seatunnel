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

package org.apache.seatunnel.engine.common.job;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HA state used to evaluate member-loss recovery degradation for one job.
 *
 * <p>The state separates member incidents, job recovery episodes, and per-pipeline deployment
 * attempts. Only confirmed deployment generations advance the job counter. Incident UUIDs, attempt
 * IDs, and the cluster member-event watermark make replay idempotent and prevent incomplete
 * tracking from being reported as clean.
 */
@Data
@NoArgsConstructor
public class DirtyJobState implements Serializable {
    private static final long serialVersionUID = 1L;

    // Stable engine job identity and distributed-map key.
    private long jobId;
    // Submission creation time preventing delayed updates from crossing job-ID reuse boundaries.
    private long jobCreateTime;
    // Positive job-scoped attempt threshold captured at submission.
    private int threshold;
    // Irreversible flag once the cumulative attempt threshold is reached.
    private boolean dirty;
    // Query result which is independent from the primary JobStatus.
    private DirtyJobEvaluationStatus evaluationStatus;
    // False whenever the engine can no longer prove the observation is complete.
    private boolean trackingComplete;
    // Cumulative job-level attempt generations across all episodes.
    private int recoveryAttemptCount;
    // Number of member-loss recovery episodes created for this job.
    private int recoveryEpisodeCount;
    // Number of distinct member UUID incidents accepted for this job.
    private int memberLossIncidentCount;
    // Mutable context for the member-loss recovery currently in progress.
    private ActiveRecoveryEpisode activeEpisode;
    // Bounded diagnostic summaries for completed, aborted, or expired episodes.
    private List<RecoveryEpisodeSummary> recentEpisodes;
    // Bounded member UUID identities used for idempotent replay.
    private Set<String> recentIncidentIds;
    // First time the cumulative threshold was reached.
    private Long firstDirtyTime;
    // Most recent time a job-level generation advanced.
    private Long lastCountedTime;
    // Highest cluster member-event sequence processed by this job.
    private long lastProcessedMemberEventSequence;
    // Most recent trusted or unclassified departure classification.
    private MemberLeaveClassification lastLeaveClassification;
    // Diagnostic UUID of the most recently accepted member incident.
    private String lastLostMemberUuid;
    // Diagnostic host and port of the most recently accepted incident.
    private String lastLostAddress;
    // Operator-facing evidence explaining an UNKNOWN result.
    private String incompleteReason;
    // Maximum retained episode summaries used by this serialized state.
    private int eventHistorySize;
    // Maximum lifetime of an episode which never completes deployment.
    private long pendingIncidentTtlMillis;

    public static DirtyJobState create(
            long jobId,
            int threshold,
            int eventHistorySize,
            long pendingIncidentTtlMillis,
            long memberEventSequence) {
        return create(
                jobId,
                0L,
                threshold,
                eventHistorySize,
                pendingIncidentTtlMillis,
                memberEventSequence);
    }

    public static DirtyJobState create(
            long jobId,
            long jobCreateTime,
            int threshold,
            int eventHistorySize,
            long pendingIncidentTtlMillis,
            long memberEventSequence) {
        DirtyJobState state = new DirtyJobState();
        state.jobId = jobId;
        state.jobCreateTime = jobCreateTime;
        state.threshold = threshold;
        state.evaluationStatus = DirtyJobEvaluationStatus.CLEAN;
        state.trackingComplete = true;
        state.recentEpisodes = new ArrayList<>();
        state.recentIncidentIds = new LinkedHashSet<>();
        state.eventHistorySize = eventHistorySize;
        state.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
        state.lastProcessedMemberEventSequence = memberEventSequence;
        return state;
    }

    public void validateConfiguration(
            int configuredThreshold,
            int configuredEventHistorySize,
            long configuredPendingIncidentTtlMillis) {
        ensureCollections();
        if (threshold != configuredThreshold
                || eventHistorySize != configuredEventHistorySize
                || pendingIncidentTtlMillis != configuredPendingIncidentTtlMillis) {
            markTrackingIncomplete("Dirty-job configuration changed for an existing job");
        }
    }

    public void processMemberEvent(
            long memberEventSequence,
            String lostMemberUuid,
            String lostAddress,
            MemberLeaveClassification classification,
            Collection<Integer> affectedPipelineIds,
            long now) {
        ensureCollections();
        lastProcessedMemberEventSequence =
                Math.max(lastProcessedMemberEventSequence, memberEventSequence);
        if (affectedPipelineIds == null || affectedPipelineIds.isEmpty()) {
            refreshEvaluation();
            return;
        }

        String incidentId = jobId + ":" + lostMemberUuid;
        lastLeaveClassification = classification;
        lastLostMemberUuid = lostMemberUuid;
        lastLostAddress = lostAddress;
        if (recentIncidentIds.contains(incidentId)) {
            refreshEvaluation();
            return;
        }

        rememberIncident(incidentId);
        expireStaleEpisode(now);
        if (activeEpisode == null) {
            activeEpisode =
                    ActiveRecoveryEpisode.create(
                            jobId + "-" + memberEventSequence,
                            affectedPipelineIds,
                            classification,
                            now);
            recoveryEpisodeCount++;
        } else {
            activeEpisode.addIncident(affectedPipelineIds, classification);
        }
        memberLossIncidentCount++;
        refreshEvaluation();
    }

    public void advanceMemberEventSequence(long memberEventSequence) {
        lastProcessedMemberEventSequence =
                Math.max(lastProcessedMemberEventSequence, memberEventSequence);
        refreshEvaluation();
    }

    /**
     * Creates or reuses the stable PREPARED attempt for an affected, not-yet-recovered pipeline.
     */
    public String prepareRecoveryAttempt(int pipelineId, long now) {
        ensureCollections();
        expireStaleEpisode(now);
        if (activeEpisode == null
                || !activeEpisode.affectedPipelineIds.contains(pipelineId)
                || activeEpisode.recoveredPipelineIds.contains(pipelineId)) {
            return null;
        }
        RecoveryDeploymentAttempt currentAttempt = activeEpisode.currentAttempts.get(pipelineId);
        if (currentAttempt != null && currentAttempt.status == RecoveryAttemptStatus.PREPARED) {
            return currentAttempt.attemptId;
        }
        int prepareSequence = activeEpisode.nextPrepareSequence(pipelineId);
        String attemptId = activeEpisode.episodeId + ":" + pipelineId + ":" + prepareSequence;
        activeEpisode.currentAttempts.put(
                pipelineId,
                new RecoveryDeploymentAttempt(
                        attemptId,
                        pipelineId,
                        prepareSequence,
                        RecoveryAttemptStatus.PREPARED,
                        now,
                        null));
        return attemptId;
    }

    /**
     * Confirms a stable attempt after the recovery pipeline state is durably set to DEPLOYING.
     *
     * @param attemptId stable identity allocated during restore preparation
     */
    public void confirmRecoveryAttempt(int pipelineId, String attemptId, long now) {
        ensureCollections();
        if (activeEpisode == null) {
            return;
        }
        RecoveryDeploymentAttempt attempt = activeEpisode.currentAttempts.get(pipelineId);
        if (attempt == null || !attempt.attemptId.equals(attemptId)) {
            return;
        }
        confirmAttempt(attempt, now);
    }

    /**
     * Reconciles the current attempt after an active-master switch without allocating a new ID.
     *
     * @param pipelineId restored pipeline whose persisted state proves deployment started
     */
    public String confirmCurrentRecoveryAttempt(int pipelineId, long now) {
        ensureCollections();
        if (activeEpisode == null) {
            return null;
        }
        RecoveryDeploymentAttempt attempt = activeEpisode.currentAttempts.get(pipelineId);
        if (attempt != null) {
            confirmAttempt(attempt, now);
            return attempt.attemptId;
        }
        return null;
    }

    /**
     * Marks one affected pipeline recovered only when the confirmed attempt reached RUNNING.
     *
     * @param attemptId stable confirmed attempt which completed deployment
     */
    public void completeRecoveryPipeline(int pipelineId, String attemptId, long now) {
        ensureCollections();
        if (activeEpisode == null) {
            return;
        }
        RecoveryDeploymentAttempt attempt = activeEpisode.currentAttempts.get(pipelineId);
        if (attempt == null
                || !attempt.attemptId.equals(attemptId)
                || attempt.status != RecoveryAttemptStatus.CONFIRMED) {
            return;
        }
        completePipeline(pipelineId, now);
    }

    /**
     * Completes the current confirmed attempt during active-master reconciliation.
     *
     * @param pipelineId restored pipeline whose task groups are all RUNNING
     */
    public void completeCurrentRecoveryPipeline(int pipelineId, long now) {
        ensureCollections();
        if (activeEpisode == null) {
            return;
        }
        RecoveryDeploymentAttempt attempt = activeEpisode.currentAttempts.get(pipelineId);
        if (attempt != null && attempt.status == RecoveryAttemptStatus.CONFIRMED) {
            completePipeline(pipelineId, now);
        }
    }

    /**
     * Snapshots an unfinished episode as aborted before terminal job history is written.
     *
     * @param now terminal snapshot time
     */
    public void finishActiveEpisode(long now) {
        ensureCollections();
        if (activeEpisode != null) {
            addRecentEpisode(activeEpisode.toSummary(RecoveryEpisodeStatus.ABORTED, now));
            activeEpisode = null;
        }
        refreshEvaluation();
    }

    /**
     * Permanently prevents this running state from being evaluated as CLEAN.
     *
     * @param reason missing or inconsistent evidence exposed to operators
     */
    public void markTrackingIncomplete(String reason) {
        trackingComplete = false;
        incompleteReason = reason;
        evaluationStatus = DirtyJobEvaluationStatus.UNKNOWN;
    }

    /**
     * Returns a query-safe copy evaluated against the current cluster member-event watermark.
     *
     * @return evaluated copy which does not mutate HA state
     */
    public DirtyJobState evaluatedCopy(long currentMemberEventSequence) {
        return evaluatedCopy(currentMemberEventSequence, System.currentTimeMillis());
    }

    /**
     * Evaluates watermark and episode TTL completeness without mutating the HA state.
     *
     * @param now query time used for deterministic TTL evaluation
     * @return evaluated copy
     */
    public DirtyJobState evaluatedCopy(long currentMemberEventSequence, long now) {
        ensureCollections();
        DirtyJobState copy = copy();
        if (copy.lastProcessedMemberEventSequence < currentMemberEventSequence) {
            copy.markTrackingIncomplete("Member-event watermark is behind the cluster sequence");
        } else if (copy.activeEpisode != null
                && copy.pendingIncidentTtlMillis > 0
                && now - copy.activeEpisode.startedTime > copy.pendingIncidentTtlMillis) {
            copy.markTrackingIncomplete(
                    "A member-loss recovery episode exceeded its configured lifetime");
        } else {
            copy.refreshEvaluation();
        }
        return copy;
    }

    /**
     * Builds the bounded list-response projection.
     *
     * @return dirty flag, evaluation status, and cumulative attempt count
     */
    public DirtyJobSummary toSummary() {
        return new DirtyJobSummary(dirty, evaluationStatus, recoveryAttemptCount);
    }

    private void confirmAttempt(RecoveryDeploymentAttempt attempt, long now) {
        if (attempt.status == RecoveryAttemptStatus.CONFIRMED) {
            return;
        }
        attempt.status = RecoveryAttemptStatus.CONFIRMED;
        attempt.confirmedTime = now;
        int generation = attempt.prepareSequence;
        if (generation > activeEpisode.confirmedGeneration) {
            recoveryAttemptCount += generation - activeEpisode.confirmedGeneration;
            activeEpisode.confirmedGeneration = generation;
            lastCountedTime = now;
        }
        if (!dirty && recoveryAttemptCount >= threshold) {
            dirty = true;
            firstDirtyTime = now;
        }
        refreshEvaluation();
    }

    private void completePipeline(int pipelineId, long now) {
        activeEpisode.recoveredPipelineIds.add(pipelineId);
        if (activeEpisode.recoveredPipelineIds.containsAll(activeEpisode.affectedPipelineIds)) {
            addRecentEpisode(activeEpisode.toSummary(RecoveryEpisodeStatus.COMPLETED, now));
            activeEpisode = null;
        }
        refreshEvaluation();
    }

    private void expireStaleEpisode(long now) {
        if (activeEpisode == null
                || pendingIncidentTtlMillis <= 0
                || now - activeEpisode.startedTime <= pendingIncidentTtlMillis) {
            return;
        }
        addRecentEpisode(activeEpisode.toSummary(RecoveryEpisodeStatus.EXPIRED, now));
        activeEpisode = null;
        markTrackingIncomplete("A member-loss recovery episode expired before recovery completed");
    }

    private void rememberIncident(String incidentId) {
        recentIncidentIds.add(incidentId);
        int incidentLimit = Math.max(1, eventHistorySize * 4);
        while (recentIncidentIds.size() > incidentLimit) {
            String oldest = recentIncidentIds.iterator().next();
            recentIncidentIds.remove(oldest);
            markTrackingIncomplete(
                    "Member-loss incident deduplication history exceeded its configured bound");
        }
    }

    private void addRecentEpisode(RecoveryEpisodeSummary summary) {
        recentEpisodes.add(summary);
        while (recentEpisodes.size() > eventHistorySize) {
            recentEpisodes.remove(0);
        }
    }

    private void refreshEvaluation() {
        if (!trackingComplete) {
            evaluationStatus = DirtyJobEvaluationStatus.UNKNOWN;
        } else if (dirty) {
            evaluationStatus = DirtyJobEvaluationStatus.DIRTY;
        } else {
            evaluationStatus = DirtyJobEvaluationStatus.CLEAN;
        }
    }

    private void ensureCollections() {
        if (recentEpisodes == null) {
            recentEpisodes = new ArrayList<>();
        }
        if (recentIncidentIds == null) {
            recentIncidentIds = new LinkedHashSet<>();
        }
        if (activeEpisode != null) {
            activeEpisode.ensureCollections();
        }
    }

    private DirtyJobState copy() {
        DirtyJobState copy = new DirtyJobState();
        copy.jobId = jobId;
        copy.jobCreateTime = jobCreateTime;
        copy.threshold = threshold;
        copy.dirty = dirty;
        copy.evaluationStatus = evaluationStatus;
        copy.trackingComplete = trackingComplete;
        copy.recoveryAttemptCount = recoveryAttemptCount;
        copy.recoveryEpisodeCount = recoveryEpisodeCount;
        copy.memberLossIncidentCount = memberLossIncidentCount;
        copy.activeEpisode = activeEpisode;
        copy.recentEpisodes = new ArrayList<>(recentEpisodes);
        copy.recentIncidentIds = new LinkedHashSet<>(recentIncidentIds);
        copy.firstDirtyTime = firstDirtyTime;
        copy.lastCountedTime = lastCountedTime;
        copy.lastProcessedMemberEventSequence = lastProcessedMemberEventSequence;
        copy.lastLeaveClassification = lastLeaveClassification;
        copy.lastLostMemberUuid = lastLostMemberUuid;
        copy.lastLostAddress = lastLostAddress;
        copy.incompleteReason = incompleteReason;
        copy.eventHistorySize = eventHistorySize;
        copy.pendingIncidentTtlMillis = pendingIncidentTtlMillis;
        return copy;
    }

    /**
     * Active, bounded recovery context for pipelines affected by member loss.
     *
     * <p>Recovered pipelines cannot consume ordinary failures until a new incident affects them.
     */
    @Data
    @NoArgsConstructor
    public static class ActiveRecoveryEpisode implements Serializable {
        private static final long serialVersionUID = 1L;

        // Stable episode identity derived from job and first event sequence.
        private String episodeId;
        // Original member-event observation time.
        private long startedTime;
        // Pipelines which must recover before this episode can close.
        private Set<Integer> affectedPipelineIds;
        // Affected pipelines already recovered by a confirmed attempt.
        private Set<Integer> recoveredPipelineIds;
        // Monotonic per-pipeline preparation sequence used in stable attempt IDs.
        private Map<Integer, Integer> prepareSequences;
        // Current prepared or confirmed attempt for each affected pipeline.
        private Map<Integer, RecoveryDeploymentAttempt> currentAttempts;
        // Highest confirmed per-pipeline generation already counted at job level.
        private int confirmedGeneration;
        // Distinct member incidents merged into this continuous episode.
        private int incidentCount;
        // Most conservative classification among merged incidents.
        private MemberLeaveClassification classification;

        private static ActiveRecoveryEpisode create(
                String episodeId,
                Collection<Integer> affectedPipelineIds,
                MemberLeaveClassification classification,
                long now) {
            ActiveRecoveryEpisode episode = new ActiveRecoveryEpisode();
            episode.episodeId = episodeId;
            episode.startedTime = now;
            episode.affectedPipelineIds = new LinkedHashSet<>(affectedPipelineIds);
            episode.recoveredPipelineIds = new LinkedHashSet<>();
            episode.prepareSequences = new LinkedHashMap<>();
            episode.currentAttempts = new LinkedHashMap<>();
            episode.incidentCount = 1;
            episode.classification = classification;
            return episode;
        }

        private void addIncident(
                Collection<Integer> pipelineIds, MemberLeaveClassification newClassification) {
            ensureCollections();
            affectedPipelineIds.addAll(pipelineIds);
            recoveredPipelineIds.removeAll(pipelineIds);
            incidentCount++;
            if (newClassification == MemberLeaveClassification.UNCLASSIFIED) {
                classification = MemberLeaveClassification.UNCLASSIFIED;
            }
        }

        private int nextPrepareSequence(int pipelineId) {
            int sequence = prepareSequences.getOrDefault(pipelineId, 0) + 1;
            prepareSequences.put(pipelineId, sequence);
            return sequence;
        }

        private RecoveryEpisodeSummary toSummary(RecoveryEpisodeStatus status, long now) {
            return new RecoveryEpisodeSummary(
                    episodeId,
                    status,
                    startedTime,
                    now,
                    affectedPipelineIds.size(),
                    incidentCount,
                    confirmedGeneration,
                    classification);
        }

        private void ensureCollections() {
            if (affectedPipelineIds == null) {
                affectedPipelineIds = new LinkedHashSet<>();
            }
            if (recoveredPipelineIds == null) {
                recoveredPipelineIds = new LinkedHashSet<>();
            }
            if (prepareSequences == null) {
                prepareSequences = new LinkedHashMap<>();
            }
            if (currentAttempts == null) {
                currentAttempts = new LinkedHashMap<>();
            }
        }
    }

    /**
     * Stable identity and lifecycle for one recovery deployment attempt.
     *
     * <p>PREPARED is reused across a master switch and counted only after confirmation.
     */
    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    public static class RecoveryDeploymentAttempt implements Serializable {
        private static final long serialVersionUID = 1L;

        // Stable identity reused by active-master reconciliation.
        private String attemptId;
        // Pipeline owning this deployment attempt.
        private int pipelineId;
        // Per-pipeline generation embedded in the stable identity.
        private int prepareSequence;
        // PREPARED attempts are not counted until they become CONFIRMED.
        private RecoveryAttemptStatus status;
        // Time restore preparation allocated this identity.
        private long preparedTime;
        // Time persisted pipeline state proved deployment had started.
        private Long confirmedTime;
    }

    /**
     * Bounded episode record retained after the active context closes.
     *
     * <p>The record intentionally stores counts rather than unbounded task-group detail.
     */
    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    public static class RecoveryEpisodeSummary implements Serializable {
        private static final long serialVersionUID = 1L;

        // Stable identity of the closed episode.
        private String episodeId;
        // Terminal outcome of the bounded episode summary.
        private RecoveryEpisodeStatus status;
        // Original event time retained for diagnostics.
        private long startedTime;
        // Completion, abort, or expiry time.
        private long finishedTime;
        // Number of distinct pipelines involved in the episode.
        private int affectedPipelineCount;
        // Number of member incidents merged into the episode.
        private int incidentCount;
        // Highest deployment generation counted for the episode.
        private int confirmedGeneration;
        // Conservative departure classification for the episode.
        private MemberLeaveClassification classification;
    }

    /** Distinguishes allocated attempts from attempts proven to have started deployment. */
    public enum RecoveryAttemptStatus {
        PREPARED,
        CONFIRMED
    }

    /** Terminal outcome retained for a bounded recovery episode summary. */
    public enum RecoveryEpisodeStatus {
        COMPLETED,
        ABORTED,
        EXPIRED
    }
}
