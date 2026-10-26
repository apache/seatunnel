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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Covers dirty-job generation counting, replay idempotency, and incomplete tracking semantics.
 *
 * <p>The cases protect the boundary that ordinary business restore must not consume member-loss
 * context.
 */
class DirtyJobStateTest {

    @Test
    void shouldCountJobLevelRecoveryGenerationsAcrossPipelines() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Arrays.asList(1, 2),
                100L);

        String pipelineOneAttempt = state.prepareRecoveryAttempt(1, 110L);
        state.confirmRecoveryAttempt(1, pipelineOneAttempt, 120L);
        String pipelineTwoAttempt = state.prepareRecoveryAttempt(2, 130L);
        state.confirmRecoveryAttempt(2, pipelineTwoAttempt, 140L);

        Assertions.assertEquals(1, state.getRecoveryAttemptCount());
        Assertions.assertEquals(DirtyJobEvaluationStatus.CLEAN, state.getEvaluationStatus());

        String secondGeneration = state.prepareRecoveryAttempt(1, 150L);
        state.confirmRecoveryAttempt(1, secondGeneration, 160L);
        state.confirmRecoveryAttempt(1, secondGeneration, 170L);

        Assertions.assertEquals(2, state.getRecoveryAttemptCount());
        Assertions.assertTrue(state.isDirty());
        Assertions.assertEquals(DirtyJobEvaluationStatus.DIRTY, state.getEvaluationStatus());
    }

    @Test
    void shouldIgnoreOrdinaryRestoreWithoutMemberLossEpisode() {
        DirtyJobState state = DirtyJobState.create(1L, 1, 10, 600_000L, 0L);

        String attemptId = state.prepareRecoveryAttempt(1, 100L);

        Assertions.assertNull(attemptId);
        Assertions.assertEquals(0, state.getRecoveryAttemptCount());
        Assertions.assertEquals(DirtyJobEvaluationStatus.CLEAN, state.getEvaluationStatus());
    }

    @Test
    void shouldReusePreparedAttemptUntilDeploymentIsConfirmed() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);

        String firstPreparation = state.prepareRecoveryAttempt(1, 110L);
        String preparationAfterMasterSwitch = state.prepareRecoveryAttempt(1, 120L);
        state.confirmRecoveryAttempt(1, preparationAfterMasterSwitch, 130L);

        Assertions.assertEquals(firstPreparation, preparationAfterMasterSwitch);
        Assertions.assertEquals(1, state.getRecoveryAttemptCount());
    }

    @Test
    void shouldReturnStableAttemptDuringMasterSwitchReconciliation() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);
        String preparedAttempt = state.prepareRecoveryAttempt(1, 110L);

        String reconciledAttempt = state.confirmCurrentRecoveryAttempt(1, 120L);
        state.completeRecoveryPipeline(1, reconciledAttempt, 130L);

        Assertions.assertEquals(preparedAttempt, reconciledAttempt);
        Assertions.assertEquals(1, state.getRecoveryAttemptCount());
        Assertions.assertNull(state.getActiveEpisode());
    }

    @Test
    void shouldDeduplicateMemberEventAndAttemptConfirmation() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);
        state.processMemberEvent(
                2L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                110L);
        String attemptId = state.prepareRecoveryAttempt(1, 120L);

        state.confirmRecoveryAttempt(1, attemptId, 130L);
        state.confirmRecoveryAttempt(1, attemptId, 140L);

        Assertions.assertEquals(1, state.getRecoveryEpisodeCount());
        Assertions.assertEquals(1, state.getMemberLossIncidentCount());
        Assertions.assertEquals(1, state.getRecoveryAttemptCount());
        Assertions.assertEquals(2L, state.getLastProcessedMemberEventSequence());
    }

    @Test
    void shouldReportUnknownWhenMemberEventWatermarkLags() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 600_000L, 3L);

        DirtyJobState evaluated = state.evaluatedCopy(4L);

        Assertions.assertFalse(evaluated.isTrackingComplete());
        Assertions.assertEquals(DirtyJobEvaluationStatus.UNKNOWN, evaluated.getEvaluationStatus());
        Assertions.assertEquals(DirtyJobEvaluationStatus.CLEAN, state.getEvaluationStatus());
    }

    @Test
    void shouldReportUnknownWhenActiveEpisodeExpires() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 10, 100L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                100L);

        DirtyJobState evaluated = state.evaluatedCopy(1L, 201L);

        Assertions.assertEquals(DirtyJobEvaluationStatus.UNKNOWN, evaluated.getEvaluationStatus());
        Assertions.assertTrue(state.isTrackingComplete());
    }

    @Test
    void shouldRequireReaffectedPipelineToRecoverAgain() {
        DirtyJobState state = DirtyJobState.create(1L, 3, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Arrays.asList(1, 2),
                100L);
        String firstAttempt = state.prepareRecoveryAttempt(1, 110L);
        state.confirmRecoveryAttempt(1, firstAttempt, 120L);
        state.completeRecoveryPipeline(1, firstAttempt, 130L);

        state.processMemberEvent(
                2L,
                "member-2",
                "10.0.0.2:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Collections.singletonList(1),
                140L);

        Assertions.assertFalse(state.getActiveEpisode().getRecoveredPipelineIds().contains(1));
    }

    @Test
    void shouldIgnoreBusinessFailureAfterPipelineRecoveredWithinActiveEpisode() {
        DirtyJobState state = DirtyJobState.create(1L, 3, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Arrays.asList(1, 2),
                100L);
        String memberLossAttempt = state.prepareRecoveryAttempt(1, 110L);
        state.confirmRecoveryAttempt(1, memberLossAttempt, 120L);
        state.completeRecoveryPipeline(1, memberLossAttempt, 130L);

        String businessFailureAttempt = state.prepareRecoveryAttempt(1, 140L);

        Assertions.assertNull(businessFailureAttempt);
        Assertions.assertEquals(1, state.getRecoveryAttemptCount());
    }

    @Test
    void shouldCloseEpisodeOnlyAfterEveryAffectedPipelineRecovers() {
        DirtyJobState state = DirtyJobState.create(1L, 3, 10, 600_000L, 0L);
        state.processMemberEvent(
                1L,
                "member-1",
                "10.0.0.1:5801",
                MemberLeaveClassification.UNCLASSIFIED,
                Arrays.asList(1, 2),
                100L);
        String firstAttempt = state.prepareRecoveryAttempt(1, 110L);
        String secondAttempt = state.prepareRecoveryAttempt(2, 120L);
        state.confirmRecoveryAttempt(1, firstAttempt, 130L);
        state.confirmRecoveryAttempt(2, secondAttempt, 140L);

        state.completeRecoveryPipeline(1, firstAttempt, 150L);
        Assertions.assertNotNull(state.getActiveEpisode());

        state.completeRecoveryPipeline(2, secondAttempt, 160L);
        Assertions.assertNull(state.getActiveEpisode());
        Assertions.assertEquals(1, state.getRecentEpisodes().size());
        Assertions.assertEquals(
                DirtyJobState.RecoveryEpisodeStatus.COMPLETED,
                state.getRecentEpisodes().get(0).getStatus());
    }

    @Test
    void shouldBecomeUnknownWhenIncidentDeduplicationHistoryOverflows() {
        DirtyJobState state = DirtyJobState.create(1L, 2, 1, 60_000L, 0L);

        for (int sequence = 1; sequence <= 5; sequence++) {
            state.processMemberEvent(
                    sequence,
                    "member-" + sequence,
                    "127.0.0.1:" + sequence,
                    MemberLeaveClassification.UNCLASSIFIED,
                    Collections.singletonList(1),
                    sequence);
        }
        String attemptId = state.prepareRecoveryAttempt(1, 10L);
        state.confirmRecoveryAttempt(1, attemptId, 11L);
        state.completeRecoveryPipeline(1, attemptId, 12L);
        state.advanceMemberEventSequence(6L);
        DirtyJobState evaluated = state.evaluatedCopy(6L, 13L);

        Assertions.assertFalse(state.isTrackingComplete());
        Assertions.assertEquals(DirtyJobEvaluationStatus.UNKNOWN, state.getEvaluationStatus());
        Assertions.assertNull(state.getActiveEpisode());
        Assertions.assertEquals(DirtyJobEvaluationStatus.UNKNOWN, evaluated.getEvaluationStatus());
    }
}
