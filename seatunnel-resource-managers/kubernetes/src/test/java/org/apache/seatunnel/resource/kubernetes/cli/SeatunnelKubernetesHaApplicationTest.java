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

package org.apache.seatunnel.resource.kubernetes.cli;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesHaClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeatunnelKubernetesHaApplicationTest {

    @Test
    void overlappingOwnersHaveOnlyOneSubmissionWinner() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        AtomicBoolean claimed = new AtomicBoolean();
        doAnswer(
                        call -> {
                            if (!claimed.compareAndSet(false, true)) {
                                throw new KubernetesClientException(409, "stale version");
                            }
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        assertTrue(HaApplicationState.claimSubmission(api, owner("1", "new", null)));
        assertFalse(HaApplicationState.claimSubmission(api, owner("1", "new", null)));
        assertFalse(HaApplicationState.claimSubmission(api, owner("2", "requested", null)));
        verify(api, times(2)).updateHaState(any(), any());
    }

    @Test
    void terminalStateCannotClaimSubmissionEvenWhenIntentIsNew() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        assertFalse(HaApplicationState.claimSubmission(api, owner("2", "new", "FAILED")));
        verify(api, never()).updateHaState(any(), any());
    }

    @Test
    void staleOwnerCannotObserveReplacedApplicationOrChangedSpecification() {
        KubernetesJob owner = owner("1", "requested", null);
        assertSame(owner, HaApplicationState.validated(owner, "uid", "immutable-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () -> HaApplicationState.validated(owner, "replacement-uid", "immutable-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () -> HaApplicationState.validated(owner, "uid", "changed-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () -> HaApplicationState.validated(owner, "uid", "immutable-spec", 18));
    }

    @Test
    void nativeTerminalRecordIsImmutableAcrossConcurrentOwnerCompletion() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob old = owner("1", "requested", null);
        doThrow(new KubernetesClientException(409, "concurrent result"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(owner("2", "requested", "SUCCEEDED"));
        HaApplicationState.recordTerminal(api, old, ApplicationStatus.FAILED);
        verify(api, times(1)).updateHaState(any(), any());
        KubernetesJob replacement = owner("2", "requested", null);
        replacement.getInternalResource().getMetadata().setUid("other-uid");
        when(api.getJob("app")).thenReturn(replacement);
        assertThrows(
                IllegalStateException.class,
                () -> HaApplicationState.recordTerminal(api, old, ApplicationStatus.FAILED));
    }

    @Test
    void unknownAndNonterminalNativeStatesNeverPermitSuccessfulCleanup() {
        for (JobStatus status : JobStatus.values()) {
            ApplicationStatus expected =
                    status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE
                            ? ApplicationStatus.SUCCEEDED
                            : status == JobStatus.FAILED
                                    ? ApplicationStatus.FAILED
                                    : status == JobStatus.CANCELED
                                            ? ApplicationStatus.CANCELED
                                            : null;
            assertEquals(expected, SeatunnelKubernetesHaApplication.terminalStatus(status));
        }
    }

    @Test
    void ambiguousSubmissionIsObservedWithoutResubmission() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob current = owner("1", "new", null);
        when(api.getJob("app")).thenReturn(current);
        KubernetesPod worker = mock(KubernetesPod.class);
        when(api.listPods(any())).thenReturn(List.of(worker));
        doAnswer(
                        call -> {
                            current.getInternalResource()
                                    .getMetadata()
                                    .setAnnotations(new HashMap<>(call.getArgument(1)));
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        doAnswer(
                        call -> {
                            when(api.listPods(any())).thenReturn(Collections.emptyList());
                            return null;
                        })
                .when(api)
                .deleteHaResources(any());
        SeatunnelKubernetesHaApplication.NativeJob job =
                mock(SeatunnelKubernetesHaApplication.NativeJob.class);
        when(job.ready()).thenReturn(true);
        doThrow(new IllegalStateException("lost acceptance ACK")).when(job).submit();
        when(job.status()).thenReturn(JobStatus.UNKNOWABLE, JobStatus.RUNNING, JobStatus.FINISHED);
        SeatunnelKubernetesHaApplication.runApplication(
                api,
                "app",
                "uid",
                "immutable-spec",
                parameters(Map.of("application.worker-count", "1")).getSpecification(),
                job);
        verify(job, times(1)).submit();
        verify(job, times(3)).status();
        assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(current));
        verify(api).deleteHaResources(current);
    }

    @Test
    void fixedWorkersMayAppearWithinTheStartupBudget() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob current = owner("1", "new", null);
        when(api.getJob("app")).thenReturn(current);
        when(api.listPods(any())).thenReturn(Collections.emptyList());
        doAnswer(
                        call -> {
                            current.getInternalResource()
                                    .getMetadata()
                                    .setAnnotations(new HashMap<>(call.getArgument(1)));
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        SeatunnelKubernetesHaApplication.NativeJob job =
                mock(SeatunnelKubernetesHaApplication.NativeJob.class);
        AtomicBoolean waiting = new AtomicBoolean(true);
        when(job.ready())
                .thenAnswer(
                        call -> {
                            if (waiting.getAndSet(false)) {
                                when(api.listPods(any()))
                                        .thenReturn(List.of(mock(KubernetesPod.class)));
                                return false;
                            }
                            return true;
                        });
        when(job.status()).thenReturn(JobStatus.FINISHED);
        doAnswer(
                        call -> {
                            when(api.listPods(any())).thenReturn(Collections.emptyList());
                            return null;
                        })
                .when(api)
                .deleteHaResources(any());
        SeatunnelKubernetesHaApplication.runApplication(
                api, "app", "uid", "immutable-spec", parameters().getSpecification(), job);
        verify(job).submit();
        verify(job, times(2)).ready();
        assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(current));
    }

    @Test
    void explicitRestoreRequiresNativeCheckpointPayloadWithoutFallback() {
        JobClient jobs = mock(JobClient.class);
        assertTrue(SeatunnelKubernetesHaApplication.restoreCheckpoints(jobs, null).isEmpty());
        when(jobs.getCheckpointData(17L, RestoreMode.CHECKPOINT))
                .thenReturn(Collections.emptyList());
        assertThrows(
                IllegalArgumentException.class,
                () -> SeatunnelKubernetesHaApplication.restoreCheckpoints(jobs, 17L));
        List<JobPipelineCheckpointData> checkpoint = List.of(mock(JobPipelineCheckpointData.class));
        when(jobs.getCheckpointData(18L, RestoreMode.CHECKPOINT)).thenReturn(checkpoint);
        assertSame(checkpoint, SeatunnelKubernetesHaApplication.restoreCheckpoints(jobs, 18L));
        verify(jobs, times(1)).getCheckpointData(18L, RestoreMode.CHECKPOINT);
    }

    @Test
    void rejectedRestorePreparationFailsWithoutSubmittingAndReleasesCompute() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob current = owner("1", "new", null);
        when(api.getJob("app")).thenReturn(current);
        when(api.listPods(any())).thenReturn(Collections.emptyList());
        doAnswer(
                        call -> {
                            current.getInternalResource()
                                    .getMetadata()
                                    .setAnnotations(new HashMap<>(call.getArgument(1)));
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        SeatunnelKubernetesHaApplication.NativeJob job =
                mock(SeatunnelKubernetesHaApplication.NativeJob.class);
        when(job.ready()).thenReturn(true);
        doThrow(new IllegalArgumentException("No checkpoint found")).when(job).prepare();
        SeatunnelKubernetesHaApplication.runApplication(
                api, "app", "uid", "immutable-spec", parameters().getSpecification(), job);
        verify(job, never()).submit();
        verify(job, never()).status();
        assertEquals(ApplicationStatus.FAILED, HaApplicationState.terminal(current));
        verify(api).deleteHaResources(current);
    }

    @Test
    void claimedJobIsAttachedWithoutExecutingAgain() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob current = owner("1", "requested", "RUNNING");
        when(api.getJob("app")).thenReturn(current);
        when(api.listPods(any())).thenReturn(Collections.emptyList());
        doAnswer(
                        call -> {
                            current.getInternalResource()
                                    .getMetadata()
                                    .setAnnotations(new HashMap<>(call.getArgument(1)));
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        SeatunnelKubernetesHaApplication.NativeJob job =
                mock(SeatunnelKubernetesHaApplication.NativeJob.class);
        when(job.status()).thenReturn(JobStatus.FINISHED);
        SeatunnelKubernetesHaApplication.runApplication(
                api, "app", "uid", "immutable-spec", parameters().getSpecification(), job);
        verify(job, never()).submit();
        assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(current));
        verify(api).deleteHaResources(current);
    }

    @Test
    void startupFailureCannotOverwriteConcurrentNativeRunning() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob stale = owner("1", "new", null);
        KubernetesJob running = owner("2", "requested", "RUNNING");
        doThrow(new KubernetesClientException(409, "concurrent native RUNNING"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(running);
        assertFalse(HaApplicationState.recordStartupFailure(api, stale));
        verify(api, times(1)).updateHaState(any(), any());
        verify(api, never()).deleteHaResources(any());
    }

    @Test
    void startupFailureCannotSupersedeAnAcceptedSubmissionBeforeRunning() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob stale = owner("1", "new", null);
        KubernetesJob accepted = owner("2", "requested", "DEPLOYING");
        doThrow(new KubernetesClientException(409, "submission claimed concurrently"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(accepted);
        assertFalse(HaApplicationState.recordStartupFailure(api, stale));
        assertFalse(HaApplicationState.recordStartupFailure(api, accepted));
        verify(api, times(1)).updateHaState(any(), any());
        assertThrows(
                IllegalStateException.class,
                () -> HaApplicationState.finalizeResources(api, accepted));
        verify(api, never()).deleteHaResources(any());
    }

    @Test
    void nativeRunningCannotOverwriteConcurrentTerminal() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob stale = owner("1", "requested", null);
        KubernetesJob completed = owner("2", "requested", "SUCCEEDED");
        doThrow(new KubernetesClientException(409, "concurrent native completion"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(completed);
        HaApplicationState.recordRunning(api, stale);
        verify(api, times(1)).updateHaState(any(), any());
        assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(completed));
    }

    @Test
    void failedRunningPersistenceDoesNotReleaseCompute() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        doThrow(new KubernetesClientException(503, "fixture API unavailable"))
                .when(api)
                .updateHaState(any(), any());
        assertThrows(
                KubernetesClientException.class,
                () -> HaApplicationState.recordRunning(api, owner("1", "requested", null)));
        verify(api, never()).deleteHaResources(any());
    }

    private static KubernetesJob owner(String version, String intent, String terminal) {
        Map<String, String> state =
                new HashMap<>(
                        Map.of(
                                HaApplicationState.ENABLED,
                                "true",
                                HaApplicationState.INTENT,
                                intent,
                                HaApplicationState.JOB_ID,
                                "17",
                                HaApplicationState.SPEC_DIGEST,
                                HaApplicationState.digest("immutable-spec")));
        if (terminal != null) {
            state.put(HaApplicationState.STATUS, terminal);
        }
        return new KubernetesJob(
                new V1Job()
                        .metadata(
                                new V1ObjectMeta()
                                        .name("app")
                                        .uid("uid")
                                        .resourceVersion(version)
                                        .annotations(state)));
    }

    private static KubernetesApplicationConfig parameters() {
        return parameters(java.util.Collections.emptyMap());
    }

    private static KubernetesApplicationConfig parameters(Map<String, String> overrides) {
        Map<String, String> values =
                Map.of(
                        "kubernetes.image",
                        "test",
                        "kubernetes.ha.enabled",
                        "true",
                        "kubernetes.master.replicas",
                        "2",
                        "kubernetes.ha.backup-count",
                        "1",
                        "kubernetes.checkpoint-pvc",
                        "shared",
                        "application.job-id",
                        "17");
        values = new HashMap<>(values);
        values.putAll(overrides);
        return KubernetesApplicationConfig.from(
                SeatunnelApplicationConfig.parse("env {}", values),
                ReadonlyConfig.fromMap(new HashMap<>(values)));
    }
}
