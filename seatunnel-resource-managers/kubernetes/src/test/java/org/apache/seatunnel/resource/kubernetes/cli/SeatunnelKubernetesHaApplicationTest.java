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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeatunnelKubernetesHaApplicationTest {

    @Test
    void overlappingOwnersHaveOnlyOneSubmissionWinner() {
        KubernetesClient api = mock(KubernetesClient.class);
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
        assertTrue(SeatunnelKubernetesHaApplication.claimSubmission(api, owner("1", "new", null)));
        assertFalse(SeatunnelKubernetesHaApplication.claimSubmission(api, owner("1", "new", null)));
        assertFalse(
                SeatunnelKubernetesHaApplication.claimSubmission(
                        api, owner("2", "requested", null)));
        verify(api, times(2)).updateHaState(any(), any());
    }

    @Test
    void initialWaitAttachesWhenAnotherOwnerClaimsWhileAStandbyIsUnavailable() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesJob initial = owner("1", "new", null);
        KubernetesJob claimed = owner("2", "requested", null);
        when(api.getJob("app")).thenReturn(initial, initial, claimed);
        org.apache.seatunnel.engine.client.job.JobClient jobs =
                mock(org.apache.seatunnel.engine.client.job.JobClient.class);
        org.apache.seatunnel.engine.client.job.ClientJobProxy proxy =
                mock(org.apache.seatunnel.engine.client.job.ClientJobProxy.class);
        when(jobs.getJobProxy(17L)).thenReturn(proxy);
        when(proxy.getJobStatus()).thenReturn(JobStatus.FINISHED);
        try (MockedConstruction<SeaTunnelClient> clients =
                mockConstruction(
                        SeaTunnelClient.class,
                        (client, context) -> {
                            when(client.getMasterCount()).thenReturn(1L);
                            when(client.getJobClient()).thenReturn(jobs);
                        })) {
            SeatunnelKubernetesHaApplication.observe(
                    api, "app", "uid", "immutable-spec", parameters());
            verify(clients.constructed().get(0), never())
                    .createExecutionContext(any(), any(), any(), any(), any());
            verify(api)
                    .updateHaState(
                            any(),
                            org.mockito.ArgumentMatchers.argThat(
                                    state ->
                                            "SUCCEEDED".equals(state.get(HaApplicationState.STATUS))
                                                    && "requested"
                                                            .equals(
                                                                    state.get(
                                                                            HaApplicationState
                                                                                    .INTENT))));
        }
    }

    @Test
    void terminalStateCannotClaimSubmissionEvenWhenIntentIsNew() {
        KubernetesClient api = mock(KubernetesClient.class);
        assertFalse(
                SeatunnelKubernetesHaApplication.claimSubmission(api, owner("2", "new", "FAILED")));
        verify(api, never()).updateHaState(any(), any());
    }

    @Test
    void staleOwnerCannotObserveReplacedApplicationOrChangedSpecification() {
        KubernetesJob owner = owner("1", "requested", null);
        assertSame(
                owner,
                SeatunnelKubernetesHaApplication.validated(owner, "uid", "immutable-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () ->
                        SeatunnelKubernetesHaApplication.validated(
                                owner, "replacement-uid", "immutable-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () -> SeatunnelKubernetesHaApplication.validated(owner, "uid", "changed-spec", 17));
        assertThrows(
                IllegalStateException.class,
                () ->
                        SeatunnelKubernetesHaApplication.validated(
                                owner, "uid", "immutable-spec", 18));
    }

    @Test
    void nativeTerminalRecordIsImmutableAcrossConcurrentOwnerCompletion() {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesJob old = owner("1", "requested", null);
        doThrow(new KubernetesClientException(409, "concurrent result"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(owner("2", "requested", "SUCCEEDED"));
        SeatunnelKubernetesHaApplication.recordTerminal(api, old, ApplicationStatus.FAILED);
        verify(api, times(1)).updateHaState(any(), any());
        KubernetesJob replacement = owner("2", "requested", null);
        replacement.getInternalResource().getMetadata().setUid("other-uid");
        when(api.getJob("app")).thenReturn(replacement);
        assertThrows(
                IllegalStateException.class,
                () ->
                        SeatunnelKubernetesHaApplication.recordTerminal(
                                api, old, ApplicationStatus.FAILED));
    }

    @Test
    void terminalReplayNeedsNoEngineEndpointAndCleanupFailureStaysRetryable() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesJob terminal = owner("3", "requested", "SUCCEEDED");
        when(api.getJob("app")).thenReturn(terminal);
        KubernetesApplicationConfig config = parameters();
        try (MockedConstruction<SeaTunnelClient> clients =
                mockConstruction(SeaTunnelClient.class)) {
            doThrow(new KubernetesClientException(500, "temporary cleanup failure"))
                    .doNothing()
                    .when(api)
                    .deleteHaResources(any());
            assertThrows(
                    KubernetesClientException.class,
                    () ->
                            SeatunnelKubernetesHaApplication.observe(
                                    api, "app", "uid", "immutable-spec", config));
            SeatunnelKubernetesHaApplication.observe(api, "app", "uid", "immutable-spec", config);
            assertTrue(clients.constructed().isEmpty());
            verify(api, times(2)).deleteHaResources(any());
            verify(api, never()).updateHaState(any(), any());
            assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(terminal));
        }
    }

    @Test
    void unknownOrTransportLossIsNotANativeTerminalResult() {
        assertNull(SeatunnelKubernetesHaApplication.terminal(JobStatus.UNKNOWABLE));
        assertNull(SeatunnelKubernetesHaApplication.terminal(null));
        assertNull(SeatunnelKubernetesHaApplication.terminal(JobStatus.RUNNING));
        assertEquals(
                ApplicationStatus.CANCELED,
                SeatunnelKubernetesHaApplication.terminal(JobStatus.CANCELED));
        assertEquals(
                ApplicationStatus.FAILED,
                SeatunnelKubernetesHaApplication.terminal(JobStatus.FAILED));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void startedJobSurvivesUnknownStatusPastStartupBudgetIncludingObserverRestart(boolean restarted)
            throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        java.util.concurrent.atomic.AtomicReference<KubernetesJob> current =
                new java.util.concurrent.atomic.AtomicReference<>(
                        owner("1", "requested", restarted ? "RUNNING" : null));
        when(api.getJob("app")).thenAnswer(call -> current.get());
        doAnswer(
                        call -> {
                            KubernetesJob updated = owner("2", "requested", null);
                            updated.getInternalResource()
                                    .getMetadata()
                                    .setAnnotations(new HashMap<>(call.getArgument(1)));
                            current.set(updated);
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        org.apache.seatunnel.engine.client.job.JobClient jobs =
                mock(org.apache.seatunnel.engine.client.job.JobClient.class);
        org.apache.seatunnel.engine.client.job.ClientJobProxy proxy =
                mock(org.apache.seatunnel.engine.client.job.ClientJobProxy.class);
        when(jobs.getJobProxy(17L)).thenReturn(proxy);
        java.util.concurrent.atomic.AtomicInteger polls =
                new java.util.concurrent.atomic.AtomicInteger();
        when(proxy.getJobStatus())
                .thenAnswer(
                        call -> {
                            int poll = polls.getAndIncrement();
                            if (poll == 0 && !restarted) {
                                return JobStatus.RUNNING;
                            }
                            if (poll < 2) {
                                return JobStatus.UNKNOWABLE;
                            }
                            if (poll == 2) {
                                throw new IllegalStateException("fixture transport unavailable");
                            }
                            return JobStatus.FINISHED;
                        });
        try (MockedConstruction<SeaTunnelClient> clients =
                mockConstruction(
                        SeaTunnelClient.class,
                        (client, context) -> when(client.getJobClient()).thenReturn(jobs))) {
            SeatunnelKubernetesHaApplication.observe(
                    api,
                    "app",
                    "uid",
                    "immutable-spec",
                    parameters(Map.of("application.startup-timeout-millis", "1")));
            assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(current.get()));
            assertEquals(4, polls.get());
            verify(api, never())
                    .updateHaState(
                            any(),
                            org.mockito.ArgumentMatchers.argThat(
                                    state ->
                                            "FAILED".equals(state.get(HaApplicationState.STATUS))));
            verify(api).deleteHaResources(current.get());
            verify(clients.constructed().get(0), never())
                    .createExecutionContext(any(), any(), any(), any(), any());
        }
    }

    @Test
    void startupFailureCannotOverwriteConcurrentNativeRunning() {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesJob stale = owner("1", "requested", null);
        KubernetesJob running = owner("2", "requested", "RUNNING");
        doThrow(new KubernetesClientException(409, "concurrent native RUNNING"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(running);
        assertFalse(SeatunnelKubernetesHaApplication.recordStartupFailure(api, stale));
        verify(api, times(1)).updateHaState(any(), any());
        verify(api, never()).deleteHaResources(any());
    }

    @Test
    void nativeRunningCannotOverwriteConcurrentTerminal() {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesJob stale = owner("1", "requested", null);
        KubernetesJob completed = owner("2", "requested", "SUCCEEDED");
        doThrow(new KubernetesClientException(409, "concurrent native completion"))
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(completed);
        SeatunnelKubernetesHaApplication.recordRunning(api, stale);
        verify(api, times(1)).updateHaState(any(), any());
        assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(completed));
    }

    @Test
    void failedRunningPersistenceDoesNotRecordTerminalOrReleaseCompute() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob("app")).thenReturn(owner("1", "requested", null));
        doThrow(new KubernetesClientException(503, "fixture API unavailable"))
                .when(api)
                .updateHaState(any(), any());
        org.apache.seatunnel.engine.client.job.ClientJobProxy proxy =
                mock(org.apache.seatunnel.engine.client.job.ClientJobProxy.class);
        when(proxy.getJobStatus()).thenReturn(JobStatus.RUNNING);
        assertThrows(KubernetesClientException.class, () -> observeAttached(api, proxy));
        verify(api, never()).deleteHaResources(any());
        verify(api)
                .updateHaState(
                        any(),
                        org.mockito.ArgumentMatchers.argThat(
                                state -> "RUNNING".equals(state.get(HaApplicationState.STATUS))));
    }

    @Test
    void neverRunningJobKeepsBoundedStartupFailure() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        java.util.concurrent.atomic.AtomicReference<KubernetesJob> current =
                new java.util.concurrent.atomic.AtomicReference<>(owner("1", "requested", null));
        when(api.getJob("app")).thenAnswer(call -> current.get());
        doAnswer(
                        call -> {
                            current.set(owner("2", "requested", "FAILED"));
                            return null;
                        })
                .when(api)
                .updateHaState(any(), any());
        org.apache.seatunnel.engine.client.job.ClientJobProxy proxy =
                mock(org.apache.seatunnel.engine.client.job.ClientJobProxy.class);
        when(proxy.getJobStatus()).thenReturn(JobStatus.UNKNOWABLE);
        observeAttached(api, proxy);
        assertEquals(ApplicationStatus.FAILED, HaApplicationState.terminal(current.get()));
        verify(api).deleteHaResources(current.get());
    }

    private static void observeAttached(
            KubernetesClient api, org.apache.seatunnel.engine.client.job.ClientJobProxy proxy)
            throws Exception {
        org.apache.seatunnel.engine.client.job.JobClient jobs =
                mock(org.apache.seatunnel.engine.client.job.JobClient.class);
        when(jobs.getJobProxy(17L)).thenReturn(proxy);
        try (MockedConstruction<SeaTunnelClient> clients =
                mockConstruction(
                        SeaTunnelClient.class,
                        (client, context) -> when(client.getJobClient()).thenReturn(jobs))) {
            SeatunnelKubernetesHaApplication.observe(
                    api,
                    "app",
                    "uid",
                    "immutable-spec",
                    parameters(Map.of("application.startup-timeout-millis", "1")));
            verify(clients.constructed().get(0), never())
                    .createExecutionContext(any(), any(), any(), any(), any());
        }
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
