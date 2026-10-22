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
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesHaClient;
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
    void retiredCoordinatorCannotRetryTerminalPublicationAfterNewActivation() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        AtomicBoolean current = new AtomicBoolean(true);
        doAnswer(
                        call -> {
                            current.set(false);
                            throw new KubernetesClientException(
                                    409, "new coordinator recorded RUNNING");
                        })
                .when(api)
                .updateHaState(any(), any());
        when(api.getJob("app")).thenReturn(owner("2", "requested", "RUNNING"));
        HaApplicationState.recordTerminal(
                api, owner("1", "requested", "RUNNING"), ApplicationStatus.FAILED, current::get);
        verify(api, times(1)).updateHaState(any(), any());
        verify(api, never()).deleteHaResources(any());
    }

    @Test
    void terminalReplayNeedsNoEngineEndpointAndCleanupFailureStaysRetryable() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
                                    api,
                                    "app",
                                    "uid",
                                    HaApplicationState.digest("immutable-spec"),
                                    17,
                                    config.getSpecification().getStartupTimeoutMillis()));
            SeatunnelKubernetesHaApplication.observe(
                    api,
                    "app",
                    "uid",
                    HaApplicationState.digest("immutable-spec"),
                    17,
                    config.getSpecification().getStartupTimeoutMillis());
            assertTrue(clients.constructed().isEmpty());
            verify(api, times(2)).deleteHaResources(any());
            verify(api, never()).updateHaState(any(), any());
            assertEquals(ApplicationStatus.SUCCEEDED, HaApplicationState.terminal(terminal));
        }
    }

    @Test
    void unknownOrTransportLossIsNotANativeTerminalResult() {
        assertNull(
                org.apache.seatunnel.engine.server.application.ApplicationJobRunner.terminalStatus(
                        JobStatus.UNKNOWABLE));
        assertNull(
                org.apache.seatunnel.engine.server.application.ApplicationJobRunner.terminalStatus(
                        null));
        assertNull(
                org.apache.seatunnel.engine.server.application.ApplicationJobRunner.terminalStatus(
                        JobStatus.RUNNING));
        assertEquals(
                ApplicationStatus.CANCELED,
                org.apache.seatunnel.engine.server.application.ApplicationJobRunner.terminalStatus(
                        JobStatus.CANCELED));
        assertEquals(
                ApplicationStatus.FAILED,
                org.apache.seatunnel.engine.server.application.ApplicationJobRunner.terminalStatus(
                        JobStatus.FAILED));
    }

    @Test
    void runningObserverDoesNotApplyStartupTimeout() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getJob("app"))
                .thenReturn(
                        owner("1", "requested", "RUNNING"), owner("2", "requested", "SUCCEEDED"));
        SeatunnelKubernetesHaApplication.observe(
                api, "app", "uid", HaApplicationState.digest("immutable-spec"), 17, 1);
        verify(api, never()).updateHaState(any(), any());
        verify(api).deleteHaResources(any());
    }

    @Test
    void startupFailureCannotOverwriteConcurrentNativeRunning() {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob stale = owner("1", "requested", null);
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

    @Test
    void neverRunningJobKeepsBoundedStartupFailure() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
        SeatunnelKubernetesHaApplication.observe(
                api, "app", "uid", HaApplicationState.digest("immutable-spec"), 17, 1);
        assertEquals(ApplicationStatus.FAILED, HaApplicationState.terminal(current.get()));
        verify(api).deleteHaResources(current.get());
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
