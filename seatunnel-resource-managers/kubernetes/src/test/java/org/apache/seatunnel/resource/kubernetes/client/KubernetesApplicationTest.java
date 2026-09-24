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

package org.apache.seatunnel.resource.kubernetes.client;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.client.ApplicationClient;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KubernetesApplicationTest {
    @Test
    void deploysDependenciesBeforeStartingOwner() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any()))
                .thenAnswer(
                        invocation -> {
                            KubernetesJob job = invocation.getArgument(0);
                            job.getInternalResource().getMetadata().setUid("server-uid");
                            return job;
                        });
        when(api.getJob(anyString())).thenReturn(job(new V1JobStatus().active(1)));
        when(api.listPods(anyString()))
                .thenReturn(Collections.singletonList(pod("master", "Running")));
        ApplicationClient client = new KubernetesApplicationDeployer(api).deploy(specification());
        assertEquals(DeployType.KUBERNETES, client.getApplicationId().getDeployType());
        InOrder order = inOrder(api);
        order.verify(api).getConfigMap("seatunnel-runtime");
        order.verify(api).createJob(any());
        order.verify(api).createSecret(any());
        order.verify(api).createService(any());
        order.verify(api).startJob(client.getApplicationId().getId());
        client.close();
        verify(api, never()).deleteApplication(anyString());
        verify(api, never()).close();
    }

    @Test
    void rollsBackPartiallyCreatedAndAmbiguouslyCreatedApplications() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any())).thenReturn(job());
        doThrow(new ApiException(403, "denied")).when(api).createService(any());
        assertThrows(
                ApiException.class,
                () -> new KubernetesApplicationDeployer(api).deploy(specification()));
        verify(api).deleteApplication(anyString());
        KubernetesClient interrupted = mock(KubernetesClient.class);
        when(interrupted.createJob(any())).thenThrow(new ApiException(0, "connection interrupted"));
        assertThrows(
                ApiException.class,
                () -> new KubernetesApplicationDeployer(interrupted).deploy(specification()));
        verify(interrupted).deleteApplication(anyString());
    }

    @Test
    void masterSchedulingTimeoutRollsBackAllApplicationResources() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any())).thenReturn(job());
        when(api.getJob(anyString())).thenReturn(job(new V1JobStatus().active(1)));
        when(api.listPods(anyString()))
                .thenReturn(Collections.singletonList(pod("master", "Pending")));
        Map<String, String> options = new HashMap<>(specification().getOptions());
        options.put(ApplicationOptions.STARTUP_TIMEOUT_MILLIS.key(), "5");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, "env {}", options);
        assertThrows(
                TimeoutException.class,
                () -> new KubernetesApplicationDeployer(api).deploy(specification));
        verify(api).deleteApplication(anyString());
    }

    @Test
    void mapsTerminalJobConditionsAndCancellation() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesApplicationClient client =
                new KubernetesApplicationClient(
                        api, new ApplicationId(DeployType.KUBERNETES, "app"));
        when(api.getJob("app"))
                .thenReturn(
                        job(
                                new V1JobStatus()
                                        .addConditionsItem(
                                                new V1JobCondition()
                                                        .type("Complete")
                                                        .status("True"))));
        assertEquals(ApplicationStatus.SUCCEEDED, client.getStatus());
        when(api.getJob("app"))
                .thenReturn(
                        job(
                                new V1JobStatus()
                                        .addConditionsItem(
                                                new V1JobCondition()
                                                        .type("Failed")
                                                        .status("True")
                                                        .reason("BackoffLimitExceeded"))));
        assertEquals(ApplicationStatus.FAILED, client.getStatus());
        assertEquals("BackoffLimitExceeded", client.getResult().getDiagnostics());
        when(api.getJob("app")).thenThrow(new ApiException(404, "gone"));
        assertEquals(ApplicationStatus.UNKNOWN, client.getStatus());
        client.cancel();
        assertEquals(ApplicationStatus.CANCELED, client.getStatus());
        verify(api).deleteApplication("app");
    }

    @Test
    void rejectsMissingImageBeforeCreatingResources() throws Exception {
        Map<String, String> options = new HashMap<>();
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, "env {}", options);
        KubernetesClient api = mock(KubernetesClient.class);
        assertThrows(
                IllegalArgumentException.class,
                () -> new KubernetesApplicationDeployer(api).deploy(specification));
        verify(api, never()).createJob(any());
        for (String name :
                Arrays.asList(
                        "Mixed_Name",
                        "---",
                        "an-application-with-a-name-that-is-longer-than-the-kubernetes-resource-name-limit")) {
            String id = KubernetesResourceFactory.newId(name);
            assertTrue(id.matches("[a-z0-9]([a-z0-9-]*[a-z0-9])?"));
            assertTrue(id.length() < 50);
        }
    }

    private static ApplicationSpecification specification() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.CONFIG_MAP.key(), "seatunnel-runtime");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return ApplicationSpecification.fromOptions(
                DeployType.KUBERNETES, "env { job.mode = BATCH }", options);
    }

    private static KubernetesJob job() {
        return job(null);
    }

    private static KubernetesJob job(V1JobStatus status) {
        KubernetesJob job =
                KubernetesResourceFactory.job(
                        "app", KubernetesApplicationParameters.from(specification()));
        job.getInternalResource().getMetadata().setUid("uid-1");
        job.getInternalResource().setStatus(status);
        return job;
    }

    private static KubernetesPod pod(String name, String phase) {
        return new KubernetesPod(
                new V1Pod()
                        .metadata(new V1ObjectMeta().name(name))
                        .status(new V1PodStatus().phase(phase)));
    }
}
