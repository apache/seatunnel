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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.deployment.SeatunnelClientProvider;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.core.classloader.JarPathResolver;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesMasterCli;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesWorkerCli;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.hazelcast.client.config.ClientConfig;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KubernetesApplicationTest {
    private MockedConstruction<SeaTunnelClient> nativeClients;
    private final List<ClientConfig> clientConfigs = new ArrayList<>();

    @BeforeEach
    void mockNativeConnections() {
        nativeClients =
                mockConstruction(
                        SeaTunnelClient.class,
                        (client, context) ->
                                clientConfigs.add((ClientConfig) context.arguments().get(0)));
    }

    @AfterEach
    void closeNativeConnections() {
        nativeClients.close();
    }

    @Test
    void startsWorkerWithoutCreatingKubernetesClient() throws Exception {
        SeaTunnelConfig config = new SeaTunnelConfig();
        try (MockedStatic<ConfigProvider> configurations = mockStatic(ConfigProvider.class);
                MockedStatic<SeaTunnelServerStarter> starter =
                        mockStatic(SeaTunnelServerStarter.class);
                MockedStatic<KubernetesClientFactory> clients =
                        mockStatic(KubernetesClientFactory.class)) {
            configurations.when(ConfigProvider::locateAndGetSeaTunnelConfig).thenReturn(config);
            SeatunnelKubernetesWorkerCli.main(new String[] {"kubernetes-app", "master:5801", "2"});
            ArgumentCaptor<JarPathResolver> resolver =
                    ArgumentCaptor.forClass(JarPathResolver.class);
            starter.verify(
                    () ->
                            SeaTunnelServerStarter.createHazelcastInstance(
                                    eq(config),
                                    isNull(),
                                    resolver.capture(),
                                    any(ResourceManagerFactory.class)));
            starter.verifyNoMoreInteractions();
            clients.verifyNoInteractions();
            assertEquals("kubernetes-app", config.getHazelcastConfig().getClusterName());
            assertEquals(
                    EngineConfig.ClusterRole.WORKER, config.getEngineConfig().getClusterRole());
            assertTrue(config.getHazelcastConfig().isLiteMember());
            assertEquals(
                    "master:5801",
                    config.getHazelcastConfig()
                            .getNetworkConfig()
                            .getJoin()
                            .getTcpIpConfig()
                            .getRequiredMember());
            assertTrue(config.getHazelcastConfig().getNetworkConfig().isPortAutoIncrement());
            assertEquals(2, config.getEngineConfig().getSlotServiceConfig().getSlotNum());
            assertFalse(config.getEngineConfig().getSlotServiceConfig().isDynamicSlot());
            assertEquals(
                    "true",
                    config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.enabled"));
            assertEquals(
                    "GRACEFUL",
                    config.getHazelcastConfig().getProperty("hazelcast.shutdownhook.policy"));
            List<URL> jars = Collections.emptyList();
            assertSame(jars, resolver.getValue().resolve(jars));
        }
    }

    @Test
    void rejectsInvalidEntrypointArgumentsBeforeStartingResources() throws Exception {
        try (MockedStatic<SeaTunnelServerStarter> starter =
                mockStatic(SeaTunnelServerStarter.class)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> SeatunnelKubernetesWorkerCli.main(new String[] {"kubernetes-app"}));
            assertEquals(
                    1,
                    catchSystemExit(
                            () ->
                                    SeatunnelKubernetesMasterCli.main(
                                            new String[] {"kubernetes-app", "master:5801", "2"})));
            starter.verifyNoInteractions();
        }
    }

    @Test
    void retrievesLazyProviderWithIndependentlyOwnedClients() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob("existing-job")).thenReturn(job(new V1JobStatus().active(1)));
        when(api.listPods(anyString()))
                .thenReturn(Collections.singletonList(pod("master", "Running")));
        SeatunnelClientProvider provider;
        SeaTunnelClient first;
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            provider = descriptor.retrieve("existing-job");
            assertTrue(nativeClients.constructed().isEmpty());
            first = provider.getClusterClient();
        }
        verify(first, never()).close();
        try (SeaTunnelClient client = first;
                SeaTunnelClient second = provider.getClusterClient()) {
            assertSame(client, nativeClients.constructed().get(0));
            assertSame(second, nativeClients.constructed().get(1));
            assertNotSame(client, second);
            assertEquals(
                    "seatunnel-application-existing-job", clientConfigs.get(0).getClusterName());
            assertEquals(
                    Collections.singletonList("10.0.0.1:5801"),
                    clientConfigs.get(0).getNetworkConfig().getAddresses());
        }
        for (SeaTunnelClient client : nativeClients.constructed()) {
            verify(client).close();
        }
        verify(api, never()).createJob(any());
        verify(api, never()).createSecret(any());
        verify(api, never()).createService(any());
        verify(api, never()).startJob(anyString());
        verify(api, never()).deleteApplication(anyString());
        verify(api).close();
    }

    @Test
    void rejectsEmptyJobNameWithoutPlatformRequests() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            assertThrows(IllegalArgumentException.class, () -> descriptor.retrieve(" "));
        }
        verify(api, never()).getJob(anyString());
    }

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
        KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options());
        String applicationId = descriptor.deployApplication(specification());
        assertTrue(nativeClients.constructed().isEmpty());
        InOrder order = inOrder(api);
        order.verify(api).getConfigMap("seatunnel-runtime");
        order.verify(api).createJob(any());
        order.verify(api).createSecret(any());
        order.verify(api).createService(any());
        order.verify(api).startJob(applicationId);
        descriptor.close();
        verify(api, never()).deleteApplication(anyString());
        verify(api).close();
    }

    @Test
    void rollsBackPartiallyCreatedAndAmbiguouslyCreatedApplications() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any())).thenReturn(job());
        doThrow(new ApiException(403, "denied")).when(api).createService(any());
        assertThrows(
                ApiException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification()));
        verify(api).deleteApplication(anyString());
        KubernetesClient interrupted = mock(KubernetesClient.class);
        when(interrupted.createJob(any())).thenThrow(new ApiException(0, "connection interrupted"));
        assertThrows(
                ApiException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(interrupted, options())
                                .deployApplication(specification()));
        verify(interrupted).deleteApplication(anyString());
    }

    @Test
    void masterSchedulingTimeoutRollsBackAllApplicationResources() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any())).thenReturn(job());
        when(api.getJob(anyString())).thenReturn(job(new V1JobStatus().active(1)));
        when(api.listPods(anyString()))
                .thenReturn(Collections.singletonList(pod("master", "Pending")));
        Map<String, String> options = options();
        options.put(ApplicationOptions.STARTUP_TIMEOUT_MILLIS.key(), "5");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        assertThrows(
                TimeoutException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification));
        verify(api).deleteApplication(anyString());
    }

    @Test
    void mapsTerminalJobConditionsAndCancellation() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        KubernetesApplicationClient client = new KubernetesApplicationClient(api, "app");
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
        assertEquals("BackoffLimitExceeded", api.getJob("app").getFailureReason());
        when(api.getJob("app")).thenThrow(new ApiException(404, "gone"));
        assertEquals(ApplicationStatus.UNKNOWN, client.getStatus());
        client.cancel();
        assertEquals(ApplicationStatus.CANCELED, client.getStatus());
        verify(api).deleteApplication("app");
    }

    @Test
    void applicationOperationsWorkAfterMasterExit() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob("finished"))
                .thenReturn(
                        job(
                                new V1JobStatus()
                                        .addConditionsItem(
                                                new V1JobCondition()
                                                        .type("Complete")
                                                        .status("True"))));
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            assertEquals(ApplicationStatus.SUCCEEDED, descriptor.getApplicationStatus("finished"));
            assertThrows(IllegalStateException.class, () -> descriptor.retrieve("finished"));
            descriptor.cancelApplication("finished");
            verify(api).deleteApplication("finished");
            when(api.getJob("finished")).thenThrow(new ApiException(404, "gone"));
            assertEquals(ApplicationStatus.UNKNOWN, descriptor.getApplicationStatus("finished"));
            assertTrue(nativeClients.constructed().isEmpty());
            verify(api, never()).listPods(anyString());
        }
    }

    @Test
    void deploymentReturnsIdWhenApplicationFinishesBeforeConnecting() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.createJob(any())).thenReturn(job());
        when(api.getJob(anyString()))
                .thenReturn(
                        job(
                                new V1JobStatus()
                                        .addConditionsItem(
                                                new V1JobCondition()
                                                        .type("Complete")
                                                        .status("True"))));
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            String id = descriptor.deployApplication(specification());
            verify(api).startJob(id);
            assertTrue(nativeClients.constructed().isEmpty());
        }
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void rejectsMissingImageBeforeCreatingResources() throws Exception {
        Map<String, String> options = new HashMap<>();
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        KubernetesClient api = mock(KubernetesClient.class);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification));
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
        return SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options());
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.CONFIG_MAP.key(), "seatunnel-runtime");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return options;
    }

    private static KubernetesJob job() {
        return job(null);
    }

    private static KubernetesJob job(V1JobStatus status) {
        KubernetesJob job =
                KubernetesResourceFactory.job(
                        "app",
                        SeatunnelKubernetesMasterCli.class.getName(),
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))));
        job.getInternalResource().getMetadata().setUid("uid-1");
        job.getInternalResource().setStatus(status);
        return job;
    }

    private static KubernetesPod pod(String name, String phase) {
        return new KubernetesPod(
                new V1Pod()
                        .metadata(new V1ObjectMeta().name(name))
                        .status(new V1PodStatus().phase(phase).podIP("10.0.0.1")));
    }
}
