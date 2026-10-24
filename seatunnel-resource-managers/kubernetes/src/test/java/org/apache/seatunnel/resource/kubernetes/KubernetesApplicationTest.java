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
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.HaApplicationState;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClientFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesHaClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.worker.SeatunnelKubernetesApplicationWorker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1Secret;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            HazelcastInstanceImpl worker = mock(HazelcastInstanceImpl.class);
            Cluster cluster = mock(Cluster.class);
            when(worker.getCluster()).thenReturn(cluster);
            starter.when(
                            () ->
                                    SeaTunnelServerStarter.createHazelcastInstance(
                                            any(SeaTunnelConfig.class),
                                            isNull(),
                                            any(ResourceManagerFactory.class)))
                    .thenReturn(worker);
            SeatunnelKubernetesApplicationWorker.main(
                    new String[] {"kubernetes-app", "master:5801", "2"});
            starter.verify(
                    () ->
                            SeaTunnelServerStarter.createHazelcastInstance(
                                    eq(config), isNull(), any(ResourceManagerFactory.class)));
            starter.verifyNoMoreInteractions();
            clients.verifyNoInteractions();
            verify(cluster).addMembershipListener(any(MembershipListener.class));
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
        }
    }

    @Test
    void workerExitsWhenMasterMemberLeaves() throws Exception {
        SeaTunnelConfig config = new SeaTunnelConfig();
        try (MockedStatic<ConfigProvider> configurations = mockStatic(ConfigProvider.class);
                MockedStatic<SeaTunnelServerStarter> starter =
                        mockStatic(SeaTunnelServerStarter.class);
                MockedStatic<KubernetesClientFactory> clients =
                        mockStatic(KubernetesClientFactory.class)) {
            configurations.when(ConfigProvider::locateAndGetSeaTunnelConfig).thenReturn(config);
            HazelcastInstanceImpl worker = mock(HazelcastInstanceImpl.class);
            Cluster cluster = mock(Cluster.class);
            when(worker.getCluster()).thenReturn(cluster);
            starter.when(
                            () ->
                                    SeaTunnelServerStarter.createHazelcastInstance(
                                            any(SeaTunnelConfig.class),
                                            isNull(),
                                            any(ResourceManagerFactory.class)))
                    .thenReturn(worker);
            SeatunnelKubernetesApplicationWorker.main(
                    new String[] {"kubernetes-app", "master:5801", "2"});

            ArgumentCaptor<MembershipListener> listener =
                    ArgumentCaptor.forClass(MembershipListener.class);
            verify(cluster).addMembershipListener(listener.capture());
            MembershipEvent event = mock(MembershipEvent.class);
            Member member = mock(Member.class);
            when(event.getMember()).thenReturn(member);
            when(member.isLiteMember()).thenReturn(false);
            assertEquals(1, catchSystemExit(() -> listener.getValue().memberRemoved(event)));
        }
    }

    @Test
    void rejectsInvalidEntrypointArgumentsBeforeStartingResources() throws Exception {
        try (MockedStatic<SeaTunnelServerStarter> starter =
                mockStatic(SeaTunnelServerStarter.class)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            SeatunnelKubernetesApplicationWorker.main(
                                    new String[] {"kubernetes-app"}));
            assertEquals(
                    1,
                    catchSystemExit(
                            () ->
                                    SeatunnelKubernetesApplicationCli.main(
                                            new String[] {"kubernetes-app", "master:5801", "2"})));
            starter.verifyNoInteractions();
        }
    }

    @Test
    void retrievesLazyProviderWithIndependentlyOwnedClients() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
        verify(api, never()).createAccompanyingResources(any(), any());
        verify(api, never()).startJob(anyString());
        verify(api, never()).deleteApplication(anyString());
        verify(api).close();
    }

    @Test
    void rejectsEmptyJobNameWithoutPlatformRequests() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            assertThrows(IllegalArgumentException.class, () -> descriptor.retrieve(" "));
        }
        verify(api, never()).getJob(anyString());
    }

    @Test
    void continuesWhenUserManagedConfigMapsCannotBeRead() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getConfigMap("platform-runtime"))
                .thenThrow(new KubernetesClientException(403, "forbidden"));
        when(api.createJob(any())).thenReturn(job());
        Map<String, String> options = options();
        options.put("kubernetes.configmap.shared.ref", "platform-runtime");
        options.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/shared.yaml");
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options)) {
            String applicationId = descriptor.deployApplication(specification());
            verify(api).startJob(applicationId);
        }
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void failsBeforeCreatingResourcesWhenReferencedConfigMapIsMissing() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getConfigMap("platform-runtime"))
                .thenThrow(new KubernetesClientException(404, "not found"));
        Map<String, String> options = options();
        options.put("kubernetes.configmap.shared.ref", "platform-runtime");
        options.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/shared.yaml");
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).createJob(any());
    }

    @Test
    void rejectsMissingReferencedConfigMapKeysBeforeCreatingResources() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getConfigMap("platform-runtime"))
                .thenReturn(
                        new KubernetesConfigMap(
                                new V1ConfigMap()
                                        .metadata(new V1ObjectMeta().name("platform-runtime"))
                                        .data(Collections.singletonMap("other.yaml", "value"))));
        Map<String, String> options = options();
        options.put("kubernetes.configmap.shared.ref", "platform-runtime");
        options.put(
                "kubernetes.configmap.shared.mount.seatunnel.yaml",
                "/opt/seatunnel/config/shared.yaml");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).createJob(any());
    }

    @Test
    void failsBeforeCreatingResourcesWhenReferencedSecretIsMissing() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getSecret("platform-secrets"))
                .thenThrow(new KubernetesClientException(404, "not found"));
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secrets");
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).createJob(any());
    }

    @Test
    void failsBeforeCreatingResourcesWhenEnvFromSecretIsMissing() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getSecret("platform-secrets"))
                .thenThrow(new KubernetesClientException(404, "not found"));
        Map<String, String> options = options();
        options.put("kubernetes.env-from.secret", "platform-secrets");
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).createJob(any());
    }

    @Test
    void rejectsMissingEnvironmentSecretKeysBeforeCreatingResources() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getSecret("platform-secrets"))
                .thenReturn(
                        new KubernetesSecret(
                                new V1Secret()
                                        .metadata(new V1ObjectMeta().name("platform-secrets"))
                                        .data(Collections.singletonMap("other", new byte[] {1}))));
        Map<String, String> options = options();
        options.put("kubernetes.env.MYSQL_PASSWORD", "secret:platform-secrets/password");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).createJob(any());
    }

    @Test
    void acceptsReferencesToTheSecretThisSubmissionCreates() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        // The application-owned Secret does not exist before this submission creates it.
        when(api.getSecret("application-secrets"))
                .thenThrow(new KubernetesClientException(404, "not found"));
        when(api.createJob(any())).thenReturn(job());
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_NAME.key(), "application-secrets");
        options.put("kubernetes.secret.data.MYSQL_PASSWORD", "secret-value");
        options.put("kubernetes.env.MYSQL_PASSWORD", "secret:application-secrets/MYSQL_PASSWORD");
        options.put("kubernetes.env-from.secret", "application-secrets");
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options)) {
            String applicationId = descriptor.deployApplication(specification());
            verify(api).startJob(applicationId);
        }
        verify(api, never()).getSecret(anyString());
    }

    @Test
    void rejectsMissingKeysInTheSecretThisSubmissionCreates() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_NAME.key(), "application-secrets");
        options.put("kubernetes.secret.data.MYSQL_PASSWORD", "secret-value");
        options.put("kubernetes.env.MYSQL_PASSWORD", "secret:application-secrets/OTHER_PASSWORD");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).getSecret(anyString());
        verify(api, never()).createJob(any());
    }

    @Test
    void acceptsReferencesToTheConfigMapThisSubmissionCreates() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        // The application-owned ConfigMap does not exist before this submission creates it.
        when(api.getConfigMap("application-config"))
                .thenThrow(new KubernetesClientException(404, "not found"));
        when(api.createJob(any())).thenReturn(job());
        Map<String, String> options = options();
        options.put("kubernetes.configmap.generated.name", "application-config");
        options.put(
                "kubernetes.configmap.generated.data.application-e2e.properties",
                "source=kubernetes-configmap");
        options.put(
                "kubernetes.configmap.generated.mount.application-e2e.properties",
                "/opt/seatunnel/config/application-e2e.properties");
        options.put(
                "kubernetes.env.APPLICATION_FILE",
                "configmap:application-config/application-e2e.properties");
        options.put("kubernetes.env-from.configmap", "application-config");
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options)) {
            String applicationId = descriptor.deployApplication(specification());
            verify(api).startJob(applicationId);
        }
        verify(api, never()).getConfigMap("application-config");
    }

    @Test
    void rejectsMissingKeysInTheConfigMapThisSubmissionCreates() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        Map<String, String> options = options();
        options.put("kubernetes.configmap.generated.name", "application-config");
        options.put("kubernetes.configmap.generated.data.application-e2e.properties", "value");
        options.put(
                "kubernetes.configmap.generated.mount.application-e2e.properties",
                "/opt/seatunnel/config/application-e2e.properties");
        options.put(
                "kubernetes.env.APPLICATION_FILE", "configmap:application-config/other.properties");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options)
                                .deployApplication(specification()));
        verify(api, never()).getConfigMap("application-config");
        verify(api, never()).createJob(any());
    }

    @Test
    void continuesWhenReferencedSecretsCannotBeRead() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getSecret("platform-secrets"))
                .thenThrow(new KubernetesClientException(403, "forbidden"));
        when(api.createJob(any())).thenReturn(job());
        Map<String, String> options = options();
        options.put(KubernetesOptions.SECRET_REF.key(), "platform-secrets");
        options.put("kubernetes.env.MYSQL_PASSWORD", "secret:platform-secrets/password");
        options.put("kubernetes.env-from.secret", "platform-secrets");
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options)) {
            String applicationId = descriptor.deployApplication(specification());
            verify(api).startJob(applicationId);
        }
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void deploysDependenciesBeforeStartingOwner() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any()))
                .thenAnswer(
                        invocation -> {
                            KubernetesJob job = invocation.getArgument(0);
                            job.getInternalResource().getMetadata().setUid("server-uid");
                            return job;
                        });
        KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options());
        String applicationId = descriptor.deployApplication(specification());
        assertTrue(nativeClients.constructed().isEmpty());
        InOrder order = inOrder(api);
        order.verify(api).getConfigMap("seatunnel-runtime");
        order.verify(api).createJob(any());
        order.verify(api).createAccompanyingResources(any(), any());
        order.verify(api).startJob(applicationId);
        descriptor.close();
        verify(api, never()).getJob(anyString());
        verify(api, never()).listPods(anyString());
        verify(api, never()).deleteApplication(anyString());
        verify(api).close();
    }

    @Test
    void submissionReturnsWithoutWaitingForTheMaster() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any())).thenReturn(job());
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, options())) {
            String applicationId = descriptor.deployApplication(specification());
            verify(api).startJob(applicationId);
            verify(api, never()).getJob(anyString());
            verify(api, never()).listPods(anyString());
            assertTrue(nativeClients.constructed().isEmpty());
        }
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void compensatesCreatedResourcesWhenAccompanyingResourcesFail() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any())).thenReturn(job());
        doThrow(new KubernetesClientException(403, "denied"))
                .when(api)
                .createAccompanyingResources(any(), any());
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification()));
        verify(api).deleteApplication(anyString());
    }

    @Test
    void compensatesAmbiguousJobCreationRequests() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any()))
                .thenThrow(new KubernetesClientException(0, "connection interrupted"));
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification()));
        verify(api, never()).createAccompanyingResources(any(), any());
        verify(api).deleteApplication(anyString());
    }

    @Test
    void compensatesAmbiguousAccompanyingResourceRequests() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any())).thenReturn(job());
        doThrow(new KubernetesClientException(0, "connection interrupted"))
                .when(api)
                .createAccompanyingResources(any(), any());
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification()));
        verify(api).deleteApplication(anyString());
    }

    @Test
    void doesNotCompensateResourcesThisSubmissionDidNotCreate() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.createJob(any())).thenThrow(new KubernetesClientException(409, "already exists"));
        assertThrows(
                KubernetesClientException.class,
                () ->
                        new KubernetesApplicationClusterDescriptor(api, options())
                                .deployApplication(specification()));
        verify(api, never()).createAccompanyingResources(any(), any());
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void mapsTerminalJobConditionsAndCancellation() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
        when(api.getJob("app")).thenThrow(new KubernetesClientException(404, "gone"));
        assertEquals(ApplicationStatus.UNKNOWN, client.getStatus());
        client.cancel();
        assertEquals(ApplicationStatus.CANCELED, client.getStatus());
        verify(api, never()).deleteOrphanPods("app");
        verify(api).deleteApplication("app");
        verify(api, never()).deleteApplication(anyString(), anyString());
    }

    @Test
    void observedHaMissingOwnerStillUsesUidScopedOrphanCleanup() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob owner = job(new V1JobStatus());
        owner.getInternalResource()
                .getMetadata()
                .setAnnotations(
                        Map.of(
                                HaApplicationState.ENABLED, "true",
                                HaApplicationState.STATUS, "RUNNING"));
        KubernetesApplicationClient handle = new KubernetesApplicationClient(api, "app");
        when(api.getJob("app")).thenReturn(owner);
        assertEquals(ApplicationStatus.RUNNING, handle.getStatus());
        when(api.getJob("app")).thenThrow(new KubernetesClientException(404, "gone"));
        handle.cancel();
        verify(api).deleteOrphanPods("app");
        verify(api, never()).deleteApplication(anyString());
    }

    @Test
    void applicationOperationsWorkAfterMasterExit() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
            when(api.getJob("finished")).thenThrow(new KubernetesClientException(404, "gone"));
            assertEquals(ApplicationStatus.UNKNOWN, descriptor.getApplicationStatus("finished"));
            assertTrue(nativeClients.constructed().isEmpty());
            verify(api, never()).listPods(anyString());
        }
    }

    @Test
    void rejectsMissingImageBeforeCreatingResources() throws Exception {
        Map<String, String> options = new HashMap<>();
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("env {}", options);
        KubernetesHaClient api = mock(KubernetesHaClient.class);
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
                        "",
                        "???",
                        "an-application-with-a-name-that-is-longer-than-the-kubernetes-resource-name-limit")) {
            String id = KubernetesResourceIdentity.newId(name);
            assertTrue(id.matches("[a-z0-9]([a-z0-9-]*[a-z0-9])?"));
            assertTrue(id.length() < 50);
        }
        // Names without any usable characters still produce a stable, usable prefix.
        assertTrue(KubernetesResourceIdentity.newId("").startsWith("seatunnel-"));
        assertTrue(KubernetesResourceIdentity.newId("???").startsWith("seatunnel-"));
    }

    private static ApplicationSpecification specification() {
        return SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options());
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put("kubernetes.configmap.runtime.ref", "seatunnel-runtime");
        options.put(
                "kubernetes.configmap.runtime.mount.seatunnel.yaml",
                "/opt/seatunnel/config/seatunnel.yaml");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return options;
    }

    private static KubernetesJob job() {
        return job(null);
    }

    private static KubernetesJob job(V1JobStatus status) {
        KubernetesJob job =
                (KubernetesJob)
                        KubernetesApplicationClusterDescriptor.assembleApplication(
                                        "app",
                                        SeatunnelKubernetesApplicationCli.class.getName(),
                                        KubernetesApplicationConfig.from(
                                                specification(),
                                                ReadonlyConfig.fromMap(new HashMap<>(options()))))
                                .get(0);
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

    @Test
    void haPrefersDurableNativeOutcomeAndDeletesByOwnerUid() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob owner =
                job(
                        new V1JobStatus()
                                .addConditionsItem(
                                        new V1JobCondition().type("Failed").status("True")));
        owner.getInternalResource().getMetadata().setUid("owner-uid");
        owner.getInternalResource()
                .getMetadata()
                .setAnnotations(
                        new HashMap<>(
                                Map.of(
                                        HaApplicationState.ENABLED, "true",
                                        HaApplicationState.STATUS, "CANCELED")));
        when(api.getJob("app")).thenReturn(owner);
        KubernetesApplicationClient handle = new KubernetesApplicationClient(api, "app");
        assertEquals(ApplicationStatus.CANCELED, handle.getStatus());
        owner.getInternalResource()
                .getMetadata()
                .getAnnotations()
                .put(HaApplicationState.STATUS, "SUCCEEDED");
        assertEquals(
                ApplicationStatus.SUCCEEDED,
                new KubernetesApplicationClient(api, "app").getStatus());
        handle.cancel();
        verify(api).deleteApplication("app", "owner-uid");
        verify(api, never()).deleteApplication("app");
    }

    @Test
    void freshHaRetrievalUsesStableServiceAndItsPersistedPort() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob owner = job(new V1JobStatus().active(1));
        owner.getInternalResource()
                .getMetadata()
                .setAnnotations(
                        Map.of(
                                HaApplicationState.ENABLED,
                                "true",
                                HaApplicationState.STATUS,
                                "RUNNING"));
        when(api.getJob("app")).thenReturn(owner);
        when(api.getService("app"))
                .thenReturn(
                        new KubernetesService(
                                new io.kubernetes.client.openapi.models.V1Service()
                                        .metadata(new V1ObjectMeta().name("app"))
                                        .spec(
                                                new io.kubernetes.client.openapi.models
                                                                .V1ServiceSpec()
                                                        .clusterIP("10.0.0.8")
                                                        .addPortsItem(
                                                                new io.kubernetes.client.openapi
                                                                                .models
                                                                                .V1ServicePort()
                                                                        .port(5909)))));
        try (MockedStatic<ConfigProvider> configurations = mockStatic(ConfigProvider.class)) {
            configurations
                    .when(ConfigProvider::locateAndGetClientConfig)
                    .thenReturn(new ClientConfig());
            try (KubernetesApplicationClusterDescriptor descriptor =
                    new KubernetesApplicationClusterDescriptor(api, options())) {
                descriptor.retrieve("app").getClusterClient().close();
            }
        }
        assertEquals(
                Collections.singletonList("10.0.0.8:5909"),
                clientConfigs.get(0).getNetworkConfig().getAddresses());
        assertFalse(clientConfigs.get(0).getNetworkConfig().isSmartRouting());
        verify(api, never()).listPods(anyString());
    }

    @Test
    void haAmbiguousOwnerCreationNeverDeletesByNameOrLabels() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        when(api.getConfigMap("seatunnel-runtime"))
                .thenReturn(
                        new KubernetesConfigMap(
                                new V1ConfigMap()
                                        .metadata(new V1ObjectMeta().name("seatunnel-runtime"))
                                        .data(Map.of("seatunnel.yaml", "seatunnel: {}"))));
        when(api.createJob(any()))
                .thenThrow(new KubernetesClientException(504, "owner create reply lost"));
        Map<String, String> ha = new HashMap<>(options());
        ha.put(KubernetesOptions.HA_ENABLED.key(), "true");
        ha.put(KubernetesOptions.MASTER_REPLICAS.key(), "2");
        ha.put(KubernetesOptions.HA_BACKUP_COUNT.key(), "1");
        try (KubernetesApplicationClusterDescriptor descriptor =
                new KubernetesApplicationClusterDescriptor(api, ha)) {
            assertThrows(Exception.class, () -> descriptor.deployApplication(specification()));
        }
        verify(api).createJob(any());
        verify(api, never()).deleteApplication(anyString());
        verify(api, never()).deleteApplication(anyString(), anyString());
        verify(api, never()).createAccompanyingResources(any(), any());
    }

    @Test
    void haStatusWaitsForNativeRunningInsteadOfWorkerPodReadiness() throws Exception {
        KubernetesHaClient api = mock(KubernetesHaClient.class);
        KubernetesJob owner = job(new V1JobStatus().active(1));
        Map<String, String> state = new HashMap<>();
        state.put(HaApplicationState.ENABLED, "true");
        state.put(HaApplicationState.STATUS, "DEPLOYING");
        owner.getInternalResource().getMetadata().setAnnotations(state);
        when(api.getJob("app")).thenReturn(owner);
        when(api.listPods(anyString())).thenReturn(List.of(pod("worker", "Running")));
        KubernetesApplicationClient client = new KubernetesApplicationClient(api, "app");
        assertEquals(ApplicationStatus.DEPLOYING, client.getStatus());
        state.put(HaApplicationState.STATUS, "RUNNING");
        assertEquals(ApplicationStatus.RUNNING, client.getStatus());
        state.put(HaApplicationState.STATUS, "unexpected");
        assertEquals(ApplicationStatus.UNKNOWN, client.getStatus());
        verify(api, never()).listPods(anyString());
    }
}
