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

package org.apache.seatunnel.resource.kubernetes.kubeclient.factory;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Secret;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KubernetesResourceFactoryTest {
    @Test
    void mountsExistingCheckpointClaimOnlyOnMaster() {
        Map<String, String> options = new HashMap<>(specification().getOptions());
        options.put(KubernetesOptions.CHECKPOINT_PVC.key(), "existing-checkpoints");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, "env {}", options);
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification);
        KubernetesJob job = KubernetesResourceFactory.job("application", parameters);
        V1Job jobResource = job.getInternalResource();
        assertEquals(
                "existing-checkpoints",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getVolumes()
                        .get(1)
                        .getPersistentVolumeClaim()
                        .getClaimName());
        assertEquals(
                "/opt/seatunnel/checkpoints",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getVolumeMounts()
                        .get(1)
                        .getMountPath());
        jobResource.getMetadata().setUid("owner-uid");
        V1Pod worker =
                KubernetesResourceFactory.worker(
                                job,
                                "worker",
                                parameters,
                                specification.getWorkerSpecification(),
                                "cluster",
                                "master:5801")
                        .getInternalResource();
        assertTrue(
                worker.getSpec().getVolumes() == null || worker.getSpec().getVolumes().isEmpty());
        assertTrue(
                worker.getSpec().getContainers().get(0).getVolumeMounts() == null
                        || worker.getSpec().getContainers().get(0).getVolumeMounts().isEmpty());
    }

    @Test
    void buildsOwnedIsolatedResourcesWithoutShellInterpolation() throws Exception {
        Map<String, String> options = new HashMap<>(specification().getOptions());
        options.put(KubernetesOptions.IMAGE_PULL_SECRETS.key(), "registry-one,registry-two");
        options.put(KubernetesOptions.MASTER_LABELS.key(), "workload:control");
        options.put(KubernetesOptions.WORKER_LABELS.key(), "workload:data");
        options.put(KubernetesOptions.MASTER_ANNOTATIONS.key(), "owner:platform");
        options.put(KubernetesOptions.WORKER_ANNOTATIONS.key(), "owner:runtime");
        options.put(KubernetesOptions.MASTER_NODE_SELECTOR.key(), "pool:master");
        options.put(KubernetesOptions.WORKER_NODE_SELECTOR.key(), "pool:worker");
        options.put(KubernetesOptions.CONFIG_MAP.key(), "seatunnel-runtime");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, "env {}", options);
        KubernetesApplicationParameters parameters =
                KubernetesApplicationParameters.from(specification);
        KubernetesJob job = KubernetesResourceFactory.job("app", parameters);
        V1Job jobResource = job.getInternalResource();
        jobResource.getMetadata().setUid("uid-1");
        assertTrue(jobResource.getSpec().getSuspend());
        assertEquals(0, jobResource.getSpec().getBackoffLimit());
        assertEquals("Never", jobResource.getSpec().getTemplate().getSpec().getRestartPolicy());
        assertEquals(
                "control",
                jobResource.getSpec().getTemplate().getMetadata().getLabels().get("workload"));
        assertEquals(
                "platform",
                jobResource.getSpec().getTemplate().getMetadata().getAnnotations().get("owner"));
        assertEquals(
                "master",
                jobResource.getSpec().getTemplate().getSpec().getNodeSelector().get("pool"));
        assertEquals(
                "registry-two",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getImagePullSecrets()
                        .get(1)
                        .getName());
        KubernetesSecret secret = KubernetesResourceFactory.secret(job, specification);
        V1Secret configuration = secret.getInternalResource();
        assertEquals("uid-1", configuration.getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals("Opaque", configuration.getType());
        assertFalse(
                configuration
                        .getStringData()
                        .get(KubernetesResourceFactory.SPECIFICATION_FILE)
                        .contains("submitter-kubeconfig"));
        assertEquals(
                0400,
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getVolumes()
                        .get(0)
                        .getSecret()
                        .getDefaultMode());
        assertEquals(
                "seatunnel-runtime",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getVolumes()
                        .get(1)
                        .getConfigMap()
                        .getName());
        assertEquals(
                "/opt/seatunnel/config",
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getVolumeMounts()
                        .get(1)
                        .getMountPath());
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getVolumeMounts()
                        .get(1)
                        .getReadOnly());
        KubernetesService service = KubernetesResourceFactory.service(job, parameters);
        assertEquals("None", service.getInternalResource().getSpec().getClusterIP());
        KubernetesPod workerPod =
                KubernetesResourceFactory.worker(
                        job,
                        "app-worker-0",
                        parameters,
                        specification.getWorkerSpecification(),
                        "isolated-cluster",
                        "10.0.0.1:5801");
        V1Pod worker = workerPod.getInternalResource();
        assertFalse(worker.getSpec().getAutomountServiceAccountToken());
        assertEquals("uid-1", worker.getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals("data", worker.getMetadata().getLabels().get("workload"));
        assertEquals("runtime", worker.getMetadata().getAnnotations().get("owner"));
        assertEquals("worker", worker.getSpec().getNodeSelector().get("pool"));
        assertEquals("registry-one", worker.getSpec().getImagePullSecrets().get(0).getName());
        assertEquals(
                "seatunnel-runtime", worker.getSpec().getVolumes().get(0).getConfigMap().getName());
        assertEquals(
                "/opt/seatunnel/config",
                worker.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath());
        assertEquals("java", worker.getSpec().getContainers().get(0).getCommand().get(0));
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(worker.getSpec().getContainers().get(0).getCommand().contains("10.0.0.1:5801"));
        assertTrue(
                worker.getSpec().getContainers().get(0).getCommand().contains("isolated-cluster"));
        assertEquals(
                "worker",
                worker.getMetadata().getLabels().get(KubernetesResourceFactory.ROLE_LABEL));
        assertTrue(
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getMetadata()
                        .getLabels()
                        .entrySet()
                        .containsAll(
                                service.getInternalResource().getSpec().getSelector().entrySet()));

        assertEquals(
                5801,
                jobResource
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getReadinessProbe()
                        .getTcpSocket()
                        .getPort()
                        .getIntValue());
        assertTrue(
                jobResource
                                .getSpec()
                                .getTemplate()
                                .getSpec()
                                .getContainers()
                                .get(0)
                                .getStartupProbe()
                        != null);
        assertEquals(
                "kill -0 1",
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getLivenessProbe()
                        .getExec()
                        .getCommand()
                        .get(2));
    }

    private static ApplicationSpecification specification() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return ApplicationSpecification.fromOptions(
                DeployType.KUBERNETES, "env { job.mode = BATCH }", options);
    }
}
