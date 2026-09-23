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

package org.apache.seatunnel.resource.kubernetes.cluster;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KubernetesResourcesTest {
    @Test
    void mountsExistingCheckpointClaimOnlyOnMaster() {
        Map<String, String> options = new HashMap<>(specification().getOptions());
        options.put(KubernetesOptions.CHECKPOINT_PVC.key(), "existing-checkpoints");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.KUBERNETES, "env {}", options);
        KubernetesOptions.validate(specification);
        V1Job job = KubernetesResources.job("application", specification);
        assertEquals(
                "existing-checkpoints",
                job.getSpec()
                        .getTemplate()
                        .getSpec()
                        .getVolumes()
                        .get(1)
                        .getPersistentVolumeClaim()
                        .getClaimName());
        assertEquals(
                "/opt/seatunnel/checkpoints",
                job.getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0)
                        .getVolumeMounts()
                        .get(1)
                        .getMountPath());
        job.getMetadata().setUid("owner-uid");
        V1Pod worker =
                KubernetesResources.worker(
                        job,
                        "worker",
                        specification,
                        specification.getWorkerSpecification(),
                        "cluster",
                        "master:5801");
        assertTrue(
                worker.getSpec().getVolumes() == null || worker.getSpec().getVolumes().isEmpty());
        assertTrue(
                worker.getSpec().getContainers().get(0).getVolumeMounts() == null
                        || worker.getSpec().getContainers().get(0).getVolumeMounts().isEmpty());
    }

    @Test
    void buildsOwnedIsolatedResourcesWithoutShellInterpolation() throws Exception {
        ApplicationSpecification specification = specification();
        V1Job job = job();
        assertTrue(job.getSpec().getSuspend());
        assertEquals(0, job.getSpec().getBackoffLimit());
        assertEquals("Never", job.getSpec().getTemplate().getSpec().getRestartPolicy());
        V1ConfigMap configuration = KubernetesResources.configMap(job, specification);
        assertEquals("uid-1", configuration.getMetadata().getOwnerReferences().get(0).getUid());
        assertFalse(
                configuration
                        .getData()
                        .get(KubernetesResources.SPECIFICATION_FILE)
                        .contains("submitter-kubeconfig"));
        assertEquals(
                "None", KubernetesResources.service(job, specification).getSpec().getClusterIP());
        V1Pod worker =
                KubernetesResources.worker(
                        job,
                        "app-worker-0",
                        specification,
                        specification.getWorkerSpecification(),
                        "isolated-cluster",
                        "10.0.0.1:5801");
        assertFalse(worker.getSpec().getAutomountServiceAccountToken());
        assertEquals("uid-1", worker.getMetadata().getOwnerReferences().get(0).getUid());
        assertEquals("java", worker.getSpec().getContainers().get(0).getCommand().get(0));
        assertTrue(
                worker.getSpec()
                        .getContainers()
                        .get(0)
                        .getCommand()
                        .contains("-Dseatunnel.config=/opt/seatunnel/config/seatunnel.yaml"));
        assertTrue(
                job.getSpec()
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
                "worker", worker.getMetadata().getLabels().get(KubernetesResources.ROLE_LABEL));
        assertEquals(
                KubernetesResources.service(job, specification).getSpec().getSelector(),
                job.getSpec().getTemplate().getMetadata().getLabels());
    }

    private static ApplicationSpecification specification() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return ApplicationSpecification.fromOptions(
                DeployType.KUBERNETES, "env { job.mode = BATCH }", options);
    }

    private static V1Job job() {
        V1Job job = KubernetesResources.job("app", specification());
        job.getMetadata().setUid("uid-1");
        return job;
    }
}
