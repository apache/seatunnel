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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesSessionConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;

class SessionWorkerPodFactoryTest {

    private static final String SESSION_YAML =
            "session:\n"
                    + "  name: seatunnel-session-cluster\n"
                    + "  cluster-name: seatunnel-session-cluster\n"
                    + "  root-config-map: seatunnel-session-cluster-meta\n"
                    + "  data-claim: seatunnel-session-cluster-data\n"
                    + "  worker:\n"
                    + "    min-workers: 2\n"
                    + "    max-workers: 8\n"
                    + "    config-map: seatunnel-session-cluster-worker\n"
                    + "    cpu-cores: 4\n"
                    + "    memory-mb: 16384\n"
                    + "    heap-mb: 8192\n"
                    + "  dynamic-worker:\n"
                    + "    enabled: true\n"
                    + "    startup-timeout-millis: 60000\n"
                    + "    retry-interval-millis: 1000\n"
                    + "kubernetes:\n"
                    + "  namespace: seatunnel\n"
                    + "  image: apache/seatunnel:3.0.1\n"
                    + "  image-pull-secrets: registry-secret\n"
                    + "  secret-ref: session-secret\n"
                    + "  worker:\n"
                    + "    node-selector: \"pool:session\"\n"
                    + "    tolerations: \"dedicated=session:NoSchedule\"\n"
                    + "  env:\n"
                    + "    SESSION_ENV: chain\n";

    @Test
    void createsBarePodOwnedBySessionRoot() throws Exception {
        KubernetesSessionConfig config = KubernetesSessionConfig.read(SESSION_YAML, 8);
        KubernetesConfigMap root =
                new KubernetesConfigMap(
                        new V1ConfigMap()
                                .metadata(
                                        new V1ObjectMeta()
                                                .name("seatunnel-session-cluster-meta")
                                                .uid("root-uid")));

        V1Pod pod =
                SessionWorkerPodFactory.createWorkerPod(
                        root,
                        "seatunnel-session-cluster-worker-1",
                        config,
                        new WorkerSpecification(16384, 4, 8));

        Assertions.assertEquals("seatunnel-session-cluster-worker-1", pod.getMetadata().getName());
        Assertions.assertEquals("root-uid", pod.getMetadata().getOwnerReferences().get(0).getUid());
        Assertions.assertEquals(
                KubernetesConstants.WORKER_ROLE,
                pod.getMetadata().getLabels().get(KubernetesConstants.ROLE_LABEL));
        Assertions.assertEquals(
                "seatunnel-session-cluster",
                pod.getMetadata().getLabels().get("seatunnel.apache.org/session"));
        Assertions.assertEquals(
                KubernetesConstants.RESTART_POLICY_NEVER, pod.getSpec().getRestartPolicy());
        Assertions.assertEquals(
                "registry-secret", pod.getSpec().getImagePullSecrets().get(0).getName());
        Assertions.assertEquals("session", pod.getSpec().getNodeSelector().get("pool"));
        Assertions.assertEquals("dedicated", pod.getSpec().getTolerations().get(0).getKey());
        Assertions.assertEquals("NoSchedule", pod.getSpec().getTolerations().get(0).getEffect());
        Assertions.assertEquals(
                "/opt/seatunnel/bin/seatunnel-cluster.sh",
                pod.getSpec().getContainers().get(0).getCommand().get(0));
        Assertions.assertFalse(pod.getSpec().getVolumes().isEmpty());
        Assertions.assertTrue(
                pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                        .anyMatch(
                                mount ->
                                        mount.getMountPath()
                                                .equals("/opt/seatunnel/config/seatunnel.yaml")));
        Assertions.assertTrue(
                pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                        .anyMatch(mount -> mount.getMountPath().equals("/opt/seatunnel/data")));
        Assertions.assertTrue(
                pod.getSpec().getVolumes().stream()
                        .anyMatch(
                                volume ->
                                        "seatunnel-session-cluster-data".equals(volume.getName())
                                                && "seatunnel-session-cluster-data"
                                                        .equals(
                                                                volume.getPersistentVolumeClaim()
                                                                        .getClaimName())));
        Assertions.assertTrue(
                pod.getSpec().getContainers().get(0).getVolumeMounts().stream()
                        .anyMatch(
                                mount ->
                                        mount.getMountPath()
                                                .equals("/etc/seatunnel-secrets/application")));
        Assertions.assertTrue(
                pod.getSpec().getContainers().get(0).getEnv().stream()
                        .anyMatch(
                                variable ->
                                        variable.getName().equals("SESSION_ENV")
                                                && variable.getValue().equals("chain")));
        Assertions.assertEquals(
                "16Gi",
                pod.getSpec()
                        .getContainers()
                        .get(0)
                        .getResources()
                        .getLimits()
                        .get(KubernetesConstants.MEMORY_RESOURCE)
                        .toSuffixedString());
    }
}
