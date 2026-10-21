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

package org.apache.seatunnel.resource.kubernetes.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KubernetesSessionConfigTest {

    private static final String SESSION_YAML =
            "session:\n"
                    + "  name: seatunnel-session-cluster\n"
                    + "  cluster-name: seatunnel-session-cluster\n"
                    + "  root-config-map: seatunnel-session-cluster-meta\n"
                    + "  data-claim: seatunnel-session-cluster-data\n"
                    + "  data-mount: /opt/seatunnel/data\n"
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
                    + "  worker:\n"
                    + "    node-selector: \"pool:session\"\n"
                    + "    tolerations: \"dedicated=session:NoSchedule\"\n";

    @Test
    void parsesSessionPolicyAndWorkerTemplate() throws Exception {
        KubernetesSessionConfig config = KubernetesSessionConfig.read(SESSION_YAML, 8);

        Assertions.assertEquals(
                "seatunnel-session-cluster", config.getSessionSpecification().getName());
        Assertions.assertEquals(2, config.getSessionSpecification().getMinWorkers());
        Assertions.assertEquals(8, config.getSessionSpecification().getMaxWorkers());
        Assertions.assertEquals(
                8, config.getSessionSpecification().getWorkerSpecification().getSlots());
        Assertions.assertEquals(
                4, config.getSessionSpecification().getWorkerSpecification().getCpuCores());
        Assertions.assertEquals(
                16384, config.getSessionSpecification().getWorkerSpecification().getMemoryMb());
        Assertions.assertEquals(8192, config.getHeapMb());
        Assertions.assertEquals("seatunnel", config.getNamespace());
        Assertions.assertEquals("seatunnel-session-cluster-data", config.getDataClaim());
        Assertions.assertEquals("/opt/seatunnel/data", config.getDataMount());
        Assertions.assertEquals("seatunnel-session-cluster-worker", config.getWorkerConfigMap());
        Assertions.assertEquals("registry-secret", config.getImagePullSecrets().get(0));
        Assertions.assertEquals("session", config.getWorkerNodeSelector().get("pool"));
        Assertions.assertEquals(1, config.getWorkerTolerations().size());
        Assertions.assertEquals("dedicated", config.getWorkerTolerations().get(0).getKey());
        Assertions.assertEquals("NoSchedule", config.getWorkerTolerations().get(0).getEffect());
        Assertions.assertEquals(0, config.getMasterNodeSelector().size());
        Assertions.assertEquals(0, config.getMasterTolerations().size());
        Assertions.assertNull(config.getSeatunnelConfigFile());
        Assertions.assertNull(config.getLog4jConfigFile());
        Assertions.assertTrue(config.getSessionSpecification().isDynamicWorkerEnabled());
    }

    @Test
    void defaultsMinWorkersToTwo() throws Exception {
        KubernetesSessionConfig config =
                KubernetesSessionConfig.read(SESSION_YAML.replace("    min-workers: 2\n", ""), 8);

        Assertions.assertEquals(2, config.getSessionSpecification().getMinWorkers());
    }

    @Test
    void rejectsHeapNotBelowContainerMemory() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesSessionConfig.read(
                                SESSION_YAML.replace("heap-mb: 8192", "heap-mb: 16384"), 8));
    }

    @Test
    void rejectsDataClaimLongerThanAVolumeName() {
        String tooLong = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesSessionConfig.read(
                                SESSION_YAML.replace(
                                        "data-claim: seatunnel-session-cluster-data",
                                        "data-claim: " + tooLong),
                                8));
    }

    @Test
    void rejectsDataClaimThatCollidesWithAReservedVolumeName() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesSessionConfig.read(
                                SESSION_YAML.replace(
                                        "data-claim: seatunnel-session-cluster-data",
                                        "data-claim: application"),
                                8));
    }

    @Test
    void rejectsDataMountThatOverlapsWorkerConfig() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesSessionConfig.read(
                                SESSION_YAML.replace(
                                        "data-mount: /opt/seatunnel/data",
                                        "data-mount: /opt/seatunnel/config"),
                                8));
    }

    @Test
    void rejectsUnknownSessionKey() {
        IllegalArgumentException failure =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KubernetesSessionConfig.read(
                                        SESSION_YAML.replace(
                                                "  worker:\n",
                                                "  unknown-option: true\n  worker:\n"),
                                        8));
        Assertions.assertTrue(failure.getMessage().contains("session.unknown-option"));
    }

    @Test
    void rejectsUnknownWorkerKey() {
        IllegalArgumentException failure =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                KubernetesSessionConfig.read(
                                        SESSION_YAML.replace(
                                                "    heap-mb: 8192\n",
                                                "    heap-mb: 8192\n    unknown-option: true\n"),
                                        8));
        Assertions.assertTrue(failure.getMessage().contains("session.worker.unknown-option"));
    }
}
