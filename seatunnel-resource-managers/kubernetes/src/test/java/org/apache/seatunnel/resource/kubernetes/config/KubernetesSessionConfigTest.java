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
                    + "  master:\n"
                    + "    port: 5801\n"
                    + "  worker:\n"
                    + "    min-workers: 2\n"
                    + "    max-workers: 8\n"
                    + "    slots: 8\n"
                    + "    cpu-cores: 4\n"
                    + "    memory-mb: 16384\n"
                    + "    heap-mb: 8192\n"
                    + "    image: apache/seatunnel:3.0.1\n"
                    + "    config-map: seatunnel-session-cluster-worker\n"
                    + "  dynamic-worker:\n"
                    + "    enabled: true\n"
                    + "    startup-timeout-millis: 60000\n"
                    + "    retry-interval-millis: 1000\n"
                    + "kubernetes:\n"
                    + "  namespace: seatunnel\n";

    @Test
    void parsesSessionWorkerTemplate() throws Exception {
        KubernetesSessionConfig config = KubernetesSessionConfig.read(SESSION_YAML);

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
        Assertions.assertEquals("seatunnel-session-cluster-worker", config.getWorkerConfigMap());
        Assertions.assertTrue(config.getSessionSpecification().isDynamicWorkerEnabled());
    }

    @Test
    void defaultsMinWorkersToTwo() throws Exception {
        KubernetesSessionConfig config =
                KubernetesSessionConfig.read(SESSION_YAML.replace("    min-workers: 2\n", ""));

        Assertions.assertEquals(2, config.getSessionSpecification().getMinWorkers());
    }

    @Test
    void rejectsDynamicHeapNotBelowContainerMemory() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        KubernetesSessionConfig.read(
                                SESSION_YAML.replace("heap-mb: 8192", "heap-mb: 16384")));
    }
}
