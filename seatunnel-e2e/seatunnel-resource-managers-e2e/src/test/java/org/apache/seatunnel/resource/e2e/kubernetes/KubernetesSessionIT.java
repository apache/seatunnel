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

package org.apache.seatunnel.resource.e2e.kubernetes;

import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.openapi.models.V1Pod;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/** End-to-end coverage for a two-master session cluster and its master-created workers. */
class KubernetesSessionIT extends AbstractKubernetesApplicationIT {

    private static final String SESSION_NAME = "seatunnel-session-cluster";
    private static final String MANIFEST_NAMESPACE = "seatunnel";
    private static final String MASTER_SELECTOR =
            "seatunnel.apache.org/session="
                    + SESSION_NAME
                    + ",seatunnel.apache.org/component=master";
    private static final String WORKER_SELECTOR =
            "seatunnel.apache.org/application-id="
                    + SESSION_NAME
                    + ",seatunnel.apache.org/role=worker";

    private String image;

    @Override
    protected void startScenarioFixtures() throws Exception {
        Map<String, String> suite =
                SeatunnelApplicationConfig.load(
                        suiteFile("suite.config").toPath(), Collections.emptyMap());
        image = suite.get(KubernetesOptions.IMAGE.key());
        applySessionManifests();
        awaitPods(MASTER_SELECTOR, 2);
        awaitPods(WORKER_SELECTOR, 2);
    }

    @Override
    protected void stopScenarioFixtures() throws Exception {
        execInCluster(
                "kubectl",
                "delete",
                "statefulset",
                SESSION_NAME + "-master",
                "--namespace",
                namespace,
                "--ignore-not-found=true");
    }

    @Test
    void runsBatchJobOnSessionWorkers() throws Exception {
        submitJob("batch", "job.conf");
    }

    @Test
    void provisionsWorkerForPendingJobSlots() throws Exception {
        submitJob("session", "dynamic-job.conf");
        awaitPods(WORKER_SELECTOR, 3);
    }

    private void submitJob(String scenario, String file) throws Exception {
        String master = runningPod(MASTER_SELECTOR);
        Path job = scenarioFile(scenario, file).toPath();
        String remoteJob = "/tmp/session-job.conf";
        writeToCluster(remoteJob, Files.readAllBytes(job));
        execInCluster(
                "kubectl",
                "cp",
                "/tmp/session-job.conf",
                namespace + "/" + master + ":" + remoteJob);

        Container.ExecResult result =
                execInCluster(
                        "kubectl",
                        "exec",
                        "--namespace",
                        namespace,
                        master,
                        "--",
                        "/opt/seatunnel/bin/seatunnel.sh",
                        "--config",
                        remoteJob,
                        "-m",
                        "cluster");
        Assertions.assertEquals(
                0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private void applySessionManifests() throws Exception {
        Path directory = Paths.get(PROJECT_ROOT_PATH, "deploy/kubernetes/session");
        List<String> manifests = new ArrayList<>();
        manifests.add(sessionConfigMap(directory));
        manifests.add(
                configMap(
                        directory,
                        "seatunnel-session-cluster-master-config",
                        "config/master",
                        "seatunnel.yaml",
                        "hazelcast-master.yaml",
                        "hazelcast-client.yaml",
                        "log4j2.properties",
                        "jvm_master_options"));
        manifests.add(
                configMap(
                        directory,
                        "seatunnel-session-cluster-worker",
                        "config/worker",
                        "seatunnel.yaml",
                        "hazelcast-worker.yaml",
                        "log4j2.properties",
                        "jvm_worker_options"));
        for (String name :
                new String[] {
                    "root-configmap.yaml",
                    "rbac.yaml",
                    "services.yaml",
                    "statefulset-master.yaml"
                }) {
            manifests.add(render(Files.readString(directory.resolve(name), StandardCharsets.UTF_8)));
        }
        applyYaml(String.join("\n---\n", manifests));
    }

    private String sessionConfigMap(Path directory) throws Exception {
        return configMap(directory, "seatunnel-session-cluster-session", "", "session.yaml");
    }

    private String configMap(Path directory, String name, String subdirectory, String... files)
            throws Exception {
        StringBuilder yaml =
                new StringBuilder()
                        .append("apiVersion: v1\n")
                        .append("kind: ConfigMap\n")
                        .append("metadata:\n")
                        .append("  name: ")
                        .append(name)
                        .append("\n")
                        .append("  namespace: ")
                        .append(namespace)
                        .append("\n")
                        .append("  labels:\n")
                        .append("    seatunnel.apache.org/session: seatunnel-session-cluster\n")
                        .append("data:\n");
        for (String file : files) {
            String content =
                    render(
                            Files.readString(
                                    directory.resolve(subdirectory).resolve(file),
                                    StandardCharsets.UTF_8));
            String indented =
                    content.lines().map(line -> "    " + line).collect(Collectors.joining("\n"));
            yaml.append("  ").append(file).append(": |\n").append(indented).append("\n");
        }
        return yaml.toString();
    }

    private String render(String content) {
        return content.replace("namespace: seatunnel", "namespace: " + namespace)
                .replace(
                        "." + MANIFEST_NAMESPACE + ".svc.cluster.local",
                        "." + namespace + ".svc.cluster.local")
                .replace("apache/seatunnel:3.0.1", image)
                .replace("-Xms3g -Xmx3g", "-Xms512m -Xmx512m")
                .replace("-Xms3g", "-Xms512m")
                .replace("-Xmx3g", "-Xmx512m")
                .replace("memory-mb: 4096", "memory-mb: 768")
                .replace("heap-mb: 3072", "heap-mb: 512")
                .replace("cpu-cores: 2", "cpu-cores: 1")
                .replace("memory: 6Gi", "memory: 768Mi")
                .replace("cpu: \"2\"", "cpu: \"500m\"")
                .replace("cpu: \"4\"", "cpu: \"1\"")
                .replace("max-workers: 32", "max-workers: 3");
    }

    private void applyYaml(String content) throws Exception {
        String encoded =
                Base64.getEncoder().encodeToString(content.getBytes(StandardCharsets.UTF_8));
        Container.ExecResult result =
                execInCluster(
                        "sh", "-c", "printf %s '" + encoded + "' | base64 -d | kubectl apply -f -");
        Assertions.assertEquals(
                0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
    }

    private void writeToCluster(String path, byte[] content) throws Exception {
        String encoded = Base64.getEncoder().encodeToString(content);
        Container.ExecResult result =
                execInCluster("sh", "-c", "printf %s '" + encoded + "' | base64 -d > " + path);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private void awaitPods(String selector, int expected) {
        Awaitility.await()
                .atMost(300, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<V1Pod> pods = pods(selector);
                            long ready =
                                    pods.stream()
                                            .filter(
                                                    pod ->
                                                            pod.getStatus() != null
                                                                    && pod.getStatus()
                                                                                    .getContainerStatuses()
                                                                            != null
                                                                    && pod.getStatus()
                                                                            .getContainerStatuses()
                                                                            .stream()
                                                                            .allMatch(
                                                                                    status ->
                                                                                            status
                                                                                                                    .getReady()
                                                                                                            != null
                                                                                                    && status
                                                                                                            .getReady()))
                                            .count();
                            Assertions.assertTrue(
                                    ready >= expected,
                                    () -> "Expected " + expected + " ready pods, got " + ready);
                        });
    }

    private List<V1Pod> pods(String selector) throws Exception {
        return core.listNamespacedPod(
                        namespace, null, null, null, null, selector, null, null, null, null, null,
                        null)
                .getItems();
    }

    private String runningPod(String selector) throws Exception {
        return pods(selector).stream()
                .filter(
                        pod ->
                                pod.getStatus() != null
                                        && "Running".equals(pod.getStatus().getPhase()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No running pod for " + selector))
                .getMetadata()
                .getName();
    }
}
