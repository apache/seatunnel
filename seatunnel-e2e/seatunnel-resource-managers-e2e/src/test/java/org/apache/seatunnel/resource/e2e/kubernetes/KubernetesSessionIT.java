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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

/** End-to-end coverage for a two-master session cluster and its master-created workers. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KubernetesSessionIT extends AbstractKubernetesApplicationIT {

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
    private static final Pattern JOB_ID_PATTERN =
            Pattern.compile("\"jobId\"\\s*:\\s*\"?([0-9]+)\"?");
    private static final Pattern FINISHED_PATTERN =
            Pattern.compile("\"jobStatus\"\\s*:\\s*\"FINISHED\"");
    private static final Pattern FAILED_PATTERN =
            Pattern.compile("\"jobStatus\"\\s*:\\s*\"(FAILED|CANCELED|UNKNOWABLE)\"");
    private static final long JOB_WAIT_SECONDS = 180L;

    private String image;

    @Override
    protected void startScenarioFixtures() throws Exception {
        Map<String, String> suite =
                SeatunnelApplicationConfig.load(
                        suiteFile("suite.config").toPath(), Collections.emptyMap());
        image = suite.get(KubernetesOptions.IMAGE.key());
        Container.ExecResult dns =
                execInCluster(
                        "kubectl",
                        "-n",
                        "kube-system",
                        "rollout",
                        "status",
                        "deployment/coredns",
                        "--timeout=120s");
        if (dns.getExitCode() != 0) {
            // The session masters retry DNS discovery; an image-pull delay must not fail the test
            // before that retry window is used.
            System.err.println(
                    "CoreDNS did not become ready before the session apply: " + dns.getStderr());
        }
        applySessionManifests();
        awaitPods(MASTER_SELECTOR, 2);
        awaitPods(WORKER_SELECTOR, 2);
    }

    @Override
    protected void stopScenarioFixtures() throws Exception {
        Container.ExecResult deleted =
                execInCluster(
                        "kubectl",
                        "delete",
                        "--namespace",
                        namespace,
                        "--ignore-not-found=true",
                        "statefulset/" + SESSION_NAME + "-master",
                        "configmap/" + SESSION_NAME + "-meta",
                        "configmap/" + SESSION_NAME + "-config",
                        "service/" + SESSION_NAME + "-headless",
                        "service/" + SESSION_NAME + "-rest",
                        "service/" + SESSION_NAME + "-client",
                        "pdb/" + SESSION_NAME + "-master",
                        "persistentvolumeclaim/" + SESSION_NAME + "-data",
                        "serviceaccount/" + SESSION_NAME + "-master",
                        "role/" + SESSION_NAME + "-master",
                        "rolebinding/" + SESSION_NAME + "-master");
        Assertions.assertEquals(0, deleted.getExitCode(), deleted.getStderr());
        awaitNoPods(MASTER_SELECTOR);
        awaitNoPods(WORKER_SELECTOR);
    }

    private void awaitNoPods(String selector) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertTrue(pods(selector).isEmpty()));
    }

    @Test
    @Order(1)
    void runsBatchJobOnSessionWorkers() throws Exception {
        submitJob("batch", "job.conf");
    }

    @Test
    @Order(2)
    void replacesDeletedWorker() throws Exception {
        String worker = runningPod(WORKER_SELECTOR);
        execInCluster(
                "kubectl",
                "delete",
                "pod",
                worker,
                "--namespace",
                namespace,
                "--ignore-not-found=true",
                "--wait=false");
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () ->
                                pods(WORKER_SELECTOR).stream()
                                        .noneMatch(
                                                pod -> worker.equals(pod.getMetadata().getName())));
        awaitPods(WORKER_SELECTOR, 2);
    }

    @Test
    @Order(3)
    void recreatesBothMastersAndReusesWorkers() throws Exception {
        List<String> masters =
                pods(MASTER_SELECTOR).stream()
                        .map(pod -> pod.getMetadata().getName())
                        .collect(Collectors.toList());
        Assertions.assertEquals(2, masters.size());
        Map<String, String> masterUids = new HashMap<>();
        for (V1Pod pod : pods(MASTER_SELECTOR)) {
            masterUids.put(pod.getMetadata().getName(), pod.getMetadata().getUid());
        }

        List<String> delete = new ArrayList<>();
        delete.add("kubectl");
        delete.add("delete");
        delete.add("pod");
        delete.add("--namespace");
        delete.add(namespace);
        delete.add("--ignore-not-found=true");
        delete.add("--wait=false");
        delete.addAll(masters);
        Container.ExecResult deleted = execInCluster(delete.toArray(new String[0]));
        Assertions.assertEquals(0, deleted.getExitCode(), deleted.getStderr());

        Awaitility.await()
                .atMost(240, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(
                        () ->
                                pods(MASTER_SELECTOR).stream()
                                                .filter(KubernetesSessionIT::ready)
                                                .filter(
                                                        pod ->
                                                                !masterUids.containsValue(
                                                                        pod.getMetadata().getUid()))
                                                .count()
                                        >= 2);
        awaitPods(MASTER_SELECTOR, 2);
        awaitPods(WORKER_SELECTOR, 2);
        submitJob("batch", "job.conf");
    }

    @Test
    @Order(4)
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
                        "curl",
                        "-sS",
                        "-X",
                        "POST",
                        "-H",
                        "Content-Type: text/plain",
                        "http://127.0.0.1:8080/submit-job?format=hocon",
                        "--data-binary",
                        "@" + remoteJob);
        Assertions.assertEquals(
                0, result.getExitCode(), () -> result.getStdout() + result.getStderr());
        if (!result.getStdout().contains("jobId")) {
            Container.ExecResult logs =
                    execInCluster(
                            "kubectl", "logs", "--namespace", namespace, master, "--tail=200");
            Assertions.fail(
                    "REST submit failed: "
                            + result.getStdout()
                            + result.getStderr()
                            + "\n"
                            + logs.getStdout()
                            + logs.getStderr());
        }
        awaitJobFinished(master, jobId(result.getStdout()));
    }

    private void awaitJobFinished(String master, String jobId) {
        Awaitility.await()
                .atMost(JOB_WAIT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Container.ExecResult status =
                                    execInCluster(
                                            "kubectl",
                                            "exec",
                                            "--namespace",
                                            namespace,
                                            master,
                                            "--",
                                            "curl",
                                            "-sS",
                                            "http://127.0.0.1:8080/job-info/" + jobId);
                            Assertions.assertEquals(
                                    0,
                                    status.getExitCode(),
                                    () -> status.getStdout() + status.getStderr());
                            Assertions.assertFalse(
                                    FAILED_PATTERN.matcher(status.getStdout()).find(),
                                    () ->
                                            "Job "
                                                    + jobId
                                                    + " failed: "
                                                    + status.getStdout()
                                                    + "\n"
                                                    + masterLogs());
                            Assertions.assertTrue(
                                    FINISHED_PATTERN.matcher(status.getStdout()).find(),
                                    () ->
                                            "Job "
                                                    + jobId
                                                    + " did not finish: "
                                                    + status.getStdout()
                                                    + "\n"
                                                    + masterLogs());
                        });
    }

    private String masterLogs() {
        StringBuilder logs = new StringBuilder();
        try {
            for (V1Pod pod : pods(MASTER_SELECTOR)) {
                String name = pod.getMetadata().getName();
                Container.ExecResult result =
                        execInCluster(
                                "kubectl", "logs", "--namespace", namespace, name, "--tail=400");
                logs.append("=== ").append(name).append(" ===\n");
                logs.append(result.getStdout()).append(result.getStderr()).append('\n');
            }
            return logs.toString();
        } catch (Exception failure) {
            return "Could not read master logs: " + failure;
        }
    }

    private static String jobId(String response) {
        Matcher matcher = JOB_ID_PATTERN.matcher(response);
        Assertions.assertTrue(
                matcher.find(), () -> "REST submit response has no job id: " + response);
        return matcher.group(1);
    }

    private void applySessionManifests() throws Exception {
        Path directory = Paths.get(PROJECT_ROOT_PATH, "deploy/kubernetes/session");
        List<String> manifests = new ArrayList<>();
        manifests.add(
                configMap(
                        directory,
                        "seatunnel-session-cluster-config",
                        "conf/seatunnel.yaml",
                        "conf/hazelcast-master.yaml",
                        "conf/hazelcast-worker.yaml",
                        "conf/hazelcast-client.yaml",
                        "conf/log4j2.properties",
                        "conf/jvm_master_options",
                        "conf/jvm_client_options",
                        "conf/jvm_worker_options",
                        "session-cluster.yaml"));
        for (String name :
                new String[] {
                    "root-configmap.yaml",
                    "rbac.yaml",
                    "services.yaml",
                    "pvc.yaml",
                    "statefulset-master.yaml"
                }) {
            manifests.add(
                    render(Files.readString(directory.resolve(name), StandardCharsets.UTF_8)));
        }
        applyYaml(String.join("\n---\n", manifests));
    }

    private String configMap(Path directory, String name, String... entries) throws Exception {
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
        for (String entry : entries) {
            int separator = entry.indexOf('=');
            String path = separator < 0 ? entry : entry.substring(separator + 1);
            String key = Paths.get(path).getFileName().toString();
            String content =
                    render(Files.readString(directory.resolve(path), StandardCharsets.UTF_8));
            String indented =
                    content.lines().map(line -> "    " + line).collect(Collectors.joining("\n"));
            yaml.append("  ").append(key).append(": |\n").append(indented).append("\n");
        }
        return yaml.toString();
    }

    private String render(String content) {
        return content.replace("namespace: seatunnel", "namespace: " + namespace)
                .replace(
                        "." + MANIFEST_NAMESPACE + ".svc.cluster.local",
                        "." + namespace + ".svc.cluster.local")
                .replace("apache/seatunnel:<session-enabled-image>", image)
                .replace("ReadWriteMany", "ReadWriteOnce")
                .replace("-Xms3g -Xmx3g", "-Xms512m -Xmx512m")
                .replace("-Xms3g", "-Xms512m")
                .replace("-Xmx3g", "-Xmx512m")
                .replace("memory-mb: 4096", "memory-mb: 768")
                .replace("heap-mb: 3072", "heap-mb: 512")
                .replace("cpu-cores: 2", "cpu-cores: 1")
                .replace("memory: 6Gi", "memory: 768Mi")
                .replace("cpu: \"2\"", "cpu: \"500m\"")
                .replace("cpu: \"4\"", "cpu: \"1\"")
                .replace("max-workers: 32", "max-workers: 4");
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
                            long ready =
                                    pods(selector).stream()
                                            .filter(KubernetesSessionIT::ready)
                                            .count();
                            Assertions.assertTrue(
                                    ready >= expected,
                                    () -> "Expected " + expected + " ready pods, got " + ready);
                        });
    }

    private static boolean ready(V1Pod pod) {
        return pod.getStatus() != null
                && pod.getStatus().getContainerStatuses() != null
                && pod.getStatus().getContainerStatuses().stream()
                        .allMatch(status -> status.getReady() != null && status.getReady());
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
