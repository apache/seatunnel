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
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider deployment through the unchanged native Zeta client, election and recovery paths. */
public class KubernetesApplicationHaIT extends AbstractKubernetesApplicationIT {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesApplicationHaIT.class);
    private static final String CLAIM = "ha-application-checkpoints";
    private static final String INSPECTOR = "ha-user-inspector";
    private String pvcUid;
    private String secretUid;
    private String runtimeUid;

    @Override
    protected void startScenarioFixtures() throws Exception {
        V1PersistentVolumeClaim claim =
                Yaml.loadAs(suiteFile("checkpoints-pvc.yaml"), V1PersistentVolumeClaim.class);
        claim.getMetadata().setName(CLAIM);
        pvcUid =
                core.createNamespacedPersistentVolumeClaim(namespace, claim, null, null, null, null)
                        .getMetadata()
                        .getUid();
        runtimeUid =
                core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null)
                        .getMetadata()
                        .getUid();
        secretUid =
                core.createNamespacedSecret(
                                namespace,
                                new V1Secret()
                                        .metadata(new V1ObjectMeta().name("ha-user-secret"))
                                        .stringData(
                                                Collections.singletonMap(
                                                        "synthetic", "user-fixture-only")),
                                null,
                                null,
                                null,
                                null)
                        .getMetadata()
                        .getUid();
        V1Pod inspector =
                new V1Pod()
                        .metadata(new V1ObjectMeta().name(INSPECTOR))
                        .spec(
                                new V1PodSpec()
                                        .restartPolicy("Never")
                                        .automountServiceAccountToken(false)
                                        .containers(
                                                Collections.singletonList(
                                                        new V1Container()
                                                                .name("seatunnel")
                                                                .image(
                                                                        haOptions()
                                                                                .get(
                                                                                        KubernetesOptions
                                                                                                .IMAGE
                                                                                                .key()))
                                                                .imagePullPolicy("Never")
                                                                .command(
                                                                        Arrays.asList(
                                                                                "bash",
                                                                                "-c",
                                                                                "while sleep 3600; do :; done"))
                                                                .volumeMounts(
                                                                        Collections.singletonList(
                                                                                new V1VolumeMount()
                                                                                        .name(
                                                                                                "data")
                                                                                        .mountPath(
                                                                                                "/checkpoints")))))
                                        .volumes(
                                                Collections.singletonList(
                                                        new V1Volume()
                                                                .name("data")
                                                                .persistentVolumeClaim(
                                                                        new V1PersistentVolumeClaimVolumeSource()
                                                                                .claimName(
                                                                                        CLAIM)))));

        core.createNamespacedPod(namespace, inspector, null, null, null, null);
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        "Running",
                                        core.readNamespacedPod(INSPECTOR, namespace, null)
                                                .getStatus()
                                                .getPhase()));
        inspect("printf preserve-user-file > /checkpoints/ha-user-sentinel");
    }

    @Override
    protected void stopScenarioFixtures() throws Exception {
        core.deleteNamespacedPod(INSPECTOR, namespace, null, null, 0, null, "Background", null);
        core.deleteNamespacedSecret(
                "ha-user-secret", namespace, null, null, null, null, "Background", null);
        core.deleteNamespacedPersistentVolumeClaim(
                CLAIM, namespace, null, null, null, null, "Background", null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void masterPodLossKeepsNativeJobAndFixedWorkers(boolean loseActive) throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                String[] before = awaitProgress(application, specification.getJobId(), -1);
                assertEquals(Long.toString(specification.getJobId()), before[6]);
                assertEquals("2", before[7], "Both native masters must join before the fault");
                V1Pod worker = workers(application).get(0);
                String nativeWorkerUid = memberUid(before, worker);
                V1Service service =
                        core.readNamespacedService(application.getClusterId(), namespace, null);
                assertEquals("ClusterIP", service.getSpec().getType());
                assertNotEquals("None", service.getSpec().getClusterIP());
                String serviceUid = service.getMetadata().getUid();
                List<V1Pod> masters = rolePods(application, "master");
                V1Pod lost =
                        masters.stream()
                                .filter(
                                        pod ->
                                                loseActive
                                                        == before[3].equals(
                                                                pod.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                V1Pod survivor =
                        masters.stream()
                                .filter(
                                        pod ->
                                                !pod.getMetadata()
                                                        .getUid()
                                                        .equals(lost.getMetadata().getUid()))
                                .findFirst()
                                .get();
                core.deleteNamespacedPod(
                        lost.getMetadata().getName(),
                        namespace,
                        null,
                        null,
                        0,
                        null,
                        "Background",
                        null);
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .pollInterval(2, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () -> {
                                    V1Pod replacement =
                                            core.readNamespacedPod(
                                                    lost.getMetadata().getName(), namespace, null);
                                    assertNotEquals(
                                            lost.getMetadata().getUid(),
                                            replacement.getMetadata().getUid());
                                    assertEquals("Running", replacement.getStatus().getPhase());
                                    String[] state = probe(application, specification.getJobId());
                                    assertEquals("RUNNING", state[0]);
                                    assertEquals(Long.toString(specification.getJobId()), state[6]);
                                    assertEquals("2", state[7]);
                                    assertEquals(
                                            survivor.getStatus().getPodIP(),
                                            state[3],
                                            "The surviving native master must own the job");
                                    assertTrue(
                                            Arrays.asList(state[4].split(","))
                                                    .contains(replacement.getStatus().getPodIP()));
                                    assertFixedWorker(application, worker, nativeWorkerUid, state);
                                });
                long recoveredCheckpoint =
                        Long.parseLong(probe(application, specification.getJobId())[1]);
                String[] after =
                        awaitProgress(
                                application,
                                specification.getJobId(),
                                Math.max(Long.parseLong(before[1]), recoveredCheckpoint));
                assertEquals(Long.toString(specification.getJobId()), after[6]);
                assertFixedWorker(application, worker, nativeWorkerUid, after);
                assertEquals(
                        serviceUid,
                        core.readNamespacedService(application.getClusterId(), namespace, null)
                                .getMetadata()
                                .getUid());
                assertEquals(ApplicationStatus.RUNNING, application.getStatus());
                LOG.info(
                        "HA native master Pod loss verified application={} job={} activeLost={} fixedWorker={}",
                        application.getClusterId(),
                        specification.getJobId(),
                        loseActive,
                        worker.getMetadata().getUid());
                // Cancel the actual native job, then verify the provider observes its terminal
                // outcome and removes the application resources.
                probeCommand(application, specification.getJobId(), "cancel");
                awaitStatus(application, ApplicationStatus.CANCELED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void applicationIdCancelRemovesRunningRecoveredHaApplication() throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitProgress(application, specification.getJobId(), -1);
                String[] before = probe(application, specification.getJobId());
                List<V1Pod> masters = rolePods(application, "master");
                V1Pod active =
                        masters.stream()
                                .filter(pod -> before[3].equals(pod.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                List<String> survivingUids =
                        masters.stream()
                                .filter(
                                        pod ->
                                                !pod.getMetadata()
                                                        .getUid()
                                                        .equals(active.getMetadata().getUid()))
                                .map(pod -> pod.getMetadata().getUid())
                                .collect(Collectors.toList());
                String name = active.getMetadata().getName();
                String uid = active.getMetadata().getUid();
                LOG.info(
                        "HA application-ID cancel fault active application={} job={} pod={}",
                        application.getClusterId(),
                        specification.getJobId(),
                        name);
                core.deleteNamespacedPod(name, namespace, null, null, 0, null, "Background", null);
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () -> {
                                    V1Pod replacement =
                                            core.readNamespacedPod(name, namespace, null);
                                    assertNotEquals(uid, replacement.getMetadata().getUid());
                                    assertEquals("Running", replacement.getStatus().getPhase());
                                    String[] state = probe(application, specification.getJobId());
                                    assertEquals("RUNNING", state[0]);
                                    assertTrue(
                                            Arrays.asList(state[4].split(","))
                                                    .contains(replacement.getStatus().getPodIP()));
                                    V1Pod promoted =
                                            rolePods(application, "master").stream()
                                                    .filter(
                                                            pod ->
                                                                    state[3].equals(
                                                                            pod.getStatus()
                                                                                    .getPodIP()))
                                                    .findFirst()
                                                    .get();
                                    assertTrue(
                                            survivingUids.contains(
                                                    promoted.getMetadata().getUid()));
                                });
                long baseline = Long.parseLong(probe(application, specification.getJobId())[1]);
                awaitProgress(application, specification.getJobId(), baseline);
                assertEquals(
                        ApplicationStatus.RUNNING,
                        deployer.getApplicationStatus(application.getClusterId()));
                deployer.cancelApplication(application.getClusterId());
                awaitHaClean(application);
                assertUserResourcesPreserved();
                // A fresh status request has no retained handle after explicit owner deletion.
                assertEquals(
                        ApplicationStatus.UNKNOWN,
                        deployer.getApplicationStatus(application.getClusterId()));
                deployer.cancelApplication(application.getClusterId());
                LOG.info(
                        "HA application-ID cancel verified application={} job={} after active-master failover",
                        application.getClusterId(),
                        specification.getJobId());
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void unfinishedBatchHasExactOutputAfterActiveMasterLoss() throws Exception {
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        V1Role original = rbac.readNamespacedRole("application", namespace, null);
        V1Role restricted = rbac.readNamespacedRole("application", namespace, null);
        restricted.getRules().stream()
                .filter(rule -> rule.getApiGroups().contains("apps"))
                .forEach(rule -> rule.setVerbs(Collections.singletonList("get")));
        Map<String, String> options = new HashMap<>(haOptions());
        ApplicationSpecification identity =
                SeatunnelApplicationConfig.parse(scenarioFile("ha", "job.conf").toPath(), options);
        options.put("application.job-id", Long.toString(identity.getJobId()));
        String output = "/opt/seatunnel/checkpoints/ha-output/" + identity.getJobId();
        String job = gatedJob(output);
        ApplicationSpecification specification = SeatunnelApplicationConfig.parse(job, options);
        try (KubernetesApplicationClient application = deployApplication(specification, options)) {
            try {
                // Keep the fixed worker alive at terminal status to inspect its local sink output.
                // Workers deliberately do not inherit the checkpoint PVC.
                rbac.replaceNamespacedRole(
                        "application", namespace, restricted, null, null, null, null);
                String[] before = awaitProgress(application, specification.getJobId(), -1);
                V1Pod worker = workers(application).get(0);
                String workerMember = memberUid(before, worker);
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .untilAsserted(() -> assertTrue(committedRows(worker, output) > 0));
                awaitGate(worker);
                long committed = committedRows(worker, output);
                assertTrue(
                        committed > 0 && committed < 240,
                        "Active-master loss must occur with unfinished input: " + committed);
                V1Pod active =
                        rolePods(application, "master").stream()
                                .filter(pod -> before[3].equals(pod.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                core.deleteNamespacedPod(
                        active.getMetadata().getName(),
                        namespace,
                        null,
                        null,
                        0,
                        null,
                        "Background",
                        null);
                Awaitility.await()
                        .atMost(120, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () ->
                                        assertNotEquals(
                                                before[3],
                                                probe(application, specification.getJobId())[3]));
                long promotedCheckpoint =
                        Long.parseLong(probe(application, specification.getJobId())[1]);
                String[] after =
                        awaitProgress(application, specification.getJobId(), promotedCheckpoint);
                assertNotEquals(before[3], after[3], "The surviving master must take over");
                assertFixedWorker(application, worker, workerMember, after);
                workerCommand(worker, "touch " + KubernetesGatedSequenceSource.RELEASE);
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                String actual =
                        workerCommand(
                                        worker,
                                        "find "
                                                + output
                                                + " -type f ! -name '.*' -exec awk '1' {} + | sort -n")
                                .trim();
                String expected = sequence(0, 240);
                assertEquals(expected, actual, "Committed marker multiset must be exact");
                LOG.info(
                        "HA unfinished batch verified application={} committedBeforeFault={} exactRows=240 unchangedWorker={}",
                        application.getClusterId(),
                        committed,
                        worker.getMetadata().getUid());
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                cancelAndAwait(application);
            }
        }
    }

    private static String gatedJob(String output) {
        return "env { job.mode = BATCH, parallelism = 1, checkpoint.interval = 1000 }\n"
                + "source { KubernetesGatedSequence {} }\n"
                + "sink { LocalFile { path = \""
                + output
                + "\", tmp_path = \"/tmp/ha-gated-staging\", file_format_type = text, row_delimiter = \"\\n\", "
                + "file_name_expression = \"${transactionId}_${now}\", is_enable_transaction = true } }";
    }

    private static void awaitGate(V1Pod worker) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertEquals(
                                        "120",
                                        workerCommand(
                                                        worker,
                                                        "cat "
                                                                + KubernetesGatedSequenceSource
                                                                        .REACHED)
                                                .trim()));
    }

    private static String sequence(int start, int end) {
        return IntStream.range(start, end)
                .mapToObj(Integer::toString)
                .collect(Collectors.joining("\n"));
    }

    private static long committedRows(V1Pod worker, String output) throws Exception {
        return Long.parseLong(
                workerCommand(
                                worker,
                                "find "
                                        + output
                                        + " -type f ! -name '.*' -exec awk 'END {print NR}' {} + | awk '{n+=$1} END {print n+0}'")
                        .trim());
    }

    private static String workerCommand(V1Pod worker, String command) throws Exception {
        return exec(worker, new String[] {"bash", "-c", command});
    }

    private static String memberUid(String[] state, V1Pod worker) {
        int index = Arrays.asList(state[4].split(",")).indexOf(worker.getStatus().getPodIP());
        assertTrue(index >= 0, "Fixed worker must be a native cluster member");
        return state[5].split(",")[index];
    }

    private static void assertFixedWorker(
            KubernetesApplicationClient application,
            V1Pod original,
            String nativeUid,
            String[] state)
            throws Exception {
        List<V1Pod> current = workers(application);
        assertEquals(1, current.size());
        assertEquals(original.getMetadata().getUid(), current.get(0).getMetadata().getUid());
        assertEquals(nativeUid, memberUid(state, current.get(0)));
    }

    @Test
    void completedBatchPassesAssertionsAndReleasesAllOwnedResources() throws Exception {
        ApplicationSpecification specification = haSpecification("batch", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void workerLossFailsApplicationWithoutReplacingTheWorker() throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitProgress(application, specification.getJobId(), -1);
                V1Pod worker = workers(application).get(0);
                core.deleteNamespacedPod(
                        worker.getMetadata().getName(),
                        namespace,
                        null,
                        null,
                        0,
                        null,
                        "Background",
                        null);
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .pollInterval(1, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    assertTrue(
                                            workers(application).stream()
                                                    .allMatch(
                                                            pod ->
                                                                    worker.getMetadata()
                                                                            .getUid()
                                                                            .equals(
                                                                                    pod.getMetadata()
                                                                                            .getUid())),
                                            "The deployment must not replace a lost fixed worker");
                                    assertEquals(ApplicationStatus.FAILED, application.getStatus());
                                });
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void acceptedSinkFailureReleasesComputeAndPreservesUserResources() throws Exception {
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        V1Role original = rbac.readNamespacedRole("application", namespace, null);
        V1Role restricted = rbac.readNamespacedRole("application", namespace, null);
        restricted.getRules().stream()
                .filter(rule -> rule.getApiGroups().contains("apps"))
                .forEach(rule -> rule.setVerbs(Collections.singletonList("get")));
        ApplicationSpecification specification = haSpecification("failed", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                rbac.replaceNamespacedRole(
                        "application", namespace, restricted, null, null, null, null);
                awaitStatus(application, ApplicationStatus.FAILED);
                assertEquals(
                        "FAILED",
                        batch.readNamespacedJob(application.getClusterId(), namespace, null)
                                .getMetadata()
                                .getAnnotations()
                                .get("seatunnel.apache.org/application-status"));
                assertTrue(
                        workers(application).stream()
                                .anyMatch(
                                        pod -> {
                                            try {
                                                String log =
                                                        core.readNamespacedPodLog(
                                                                pod.getMetadata().getName(),
                                                                namespace,
                                                                null,
                                                                null,
                                                                null,
                                                                null,
                                                                null,
                                                                null,
                                                                null,
                                                                null,
                                                                null);
                                                return log != null
                                                        && log.contains("fail rule:")
                                                        && log.contains("MIN_LENGTH");
                                            } catch (Exception error) {
                                                throw new IllegalStateException(error);
                                            }
                                        }),
                        "The Assert sink must fail its configured MIN_LENGTH rule");
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void terminalOwnerRetriesCleanupAfterApiPermissionReturns() throws Exception {
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        V1Role original = rbac.readNamespacedRole("application", namespace, null);
        V1Role restricted = rbac.readNamespacedRole("application", namespace, null);
        restricted.getRules().stream()
                .filter(rule -> rule.getApiGroups().contains("apps"))
                .forEach(rule -> rule.setVerbs(Collections.singletonList("get")));
        ApplicationSpecification specification = haSpecification("batch", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                rbac.replaceNamespacedRole(
                        "application", namespace, restricted, null, null, null, null);
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                assertEquals(
                        ApplicationStatus.SUCCEEDED,
                        deployer.getApplicationStatus(application.getClusterId()));
                assertEquals(2, rolePods(application, "master").size());
                assertFalse(
                        missing(
                                () ->
                                        core.readNamespacedSecret(
                                                application.getClusterId(), namespace, null)));
                assertFalse(
                        missing(
                                () ->
                                        core.readNamespacedConfigMap(
                                                application.getClusterId() + "-owned",
                                                namespace,
                                                null)));
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                original.getMetadata()
                        .setResourceVersion(
                                rbac.readNamespacedRole("application", namespace, null)
                                        .getMetadata()
                                        .getResourceVersion());
                rbac.replaceNamespacedRole(
                        "application", namespace, original, null, null, null, null);
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void explicitCheckpointRestoreUsesANewNativeJobIdentity() throws Exception {
        ApplicationSpecification original = haSpecification(null, null);
        long sourceCheckpoint;
        try (KubernetesApplicationClient first = deployApplication(original, "ha")) {
            try {
                sourceCheckpoint = Long.parseLong(awaitProgress(first, original.getJobId(), -1)[1]);
                probeCommand(first, original.getJobId(), "cancel");
                awaitStatus(first, ApplicationStatus.CANCELED);
                awaitHaClean(first);
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(first, null);
                throw failure;
            } finally {
                cancelAndAwait(first);
            }
        }
        ApplicationSpecification restored = haSpecification(null, original.getJobId());
        assertNotEquals(original.getJobId(), restored.getJobId());
        try (KubernetesApplicationClient second = deployApplication(restored, "ha")) {
            try {
                awaitProgress(second, restored.getJobId(), -1);
                Pattern restoredCheckpoint =
                        Pattern.compile(
                                "Restore checkpoint, job id: "
                                        + restored.getJobId()
                                        + ", pipeline id: [0-9]+, checkpoint id: ([0-9]+)");
                long loadedCheckpoint = -1;
                for (V1Pod master : rolePods(second, "master")) {
                    Matcher restoredLog =
                            restoredCheckpoint.matcher(
                                    retainedPodLogs(master.getMetadata().getName()));
                    while (restoredLog.find()) {
                        loadedCheckpoint =
                                Math.max(loadedCheckpoint, Long.parseLong(restoredLog.group(1)));
                    }
                }
                assertTrue(
                        loadedCheckpoint >= sourceCheckpoint,
                        "The new native job must load the retained source checkpoint");
                String[] progress = awaitProgress(second, restored.getJobId(), loadedCheckpoint);
                assertEquals(Long.toString(restored.getJobId()), progress[6]);
                probeCommand(second, restored.getJobId(), "cancel");
                awaitStatus(second, ApplicationStatus.CANCELED);
                awaitHaClean(second);
                assertUserResourcesPreserved();
                LOG.info(
                        "HA explicit checkpoint restore verified source={} destination={}",
                        original.getJobId(),
                        restored.getJobId());
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(second, null);
                throw failure;
            } finally {
                cancelAndAwait(second);
            }
        }
    }

    @Test
    void missingRestoreCheckpointFailsAndCleansResources() throws Exception {
        ApplicationSpecification specification = haSpecification("batch", 9223372036854775000L);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application, null);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    private static Map<String, String> haOptions() throws Exception {
        return SeatunnelApplicationConfig.load(
                scenarioFile("ha", "application.config").toPath(), Collections.emptyMap());
    }

    private static ApplicationSpecification haSpecification(String sinkMode, Long restore)
            throws Exception {
        Map<String, String> options = new HashMap<>(haOptions());
        if (restore != null) {
            options.put("application.restore-job-id", restore.toString());
        }
        if (sinkMode == null) {
            return SeatunnelApplicationConfig.parse(
                    scenarioFile("ha", "job.conf").toPath(), options);
        }
        String job = Files.readString(scenarioFile("batch", "job.conf").toPath());
        if (sinkMode.equals("failed")) {
            job =
                    job.replace("job.mode = BATCH", "job.mode = BATCH\n  job.retry.times = 0")
                            .replace(
                                    "rule_type = NOT_NULL",
                                    "rule_type = MIN_LENGTH\n              rule_value = 1000");
        }
        org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory.parseString(job);
        return SeatunnelApplicationConfig.parse(job, options);
    }

    private static List<V1Pod> rolePods(KubernetesApplicationClient application, String role)
            throws Exception {
        return core.listNamespacedPod(
                        namespace,
                        null,
                        null,
                        null,
                        null,
                        APPLICATION_LABEL
                                + "="
                                + application.getClusterId()
                                + ","
                                + ROLE_LABEL
                                + "="
                                + role,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null)
                .getItems();
    }

    private static String[] probe(KubernetesApplicationClient application, long job)
            throws Exception {
        return Arrays.stream(probeCommand(application, job, "status").split("\n"))
                .filter(line -> line.startsWith("HA_PROBE "))
                .findFirst()
                .get()
                .substring("HA_PROBE ".length())
                .trim()
                .split(" ");
    }

    private static String probeCommand(
            KubernetesApplicationClient application, long job, String action) throws Exception {
        V1Pod inspector = core.readNamespacedPod(INSPECTOR, namespace, null);
        return exec(
                inspector,
                new String[] {
                    "java",
                    "-Xmx128m",
                    "--add-opens=java.base/java.lang=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "-Dseatunnel.home=/opt/seatunnel",
                    "-cp",
                    "/opt/seatunnel/test-probe:/opt/seatunnel/starter/*:/opt/seatunnel/lib/*",
                    KubernetesApplicationProbe.class.getName(),
                    application.getClusterId(),
                    Long.toString(job),
                    "5801",
                    action
                });
    }

    private static String[] awaitProgress(
            KubernetesApplicationClient application, long job, long previous) {
        AtomicReference<String[]> observed = new AtomicReference<>();
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            String[] state = probe(application, job);
                            assertEquals("RUNNING", state[0]);
                            assertEquals(Long.toString(job), state[6]);
                            assertTrue(
                                    Long.parseLong(state[1]) > previous,
                                    "Fresh native completed checkpoint required");
                            observed.set(state);
                        });
        return observed.get();
    }

    private static String inspect(String command) throws Exception {
        return exec(
                core.readNamespacedPod(INSPECTOR, namespace, null),
                new String[] {"bash", "-c", command});
    }

    private static String exec(V1Pod pod, String[] command) throws Exception {
        Process process = new Exec(apiClient).exec(pod, command, "seatunnel", false, false);
        // Drain both streams concurrently so a verbose native client cannot fill its pipes.
        ExecutorService outputReaders = Executors.newFixedThreadPool(2);
        try {
            InputStream stdoutStream = process.getInputStream();
            InputStream stderrStream = process.getErrorStream();
            Future<byte[]> captured = outputReaders.submit(stdoutStream::readAllBytes);
            Future<byte[]> errors = outputReaders.submit(stderrStream::readAllBytes);
            assertTrue(process.waitFor(45, TimeUnit.SECONDS), "In-cluster command timed out");
            byte[] output = captured.get(5, TimeUnit.SECONDS);
            byte[] stderr = errors.get(5, TimeUnit.SECONDS);
            assertEquals(
                    0,
                    process.exitValue(),
                    new String(output, StandardCharsets.UTF_8)
                            + new String(stderr, StandardCharsets.UTF_8));
            return new String(output, StandardCharsets.UTF_8);
        } finally {
            outputReaders.shutdownNow();
            process.destroy();
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }

    private static void awaitHaClean(KubernetesApplicationClient application) throws Exception {
        // Dependent workload controllers and their Pods can disappear in different GC cycles.
        AppsV1Api apps = new AppsV1Api(apiClient);
        Set<String> oomOwners = new HashSet<>();
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // Keep observed failures even after the Job's Pods are
                            // garbage-collected.
                            for (V1Pod owner : rolePods(application, "owner")) {
                                if (owner.getStatus() == null
                                        || owner.getStatus().getContainerStatuses() == null) {
                                    continue;
                                }
                                owner.getStatus()
                                        .getContainerStatuses()
                                        .forEach(
                                                status -> {
                                                    if (status.getState() != null
                                                            && status.getState().getTerminated()
                                                                    != null
                                                            && "OOMKilled"
                                                                    .equals(
                                                                            status.getState()
                                                                                    .getTerminated()
                                                                                    .getReason())) {
                                                        oomOwners.add(
                                                                owner.getMetadata().getName());
                                                    }
                                                });
                            }
                            assertAllResourcesRemoved(application);
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedConfigMap(
                                                            application.getClusterId() + "-owned",
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    apps.readNamespacedStatefulSet(
                                                            application.getClusterId() + "-master",
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    apps.readNamespacedStatefulSet(
                                                            application.getClusterId() + "-worker",
                                                            namespace,
                                                            null)));
                            assertTrue(
                                    missing(
                                            () ->
                                                    core.readNamespacedService(
                                                            application.getClusterId()
                                                                    + "-discovery",
                                                            namespace,
                                                            null)));
                        });
        assertTrue(oomOwners.isEmpty(), "HA client owners OOM-killed during cleanup: " + oomOwners);
        LOG.info("HA cleanup verified application={}", application.getClusterId());
    }

    private void assertUserResourcesPreserved() throws Exception {
        assertEquals(
                pvcUid,
                core.readNamespacedPersistentVolumeClaim(CLAIM, namespace, null)
                        .getMetadata()
                        .getUid());
        assertEquals(
                secretUid,
                core.readNamespacedSecret("ha-user-secret", namespace, null)
                        .getMetadata()
                        .getUid());
        assertEquals(
                runtimeUid,
                core.readNamespacedConfigMap(RUNTIME_CONFIG_MAP, namespace, null)
                        .getMetadata()
                        .getUid());
        assertEquals("preserve-user-file", inspect("cat /checkpoints/ha-user-sentinel"));
    }
}
