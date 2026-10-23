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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.util.Yaml;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full provider/launcher path: controller replacement, native recovery, restoration and cleanup.
 */
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

        if (MULTI_NODE) {
            inspector
                    .getSpec()
                    .setNodeSelector(Collections.singletonMap("seatunnel.e2e/control", "true"));
        }
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

    @Test
    void dualMastersAndOwnerRecoverWithoutResubmissionThenCheckpointRestores() throws Exception {
        int masters = 2;
        ApplicationSpecification first = haSpecification(null, null);
        Map<String, String> options = new HashMap<>(haOptions());
        options.put(KubernetesOptions.MASTER_REPLICAS.key(), Integer.toString(masters));
        try (KubernetesApplicationClient application = deployApplication(first, options)) {
            try {
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    assertEquals(masters, rolePods(application, "master").size());
                                    assertEquals(1, workers(application).size());
                                    assertTrue(
                                            workers(application).stream()
                                                    .allMatch(
                                                            pod ->
                                                                    "Running"
                                                                            .equals(
                                                                                    pod.getStatus()
                                                                                            .getPhase())));
                                });
                String[] initial = awaitProgress(application, first.getJobId(), -1);
                V1Pod fixedWorker = workers(application).get(0);
                String workerMember = memberUid(initial, fixedWorker);
                String serviceUid =
                        core.readNamespacedService(application.getClusterId(), namespace, null)
                                .getMetadata()
                                .getUid();
                String serviceIp =
                        core.readNamespacedService(application.getClusterId(), namespace, null)
                                .getSpec()
                                .getClusterIP();
                assertNotEquals("None", serviceIp);
                java.util.concurrent.CompletableFuture<String> persistent =
                        java.util.concurrent.CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return probeCommand(
                                                application, first.getJobId(), "persistent");
                                    } catch (Exception e) {
                                        throw new java.util.concurrent.CompletionException(e);
                                    }
                                });
                String clientPrefix = "/checkpoints/ha-client-" + first.getJobId();
                Awaitility.await()
                        .atMost(30, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                inspect(
                                                                "cat "
                                                                        + clientPrefix
                                                                        + ".state 2>/dev/null || true")
                                                        .contains("READY")));
                for (String role : Arrays.asList("standby", "active", "owner")) {
                    String[] before = probe(application, first.getJobId());
                    List<V1Pod> candidates =
                            rolePods(application, role.equals("owner") ? "owner" : "master");
                    V1Pod victim =
                            candidates.stream()
                                    .filter(
                                            pod ->
                                                    role.equals("owner")
                                                            || role.equals("active")
                                                                    == before[3].equals(
                                                                            pod.getStatus()
                                                                                    .getPodIP()))
                                    .findFirst()
                                    .get();
                    Map<String, String> survivingMasters = new HashMap<>();
                    for (V1Pod master : rolePods(application, "master")) {
                        if (!master.getMetadata().getUid().equals(victim.getMetadata().getUid())) {
                            survivingMasters.put(
                                    master.getStatus().getPodIP(), master.getMetadata().getUid());
                        }
                    }
                    List<String> previousIps = Arrays.asList(before[4].split(","));
                    String previousNativeUid =
                            role.equals("owner")
                                    ? "observer"
                                    : before[5]
                                            .split(",")[
                                            previousIps.indexOf(victim.getStatus().getPodIP())];
                    LOG.info(
                            "HA fault {} application={} job={} pod={} before={}",
                            role,
                            application.getClusterId(),
                            first.getJobId(),
                            victim.getMetadata().getName(),
                            Arrays.toString(before));
                    String uid = victim.getMetadata().getUid();
                    String name = victim.getMetadata().getName();
                    core.deleteNamespacedPod(
                            name, namespace, null, null, 0, null, "Background", null);
                    Awaitility.await()
                            .atMost(180, TimeUnit.SECONDS)
                            .untilAsserted(
                                    () -> {
                                        List<V1Pod> replacements =
                                                rolePods(
                                                        application,
                                                        role.equals("standby")
                                                                        || role.equals("active")
                                                                ? "master"
                                                                : role);
                                        assertTrue(
                                                replacements.stream()
                                                        .anyMatch(
                                                                pod ->
                                                                        !uid.equals(
                                                                                        pod.getMetadata()
                                                                                                .getUid())
                                                                                && "Running"
                                                                                        .equals(
                                                                                                pod.getStatus()
                                                                                                        .getPhase())
                                                                                && (role.equals(
                                                                                                "owner")
                                                                                        || name
                                                                                                .equals(
                                                                                                        pod.getMetadata()
                                                                                                                .getName()))));
                                    });
                    if (!role.equals("owner")) {
                        Awaitility.await()
                                .atMost(180, TimeUnit.SECONDS)
                                .ignoreExceptions()
                                .untilAsserted(
                                        () -> {
                                            String[] state = probe(application, first.getJobId());
                                            String replacementIp =
                                                    core.readNamespacedPod(name, namespace, null)
                                                            .getStatus()
                                                            .getPodIP();
                                            List<String> currentIps =
                                                    Arrays.asList(state[4].split(","));
                                            assertTrue(
                                                    currentIps.contains(replacementIp),
                                                    "Replacement must join the native cluster");
                                            assertNotEquals(
                                                    previousNativeUid,
                                                    state[5].split(",")[
                                                            currentIps.indexOf(replacementIp)],
                                                    "A new native member must replace the deleted process");
                                        });
                    }
                    // Baseline after replacement membership, so pre-fault progress cannot pass.
                    long recoveredCheckpoint =
                            Long.parseLong(probe(application, first.getJobId())[1]);
                    awaitProgress(application, first.getJobId(), recoveredCheckpoint);
                    String[] after = probe(application, first.getJobId());
                    LOG.info(
                            "HA recovered {} application={} job={} after={}",
                            role,
                            application.getClusterId(),
                            first.getJobId(),
                            Arrays.toString(after));
                    assertEquals("RUNNING", after[0]);
                    assertFixedWorker(application, fixedWorker, workerMember, after);
                    if (role.equals("active")) {
                        assertTrue(
                                survivingMasters.containsKey(after[3]),
                                "An original standby must become active");
                        V1Pod promoted =
                                rolePods(application, "master").stream()
                                        .filter(pod -> after[3].equals(pod.getStatus().getPodIP()))
                                        .findFirst()
                                        .get();
                        assertEquals(
                                survivingMasters.get(after[3]), promoted.getMetadata().getUid());
                    }
                    if (role.equals("standby")) {
                        assertEquals(
                                before[2],
                                after[2],
                                "Standby loss must not restore a running pipeline");
                    }
                    assertEquals(
                            serviceUid,
                            core.readNamespacedService(application.getClusterId(), namespace, null)
                                    .getMetadata()
                                    .getUid());
                    assertEquals(
                            serviceIp,
                            core.readNamespacedService(application.getClusterId(), namespace, null)
                                    .getSpec()
                                    .getClusterIP());
                    assertEquals(
                            ApplicationStatus.RUNNING,
                            deployer.getApplicationStatus(application.getClusterId()));
                    assertEquals(
                            Long.toString(first.getJobId()),
                            batch.readNamespacedJob(application.getClusterId(), namespace, null)
                                    .getMetadata()
                                    .getAnnotations()
                                    .get("seatunnel.apache.org/job-id"));
                }
                inspect("printf cancel > " + clientPrefix + ".command");
                String persistentLog = persistent.get(60, TimeUnit.SECONDS);
                assertTrue(persistentLog.contains("HA_PERSISTENT_RECOVERED"), persistentLog);
                assertTrue(persistentLog.contains("HA_PERSISTENT_CANCELED"), persistentLog);
                LOG.info(
                        "HA persistent client verified masters={} application={} {}",
                        masters,
                        application.getClusterId(),
                        persistentLog);

                awaitStatus(application, ApplicationStatus.CANCELED);
                assertEquals(ApplicationStatus.CANCELED, application.getStatus());
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
        ApplicationSpecification restored = haSpecification(null, first.getJobId());
        assertNotEquals(first.getJobId(), restored.getJobId());
        try (KubernetesApplicationClient application = deployApplication(restored, options)) {
            try {
                awaitProgress(application, restored.getJobId(), -1);
                List<V1Pod> restoredWorkers = workers(application);
                assertEquals(1, restoredWorkers.size());
                for (V1Pod worker : restoredWorkers) {
                    Awaitility.await()
                            .atMost(30, TimeUnit.SECONDS)
                            .untilAsserted(
                                    () -> {
                                        String logs =
                                                livePodLogs(
                                                        application,
                                                        worker.getMetadata().getName());
                                        assertFalse(
                                                logs.isEmpty(),
                                                "Restored worker logs must be readable");
                                        assertFalse(
                                                logs.contains("APPLICATION_E2E_DATA"),
                                                "Cold checkpoint restore must not replay the already committed marker");
                                    });
                }
                probeCommand(application, restored.getJobId(), "cancel");
                awaitStatus(application, ApplicationStatus.CANCELED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application);
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
                                .collect(java.util.stream.Collectors.toList());
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
                recordApplicationDiagnostics(application);
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
        String job =
                "env { job.mode = BATCH, parallelism = 1, checkpoint.interval = 1000 }\n"
                        + "source { FakeSource { row.num = 240, split.num = 120, "
                        + "schema { fields { value = string } }, "
                        + "rows = [{ kind = INSERT, fields = [\"MARKER_A\"] }, "
                        + "{ kind = INSERT, fields = [\"MARKER_B\"] }] } }\n"
                        + "sink { LocalFile { path = \""
                        + output
                        + "\", tmp_path = \"/checkpoints/ha-staging/"
                        + identity.getJobId()
                        + "\", file_format_type = text, row_delimiter = \"\\n\", "
                        + "file_name_expression = \"${transactionId}_${now}\", "
                        + "is_enable_transaction = true } }";
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
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                String actual =
                        workerCommand(
                                        worker,
                                        "find "
                                                + output
                                                + " -type f ! -name '.*' -exec awk '1' {} + | sort")
                                .trim();
                String expected =
                        java.util.stream.Stream.concat(
                                        Collections.nCopies(120, "MARKER_A").stream(),
                                        Collections.nCopies(120, "MARKER_B").stream())
                                .collect(java.util.stream.Collectors.joining("\n"));
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
                recordApplicationDiagnostics(application);
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
                recordApplicationDiagnostics(application);
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
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application);
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
                recordApplicationDiagnostics(application);
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
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                rolePods(application, "owner").stream()
                                                        .anyMatch(
                                                                pod ->
                                                                        "Failed"
                                                                                .equals(
                                                                                        pod.getStatus()
                                                                                                .getPhase()))));
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
                AppsV1Api apps = new AppsV1Api(apiClient);
                for (String role : Collections.singletonList("master")) {
                    String name = application.getClusterId() + "-" + role;
                    io.kubernetes.client.openapi.models.V1StatefulSet pool =
                            apps.readNamespacedStatefulSet(name, namespace, null);
                    pool.getSpec().setReplicas(0);
                    apps.replaceNamespacedStatefulSet(
                            name, namespace, pool, null, null, null, null);
                }
                for (V1Pod worker : workers(application)) {
                    core.deleteNamespacedPod(
                            worker.getMetadata().getName(),
                            namespace,
                            null,
                            null,
                            0,
                            null,
                            "Background",
                            null);
                }
                Awaitility.await()
                        .atMost(90, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    assertTrue(rolePods(application, "master").isEmpty());
                                    assertTrue(rolePods(application, "worker").isEmpty());
                                });
                LOG.info(
                        "HA terminal replay has no native endpoint application={}",
                        application.getClusterId());
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
                recordApplicationDiagnostics(application);
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
    void missingRestoreCheckpointFailsAndCleansResources() throws Exception {
        ApplicationSpecification specification = haSpecification("batch", 9223372036854775000L);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void checkpointStorageOutageRecoversWithoutResubmission() throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployRemoteApplication(specification)) {
            try {
                awaitProgress(application, specification.getJobId(), 3);
                String[] before = probe(application, specification.getJobId());
                V1Pod active =
                        rolePods(application, "master").stream()
                                .filter(pod -> before[3].equals(pod.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                java.nio.file.Path evidence = java.nio.file.Paths.get("target", "failsafe-reports");
                java.nio.file.Files.writeString(
                        evidence.resolve(application.getClusterId() + "-before-storage-cut.log"),
                        livePodLogs(application, active.getMetadata().getName()));
                storageAvailable(false);
                LOG.info(
                        "HA storage unavailable application={} before={}",
                        application.getClusterId(),
                        Arrays.toString(before));
                // A checkpoint persisted before the pause may publish its completion later.
                // Observe an actual failed storage attempt before starting the stall window.
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                livePodLogs(
                                                                application,
                                                                active.getMetadata().getName())
                                                        .contains(
                                                                "Failed to write checkpoint data")));
                String stalledLogs = livePodLogs(application, active.getMetadata().getName());
                long stalled = lastNotifiedCheckpoint(stalledLogs, specification.getJobId());
                assertTrue(
                        stalled >= Long.parseLong(before[1]),
                        "Storage evidence must include the known completed checkpoint");
                TimeUnit.SECONDS.sleep(8);
                String laterLogs = livePodLogs(application, active.getMetadata().getName());
                java.nio.file.Files.writeString(
                        evidence.resolve(application.getClusterId() + "-storage-paused.log"),
                        laterLogs);
                assertEquals(
                        stalled,
                        lastNotifiedCheckpoint(laterLogs, specification.getJobId()),
                        "Unavailable storage must not publish a new durable checkpoint completion");
                assertEquals(ApplicationStatus.RUNNING, application.getStatus());
                storageAvailable(true);
                long baseline = Long.parseLong(probe(application, specification.getJobId())[1]);
                awaitProgress(application, specification.getJobId(), baseline);
                assertTrue(storedCheckpoint(specification.getJobId()) > baseline);
                LOG.info(
                        "HA storage recovered application={} after={}",
                        application.getClusterId(),
                        Arrays.toString(probe(application, specification.getJobId())));
                application.cancel();
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                storageAvailable(true);
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                storageAvailable(true);
                cancelAndAwait(application);
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void masterFailoverDuringStorageOutageRetainsRunningTasks() throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployRemoteApplication(specification)) {
            try {
                awaitProgress(application, specification.getJobId(), 3);
                storedCheckpoint(specification.getJobId());
                String[] before = probe(application, specification.getJobId());
                List<V1Pod> masters = rolePods(application, "master");
                V1Pod active =
                        masters.stream()
                                .filter(pod -> before[3].equals(pod.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                V1Pod survivor =
                        masters.stream()
                                .filter(
                                        pod ->
                                                !pod.getMetadata()
                                                        .getUid()
                                                        .equals(active.getMetadata().getUid()))
                                .findFirst()
                                .get();
                V1Pod fixedWorker = workers(application).get(0);
                String fixedMember = memberUid(before, fixedWorker);
                storageAvailable(false);
                Awaitility.await()
                        .atMost(60, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                livePodLogs(
                                                                application,
                                                                active.getMetadata().getName())
                                                        .contains(
                                                                "Failed to write checkpoint data")));
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
                        .atMost(180, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () -> {
                                    String view =
                                            localProbe(
                                                    survivor, specification.getJobId(), "members");
                                    String memberLine =
                                            Arrays.stream(view.split("\n"))
                                                    .filter(line -> line.startsWith("HA_MEMBERS "))
                                                    .findFirst()
                                                    .get();
                                    assertNotEquals(
                                            before[3],
                                            memberLine.split(" ")[2],
                                            "A surviving master must take over before storage returns");
                                });
                TimeUnit.SECONDS.sleep(15);
                storageAvailable(true);
                String[] after =
                        awaitProgress(
                                application, specification.getJobId(), Long.parseLong(before[1]));
                assertTrue(storedCheckpoint(specification.getJobId()) > Long.parseLong(before[1]));
                assertFixedWorker(application, fixedWorker, fixedMember, after);
                int markers = 0;
                for (V1Pod worker : workers(application)) {
                    markers +=
                            livePodLogs(application, worker.getMetadata().getName())
                                            .split("APPLICATION_E2E_DATA", -1)
                                            .length
                                    - 1;
                }
                assertEquals(
                        1, markers, "Recovery must not replay the already checkpointed marker");
                LOG.info(
                        "HA master takeover during storage outage recovered application={} sameJob={} before={} after={}",
                        application.getClusterId(),
                        specification.getJobId(),
                        Arrays.toString(before),
                        Arrays.toString(after));
                application.cancel();
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                storageAvailable(true);
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                storageAvailable(true);
                cancelAndAwait(application);
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void cancellationDuringStorageOutageRemovesOwnedResources() throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployRemoteApplication(specification)) {
            try {
                awaitProgress(application, specification.getJobId(), 3);
                storedCheckpoint(specification.getJobId());
                storageAvailable(false);
                TimeUnit.SECONDS.sleep(10);
                application.cancel();
                awaitAllResourcesRemoved(application);
                storageAvailable(true);
                storedCheckpoint(specification.getJobId());
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                storageAvailable(true);
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                storageAvailable(true);
                cancelAndAwait(application);
            }
        }
    }

    private static long lastNotifiedCheckpoint(String logs, long job) {
        java.util.regex.Matcher matcher =
                java.util.regex.Pattern.compile(
                                "pending checkpoint notify finished, job id: "
                                        + job
                                        + ", pipeline id: [0-9]+, checkpoint id: ([0-9]+)!")
                        .matcher(logs);
        long latest = -1;
        while (matcher.find()) {
            latest = Math.max(latest, Long.parseLong(matcher.group(1)));
        }
        return latest;
    }

    private static String localProbe(V1Pod pod, long job, String action) throws Exception {
        return localProbe(pod, job, action, "127.0.0.1");
    }

    private static Map<String, String> remoteOptions() throws Exception {
        Map<String, String> options = new HashMap<>(haOptions());
        options.remove(KubernetesOptions.CHECKPOINT_PVC.key());
        options.put("kubernetes.master.node-selector", "seatunnel.e2e/compute:true");
        options.put("kubernetes.worker.node-selector", "seatunnel.e2e/control:true");
        return options;
    }

    private static KubernetesApplicationClient deployRemoteApplication(
            ApplicationSpecification specification) throws Exception {
        return deployApplication(specification, remoteOptions());
    }

    private static String localProbe(V1Pod pod, long job, String action, String address)
            throws Exception {
        return exec(
                pod,
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
                    pod.getMetadata().getLabels().get(APPLICATION_LABEL),
                    Long.toString(job),
                    "5801",
                    action,
                    address
                });
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void activeMasterNodeLossKeepsFixedWorkerAndMakesDurableProgress() throws Exception {
        verifyMasterNodeLoss(true);
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void standbyMasterNodeLossKeepsFixedWorkerAndMakesDurableProgress() throws Exception {
        verifyMasterNodeLoss(false);
    }

    private void verifyMasterNodeLoss(boolean loseActive) throws Exception {
        ApplicationSpecification specification = haSpecification(null, null);
        try (KubernetesApplicationClient application = deployRemoteApplication(specification)) {
            String stoppedNode = null;
            Throwable scenarioFailure = null;
            try {
                String[] before = awaitProgress(application, specification.getJobId(), 3);
                List<V1Pod> masters = rolePods(application, "master");
                assertEquals(2, masters.size());
                assertEquals(
                        2L,
                        masters.stream().map(p -> p.getSpec().getNodeName()).distinct().count(),
                        "Both native masters must run on different Kubernetes nodes");
                V1Pod target =
                        masters.stream()
                                .filter(
                                        p ->
                                                loseActive
                                                        == before[3].equals(
                                                                p.getStatus().getPodIP()))
                                .findFirst()
                                .get();
                V1Pod worker = workers(application).get(0);
                assertNotEquals(target.getSpec().getNodeName(), worker.getSpec().getNodeName());
                String fixedMember = memberUid(before, worker);
                String targetAddress = target.getStatus().getPodIP();
                java.util.Set<String> survivingMembers =
                        new java.util.HashSet<>(Arrays.asList(before[5].split(",")));
                int index = Arrays.asList(before[4].split(",")).indexOf(targetAddress);
                assertTrue(index >= 0);
                assertEquals(3, survivingMembers.size());
                assertTrue(survivingMembers.remove(before[5].split(",")[index]));
                stoppedNode = target.getSpec().getNodeName();
                LOG.info(
                        "HA node fault active={} node={} before={}",
                        loseActive,
                        stoppedNode,
                        Arrays.toString(before));
                pauseComputeNode(stoppedNode, true);
                String node = stoppedNode;
                Awaitility.await()
                        .atMost(120, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                core.readNode(node, null).getStatus()
                                                        .getConditions().stream()
                                                        .anyMatch(
                                                                c ->
                                                                        "Ready".equals(c.getType())
                                                                                && !"True"
                                                                                        .equals(
                                                                                                c
                                                                                                        .getStatus()))));
                Awaitility.await()
                        .atMost(180, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () -> {
                                    String[] state = probe(application, specification.getJobId());
                                    assertEquals(
                                            survivingMembers,
                                            new java.util.HashSet<>(
                                                    Arrays.asList(state[5].split(","))));
                                    if (loseActive) {
                                        assertNotEquals(before[3], state[3]);
                                    } else {
                                        assertEquals(before[3], state[3]);
                                    }
                                });
                long baseline = Long.parseLong(probe(application, specification.getJobId())[1]);
                String[] after = awaitProgress(application, specification.getJobId(), baseline);
                assertFixedWorker(application, worker, fixedMember, after);
                assertTrue(storedCheckpoint(specification.getJobId()) > baseline);
                assertEquals(
                        1,
                        livePodLogs(application, worker.getMetadata().getName())
                                        .split("APPLICATION_E2E_DATA", -1)
                                        .length
                                - 1);
                LOG.info(
                        "HA node failover verified while node unavailable active={} node={} after={}",
                        loseActive,
                        node,
                        Arrays.toString(after));
                // Drain surviving compute before thawing the failed node for teardown only.
                // Live partition healing and split-brain protection are outside this scope.
                application.cancel();
                awaitSurvivingComputeRemoved(application, target);
                pauseComputeNode(stoppedNode, false);
                stoppedNode = null;
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                scenarioFailure = failure;
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                Throwable cleanupFailure = null;
                try {
                    application.cancel();
                } catch (Exception | AssertionError failure) {
                    cleanupFailure = failure;
                }
                try {
                    if (stoppedNode != null) {
                        pauseComputeNode(stoppedNode, false);
                    }
                } catch (Exception | AssertionError failure) {
                    if (cleanupFailure == null) {
                        cleanupFailure = failure;
                    } else {
                        cleanupFailure.addSuppressed(failure);
                    }
                }
                try {
                    cancelAndAwait(application);
                } catch (Exception | AssertionError failure) {
                    if (cleanupFailure == null) {
                        cleanupFailure = failure;
                    } else {
                        cleanupFailure.addSuppressed(failure);
                    }
                }
                if (cleanupFailure != null) {
                    if (scenarioFailure != null) {
                        scenarioFailure.addSuppressed(cleanupFailure);
                    } else if (cleanupFailure instanceof Exception) {
                        throw (Exception) cleanupFailure;
                    } else {
                        throw (AssertionError) cleanupFailure;
                    }
                }
            }
        }
    }

    private static void awaitSurvivingComputeRemoved(
            KubernetesApplicationClient application, V1Pod stopped) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(workers(application).isEmpty());
                            assertTrue(
                                    rolePods(application, "master").stream()
                                            .allMatch(
                                                    p ->
                                                            p.getMetadata()
                                                                    .getUid()
                                                                    .equals(
                                                                            stopped.getMetadata()
                                                                                    .getUid())));
                        });
    }

    @Test
    @EnabledIfSystemProperty(named = "seatunnel.kubernetes.e2e.multinode", matches = "true")
    void coldRestoreStorageReadFailsClosedAndExplicitRetryRecovers() throws Exception {
        ApplicationSpecification source = haSpecification(null, null);
        long sourceCheckpoint;
        try (KubernetesApplicationClient application = deployRemoteApplication(source)) {
            try {
                awaitProgress(application, source.getJobId(), 3);
                sourceCheckpoint = storedCheckpoint(source.getJobId());
                String readable = probeCommand(application, source.getJobId(), "checkpoint-read");
                assertTrue(readable.contains("HA_CHECKPOINT_READ_OK " + source.getJobId() + " 1"));
                storageAvailable(false);
                String failed = probeCommand(application, source.getJobId(), "checkpoint-read");
                assertTrue(
                        failed.contains("HA_CHECKPOINT_READ_FAILED " + source.getJobId())
                                && failed.contains("CheckpointStorageException"),
                        failed);
                java.nio.file.Files.writeString(
                        java.nio.file.Paths.get(
                                "target", "failsafe-reports", "storage-read-outage.log"),
                        readable + "\n" + failed);
                application.cancel();
                awaitHaClean(application);
            } finally {
                storageAvailable(true);
                cancelAndAwait(application);
            }
        }
        sourceCheckpoint = storedCheckpoint(source.getJobId());
        ApplicationSpecification blocked = haSpecification(null, source.getJobId());
        RbacAuthorizationV1Api rbac = new RbacAuthorizationV1Api(apiClient);
        V1Role originalRole = rbac.readNamespacedRole("application", namespace, null);
        V1Role guarded = rbac.readNamespacedRole("application", namespace, null);
        guarded.getRules()
                .forEach(
                        rule ->
                                rule.setVerbs(
                                        rule.getVerbs().stream()
                                                .filter(verb -> !verb.startsWith("delete"))
                                                .collect(java.util.stream.Collectors.toList())));
        try {
            rbac.replaceNamespacedRole("application", namespace, guarded, null, null, null, null);
            Map<String, String> blockedOptions = remoteOptions();
            String placement = "seatunnel.e2e/cold-restore";
            blockedOptions.put(
                    "kubernetes.worker.node-selector", placement + ":" + blocked.getJobId());
            try (KubernetesApplicationClient application =
                    deployApplication(blocked, blockedOptions)) {
                try {
                    // Establish native cluster/checkpoint-read readiness with healthy storage,
                    // but keep the worker Pending so submission cannot reach its restore read.
                    Awaitility.await()
                            .atMost(180, TimeUnit.SECONDS)
                            .ignoreExceptions()
                            .untilAsserted(
                                    () -> {
                                        assertTrue(
                                                probeCommand(
                                                                application,
                                                                source.getJobId(),
                                                                "members")
                                                        .contains("HA_MEMBERS 2 "));
                                        List<V1Pod> pending = workers(application);
                                        assertEquals(1, pending.size());
                                        assertEquals(
                                                "Pending", pending.get(0).getStatus().getPhase());
                                    });
                    assertTrue(
                            probeCommand(application, source.getJobId(), "checkpoint-read")
                                    .contains("HA_CHECKPOINT_READ_OK " + source.getJobId() + " 1"));
                    storageAvailable(false);
                    assertEquals(
                            0,
                            execInCluster(
                                            "kubectl",
                                            "label",
                                            "node",
                                            "ha-server",
                                            placement + "=" + blocked.getJobId(),
                                            "--overwrite")
                                    .getExitCode());
                    awaitStatus(application, ApplicationStatus.FAILED);
                    // The terminal annotation can be visible before kubelet exposes the final
                    // application log records. Keep the retained Pods until both are observable.
                    Awaitility.await()
                            .atMost(30, TimeUnit.SECONDS)
                            .untilAsserted(
                                    () -> {
                                        StringBuilder failureLogs = new StringBuilder();
                                        for (V1Pod master : rolePods(application, "master")) {
                                            failureLogs.append(
                                                    retainedPodLogs(
                                                            master.getMetadata().getName()));
                                        }
                                        String failure = failureLogs.toString();
                                        assertTrue(
                                                failure.contains("CheckpointStorageException")
                                                        && (failure.contains("Failed to list files")
                                                                || failure.contains(
                                                                        "Failed to read checkpoint")),
                                                failure);
                                        assertTrue(
                                                failure.contains(Long.toString(source.getJobId())),
                                                "Storage failure must identify the requested source checkpoint job");
                                    });
                    assertNoSourceReplay(
                            application, "Failed cold read must not run an empty source state");
                    recordApplicationDiagnostics(application);
                } catch (Exception | AssertionError failure) {
                    recordApplicationDiagnostics(application, null);
                    throw failure;
                } finally {
                    assertEquals(
                            0,
                            execInCluster("kubectl", "label", "node", "ha-server", placement + "-")
                                    .getExitCode());
                    restoreApplicationRole(rbac, originalRole);
                    awaitHaClean(application);
                    storageAvailable(true);
                    cancelAndAwait(application);
                }
            }
        } finally {
            try {
                storageAvailable(true);
            } finally {
                restoreApplicationRole(rbac, originalRole);
            }
        }
        assertUserResourcesPreserved();
        ApplicationSpecification retry = haSpecification(null, source.getJobId());
        try (KubernetesApplicationClient application = deployRemoteApplication(retry)) {
            try {
                String[] restored = awaitProgress(application, retry.getJobId(), sourceCheckpoint);
                boolean loadedCheckpoint = false;
                for (V1Pod master : rolePods(application, "master")) {
                    loadedCheckpoint |=
                            livePodLogs(application, master.getMetadata().getName())
                                    .contains(
                                            "pipeline(1) restore with CHECKPOINT on checkpointId("
                                                    + sourceCheckpoint
                                                    + ")");
                }
                assertTrue(
                        loadedCheckpoint, "Explicit retry must load the exact retained checkpoint");
                assertNoSourceReplay(
                        application,
                        "Explicit retry must not replay the checkpointed source marker");
                storedCheckpoint(retry.getJobId());
                application.cancel();
                awaitHaClean(application);
                assertUserResourcesPreserved();
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(application);
                throw failure;
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    private static void assertNoSourceReplay(
            KubernetesApplicationClient application, String message) throws Exception {
        List<V1Pod> fixedWorkers = workers(application);
        assertEquals(1, fixedWorkers.size(), "Expected the configured fixed worker");
        String logs = retainedPodLogs(fixedWorkers.get(0).getMetadata().getName());
        assertFalse(
                logs.isBlank(), "Retained worker logs are required for the no-replay assertion");
        assertFalse(logs.contains("APPLICATION_E2E_DATA"), message);
    }

    private static void restoreApplicationRole(RbacAuthorizationV1Api rbac, V1Role original)
            throws Exception {
        V1Role current = rbac.readNamespacedRole("application", namespace, null);
        current.setRules(original.getRules());
        rbac.replaceNamespacedRole("application", namespace, current, null, null, null, null);
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
        String job = java.nio.file.Files.readString(scenarioFile("batch", "job.conf").toPath());
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
        V1Pod owner = core.readNamespacedPod(INSPECTOR, namespace, null);
        return exec(
                owner,
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
        java.util.concurrent.atomic.AtomicReference<String[]> observed =
                new java.util.concurrent.atomic.AtomicReference<>();
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            String[] state = probe(application, job);
                            assertEquals("RUNNING", state[0]);
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
        // A persistent probe already blocks a common-pool task. Its two stream readers must
        // not exhaust that same pool and prevent later commands from draining their pipes.
        ExecutorService outputReaders = Executors.newFixedThreadPool(2);
        try {
            InputStream stdoutStream = process.getInputStream();
            InputStream stderrStream = process.getErrorStream();
            Future<byte[]> captured = outputReaders.submit(stdoutStream::readAllBytes);
            Future<byte[]> errors = outputReaders.submit(stderrStream::readAllBytes);
            assertTrue(
                    process.waitFor(
                            Arrays.asList(command).contains("persistent") ? 480 : 45,
                            TimeUnit.SECONDS),
                    "In-cluster command timed out");
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
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
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
        LOG.info("HA cleanup verified application={}", application.getClusterId());
    }

    private void assertUserResourcesPreserved() throws Exception {
        if (MULTI_NODE) {
            com.amazonaws.services.s3.AmazonS3 client = storageClient();
            try {
                assertEquals(
                        "preserve-user-object",
                        client.getObjectAsString("ha-checkpoints", "user-sentinel"));
            } finally {
                client.shutdown();
            }
        }
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
