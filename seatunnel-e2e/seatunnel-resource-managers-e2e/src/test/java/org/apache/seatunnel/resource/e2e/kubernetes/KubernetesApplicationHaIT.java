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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {2, 3})
    void mastersWorkersAndOwnerRecoverWithoutResubmissionThenCheckpointRestores(int masters)
            throws Exception {
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
                                    assertEquals(2, workers(application).size());
                                    assertTrue(
                                            workers(application).stream()
                                                    .allMatch(
                                                            pod ->
                                                                    "Running"
                                                                            .equals(
                                                                                    pod.getStatus()
                                                                                            .getPhase())));
                                });
                awaitProgress(application, first.getJobId(), -1);
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
                for (String role : Arrays.asList("standby", "active", "worker", "owner")) {
                    String[] before = probe(application, first.getJobId());
                    List<V1Pod> candidates =
                            role.equals("owner")
                                    ? rolePods(application, "owner")
                                    : role.equals("worker")
                                            ? workers(application)
                                            : rolePods(application, "master");
                    V1Pod victim = null;
                    if (role.equals("worker")) {
                        for (V1Pod candidate : candidates) {
                            if (livePodLogs(application, candidate.getMetadata().getName())
                                    .contains("APPLICATION_E2E_DATA")) {
                                victim = candidate;
                                break;
                            }
                        }
                        assertNotNull(victim, "A worker must be executing the streaming pipeline");
                    } else {
                        victim =
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
                    }
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
                    if (role.equals("worker")) {
                        assertTrue(
                                Long.parseLong(after[2]) > Long.parseLong(before[2]),
                                "Native restoration counter must advance");
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
                assertEquals(2, restoredWorkers.size());
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
    void completedBatchHasExactOutputAndReleasesAllOwnedResources() throws Exception {
        ApplicationSpecification specification = haSpecification("batch", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                assertEquals(
                        "120",
                        inspect(
                                        "find /checkpoints/ha-output/"
                                                + specification.getJobId()
                                                + " -type f ! -name '.*' -exec awk 'END {print NR}' {} + | awk '{n+=$1} END {print n+0}'")
                                .trim());
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
    void batchOutputRemainsCompleteAcrossWorkerAndActiveMasterLoss() throws Exception {
        ApplicationSpecification specification = haSpecification("batch-faults", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitProgress(application, specification.getJobId(), -1);
                Awaitility.await()
                        .atMost(90, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        assertTrue(
                                                committedRows(specification.getJobId()) > 0,
                                                "Committed input is required before fault injection"));
                for (String role : Arrays.asList("worker", "active")) {
                    long committed = committedRows(specification.getJobId());
                    assertTrue(
                            committed > 0 && committed < 480,
                            "Fault must occur while input is unfinished: " + committed);
                    String[] before = probe(application, specification.getJobId());
                    V1Pod victim =
                            (role.equals("worker")
                                            ? workers(application)
                                            : rolePods(application, "master"))
                                    .stream()
                                            .filter(
                                                    pod ->
                                                            role.equals("worker")
                                                                    || before[3].equals(
                                                                            pod.getStatus()
                                                                                    .getPodIP()))
                                            .findFirst()
                                            .get();
                    String name = victim.getMetadata().getName();
                    String uid = victim.getMetadata().getUid();
                    String oldMember =
                            before[5]
                                    .split(",")[
                                    Arrays.asList(before[4].split(","))
                                            .indexOf(victim.getStatus().getPodIP())];
                    LOG.info(
                            "HA data fault {} application={} before={}",
                            role,
                            application.getClusterId(),
                            Arrays.toString(before));
                    core.deleteNamespacedPod(
                            name, namespace, null, null, 0, null, "Background", null);
                    Awaitility.await()
                            .atMost(180, TimeUnit.SECONDS)
                            .ignoreExceptions()
                            .untilAsserted(
                                    () -> {
                                        V1Pod replacement =
                                                core.readNamespacedPod(name, namespace, null);
                                        assertNotEquals(uid, replacement.getMetadata().getUid());
                                        assertEquals("Running", replacement.getStatus().getPhase());
                                        String[] state =
                                                probe(application, specification.getJobId());
                                        assertTrue(
                                                Arrays.asList(state[4].split(","))
                                                        .contains(
                                                                replacement
                                                                        .getStatus()
                                                                        .getPodIP()));
                                        assertFalse(
                                                Arrays.asList(state[5].split(","))
                                                        .contains(oldMember));
                                        if (role.equals("worker")) {
                                            assertTrue(
                                                    Long.parseLong(state[2])
                                                            > Long.parseLong(before[2]));
                                        }
                                    });
                    long baseline = Long.parseLong(probe(application, specification.getJobId())[1]);
                    awaitProgress(application, specification.getJobId(), baseline);
                    LOG.info(
                            "HA data recovered {} application={} after={}",
                            role,
                            application.getClusterId(),
                            Arrays.toString(probe(application, specification.getJobId())));
                }
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                String actual =
                        inspect(
                                        "find /checkpoints/ha-output/"
                                                + specification.getJobId()
                                                + " -type f ! -name '.*' -exec awk '{print $0}' {} + | sort -n")
                                .trim();
                String expected =
                        java.util.stream.IntStream.rangeClosed(1, 480)
                                .mapToObj(Integer::toString)
                                .collect(java.util.stream.Collectors.joining("\n"));
                assertEquals(
                        expected,
                        actual,
                        "Every input ID must appear exactly once after native recovery");
                LOG.info(
                        "HA exact ID multiset verified application={} uniqueRows=480 after worker and active master loss",
                        application.getClusterId());
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
    void acceptedSinkFailureReleasesComputeAndKeepsDurableData() throws Exception {
        ApplicationSpecification specification = haSpecification("failed", null);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitProgress(application, specification.getJobId(), -1);
                String staging = "/checkpoints/ha-staging/" + specification.getJobId();
                inspect("mv " + staging + " " + staging + "-saved && touch " + staging);
                awaitStatus(application, ApplicationStatus.FAILED);
                assertEquals(
                        "FAILED",
                        batch.readNamespacedJob(application.getClusterId(), namespace, null)
                                .getMetadata()
                                .getAnnotations()
                                .get("seatunnel.apache.org/application-status"));
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
                assertEquals(3, rolePods(application, "master").size());
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
                for (String role : Arrays.asList("master", "worker")) {
                    String name = application.getClusterId() + "-" + role;
                    io.kubernetes.client.openapi.models.V1StatefulSet pool =
                            apps.readNamespacedStatefulSet(name, namespace, null);
                    pool.getSpec().setReplicas(0);
                    apps.replaceNamespacedStatefulSet(
                            name, namespace, pool, null, null, null, null);
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
    void missingRestoreCheckpointFailsWithoutFreshOutput() throws Exception {
        ApplicationSpecification specification = haSpecification("batch", 9223372036854775000L);
        try (KubernetesApplicationClient application = deployApplication(specification, "ha")) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                assertEquals(
                        "absent",
                        inspect(
                                        "test ! -d /checkpoints/ha-output/"
                                                + specification.getJobId()
                                                + " && echo absent")
                                .trim());
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

    private static long committedRows(long job) throws Exception {
        return Long.parseLong(
                inspect(
                                "find /checkpoints/ha-output/"
                                        + job
                                        + " -type f ! -name '.*' -exec awk 'END {print NR}' {} + | awk '{n+=$1} END {print n+0}'")
                        .trim());
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
        ApplicationSpecification identity = SeatunnelApplicationConfig.parse("env {}", options);
        options.put("application.job-id", Long.toString(identity.getJobId()));
        String mode = sinkMode.startsWith("batch") ? "BATCH" : "STREAMING";
        boolean faultBatch = sinkMode.equals("batch-faults");
        int rows = faultBatch ? 480 : 120;
        String input = "/checkpoints/ha-input/" + identity.getJobId();
        inspect(
                "mkdir -p "
                        + input
                        + "; i=1; while [ $i -le "
                        + rows
                        + " ]; do printf '%s\\n' \"$i\" > "
                        + input
                        + "/$i.txt; i=$((i+1)); done");
        String job =
                "env { job.mode = "
                        + mode
                        + ", parallelism = 2, checkpoint.interval = 1000, read_limit.rows_per_second = "
                        + (faultBatch ? 1 : 50)
                        + ", job.retry.times = 3 }\n"
                        + "source { LocalFile { path = \"/opt/seatunnel/checkpoints/ha-input/"
                        + identity.getJobId()
                        + "\", file_format_type = \"text\", schema = { fields { value = int } } } }\n"
                        + "sink { LocalFile { path = \"/opt/seatunnel/checkpoints/ha-output/"
                        + identity.getJobId()
                        + "\", tmp_path = \"/opt/seatunnel/checkpoints/ha-staging/"
                        + identity.getJobId()
                        + "\", file_format_type = \"text\", field_delimiter = \"|\", row_delimiter = \"\\n\", file_name_expression = \"${transactionId}_${now}\", is_enable_transaction = true } }";
        if (sinkMode.equals("failed")) {
            job =
                    "env { job.mode = STREAMING, parallelism = 2, checkpoint.interval = 1000, job.retry.times = 0 }\n"
                            + "source { FakeSource { row.num = 120, split.num = 60, split.read-interval = 500, schema = { fields { value = int } } } }\n"
                            + job.substring(job.indexOf("sink {"));
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
        try {
            java.util.concurrent.CompletableFuture<byte[]> captured =
                    capture(process.getInputStream());
            java.util.concurrent.CompletableFuture<byte[]> errors =
                    capture(process.getErrorStream());
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
            process.destroy();
            process.getInputStream().close();
            process.getErrorStream().close();
        }
    }

    private static java.util.concurrent.CompletableFuture<byte[]> capture(
            java.io.InputStream stream) {
        return java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return stream.readAllBytes();
                    } catch (java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
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
