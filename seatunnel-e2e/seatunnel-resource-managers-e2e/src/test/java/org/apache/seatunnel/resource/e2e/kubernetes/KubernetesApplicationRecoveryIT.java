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

import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Yaml;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies durable checkpoint recovery across worker failure in a disposable K3s cluster. */
public class KubernetesApplicationRecoveryIT extends AbstractKubernetesApplicationIT {
    private static final long CHECKPOINT_TIMEOUT_SECONDS = 180;
    private static final long RESTORED_WORKER_TIMEOUT_SECONDS = 30;

    @Test
    void durableCheckpointRestoresSourceProgressAfterWorkerFailure() throws Exception {
        V1PersistentVolumeClaim checkpointVolume =
                Yaml.loadAs(suiteFile("checkpoints-pvc.yaml"), V1PersistentVolumeClaim.class);
        String claim = checkpointVolume.getMetadata().getName();
        core.createNamespacedPersistentVolumeClaim(
                namespace, checkpointVolume, null, null, null, null);
        String marker = "APPLICATION_E2E_DATA";
        ApplicationSpecification firstSpecification = specification("checkpoint");
        try (KubernetesApplicationClient first =
                deployApplication(firstSpecification, "checkpoint")) {
            try {
                awaitWorkersRunning(first, 1);
                String worker = workers(first).get(0).getMetadata().getName();
                Awaitility.await()
                        .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> assertTrue(livePodLogs(first, worker).contains(marker)));
                // At most one checkpoint is pending; two further completions ensure that the
                // accepted checkpoint was triggered after the marker was observed.
                int completed =
                        completedCheckpoints(
                                livePodLogs(first, masterPod(first).getMetadata().getName()),
                                firstSpecification.getJobId());
                awaitDurableCheckpoint(first, firstSpecification.getJobId(), completed + 2);
                core.deleteNamespacedPod(
                        worker, namespace, null, null, 0, null, "Background", null);
                awaitStatus(first, ApplicationStatus.FAILED);
                awaitWorkersRemoved(first);
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(first);
                throw failure;
            } finally {
                cancelAndAwait(first);
            }
        }
        V1PersistentVolumeClaim retained =
                core.readNamespacedPersistentVolumeClaim(claim, namespace, null);
        assertEquals("Bound", retained.getStatus().getPhase());
        assertTrue(
                retained.getMetadata().getOwnerReferences() == null
                        || retained.getMetadata().getOwnerReferences().isEmpty());
        ApplicationSpecification restoredSpecification = specification("checkpoint-restore");
        assertEquals(firstSpecification.getJobId(), restoredSpecification.getRestoreJobId());
        assertNotEquals(firstSpecification.getJobId(), restoredSpecification.getJobId());
        try (KubernetesApplicationClient restored =
                deployApplication(restoredSpecification, "checkpoint-restore")) {
            try {
                awaitWorkersRunning(restored, 1);
                awaitDurableCheckpoint(restored, restoredSpecification.getJobId(), 1);
                String restoredWorker = workers(restored).get(0).getMetadata().getName();
                Awaitility.await()
                        .atMost(RESTORED_WORKER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    String logs = livePodLogs(restored, restoredWorker);
                                    assertFalse(
                                            logs.isEmpty(),
                                            "Restored worker logs must be readable");
                                    assertFalse(
                                            logs.contains(marker),
                                            "The restored source must not replay rows included in the durable checkpoint");
                                });
            } catch (Exception | AssertionError failure) {
                recordApplicationDiagnostics(restored);
                throw failure;
            } finally {
                cancelAndAwait(restored);
            }
        }
        assertEquals(
                "Bound",
                core.readNamespacedPersistentVolumeClaim(claim, namespace, null)
                        .getStatus()
                        .getPhase());
    }

    private static int completedCheckpoints(String logs, long jobId) {
        return logs.split(
                                Pattern.quote(
                                        "pending checkpoint notify finished, job id: "
                                                + jobId
                                                + ","),
                                -1)
                        .length
                - 1;
    }

    private static void awaitDurableCheckpoint(
            KubernetesApplicationClient application, long jobId, int expectedCompletions) {
        withPodDiagnostics(
                application,
                () ->
                        Awaitility.await()
                                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                .pollInterval(2, TimeUnit.SECONDS)
                                .untilAsserted(
                                        () -> {
                                            V1Pod master = masterPod(application);
                                            assertTrue(
                                                    completedCheckpoints(
                                                                    livePodLogs(
                                                                            application,
                                                                            master.getMetadata()
                                                                                    .getName()),
                                                                    jobId)
                                                            >= expectedCompletions);
                                            assertTrue(
                                                    checkpointFile(master, jobId)
                                                            .contains(Long.toString(jobId)));
                                        }));
    }

    private static String checkpointFile(V1Pod master, long jobId) throws Exception {
        Process process =
                new Exec(apiClient)
                        .exec(
                                master,
                                new String[] {
                                    "find",
                                    "/opt/seatunnel/checkpoints",
                                    "-path",
                                    "*/" + jobId + "/*",
                                    "-type",
                                    "f",
                                    "!",
                                    "-name",
                                    "*tmp",
                                    "!",
                                    "-name",
                                    "*.crc",
                                    "-size",
                                    "+0c",
                                    "-print",
                                    "-quit"
                                },
                                "seatunnel",
                                false,
                                false);
        // Capture stdout before the SDK closes its WebSocket on process exit. Limit find to one
        // path so that waiting for exit cannot fill the SDK's stdout pipe and block completion.
        try (InputStream input = process.getInputStream();
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            assertTrue(
                    process.waitFor(15, TimeUnit.SECONDS),
                    "Checkpoint volume inspection timed out");
            assertEquals(0, process.exitValue());
            byte[] buffer = new byte[1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            process.destroy();
        }
    }
}
