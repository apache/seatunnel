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

import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApplicationClient;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Pod;

import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs the successful batch submission and the invalid-job failure path in a disposable K3s. */
public class KubernetesApplicationBatchIT extends AbstractKubernetesApplicationIT {

    @Test
    void fakeSourceSubmissionCompletesWithAssertAndReleasesWorkers() throws Exception {
        ApplicationSpecification specification = specification("batch");
        try (KubernetesApplicationClient application = deployApplication(specification, "batch")) {
            try {
                awaitStatus(application, ApplicationStatus.SUCCEEDED);
                awaitWorkersRemoved(application);
                assertEquals(
                        ApplicationStatus.SUCCEEDED,
                        application.getStatus(),
                        "Runner cleanup must preserve the successful application result");
                assertEquals(
                        1,
                        batch.readNamespacedJob(application.getClusterId(), namespace, null)
                                .getStatus()
                                .getSucceeded());
                Properties runtime = new Properties();
                runtime.load(
                        new StringReader(
                                new String(
                                        Base64.getDecoder()
                                                .decode(
                                                        inlineSpecification(
                                                                masterPod(application))),
                                        StandardCharsets.UTF_8)));
                assertEquals("V1", runtime.getProperty("format.version"));
                assertEquals(
                        Long.toString(specification.getJobId()),
                        runtime.getProperty(ApplicationOptions.JOB_ID.key()));
                assertEquals(namespace, runtime.getProperty(KubernetesOptions.NAMESPACE.key()));
                assertEquals(
                        "Never", runtime.getProperty(KubernetesOptions.IMAGE_PULL_POLICY.key()));
                assertFalse(runtime.containsKey(KubernetesOptions.KUBE_CONFIG.key()));
                assertTrue(
                        missing(
                                () ->
                                        core.readNamespacedSecret(
                                                application.getClusterId(), namespace, null)));
                assertStatusFromApplicationCli(application.getClusterId());
                assertTrue(
                        missing(
                                () ->
                                        core.readNamespacedConfigMap(
                                                application.getClusterId(), namespace, null)));
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    @Test
    void invalidJobFailsAndReleasesWorkers() throws Exception {
        try (KubernetesApplicationClient application = deployApplication("invalid-job")) {
            try {
                awaitStatus(application, ApplicationStatus.FAILED);
                awaitWorkersRemoved(application);
                String masterLogs = retainedPodLogs(masterPod(application).getMetadata().getName());
                assertTrue(
                        masterLogs.contains("NonexistentSink"),
                        () ->
                                "The application must reach job parsing, not fail during runtime startup. Master logs:\n"
                                        + masterLogs);
            } finally {
                cancelAndAwait(application);
            }
        }
    }

    /** Queries a finished application through the packaged CLI without connecting to its master. */
    private static void assertStatusFromApplicationCli(String applicationId) throws Exception {
        String classpath =
                DependencyJar.staged("seatunnel-starter.jar").path()
                        + File.pathSeparator
                        + DependencyJar.staged("seatunnel-resource-manager-kubernetes.jar").path();
        Path output = Files.createTempFile("seatunnel-application-status-", ".log");
        Process process =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "java")
                                        .toString(),
                                "-cp",
                                classpath,
                                "org.apache.seatunnel.core.starter.seatunnel.SeaTunnelApplication",
                                "status",
                                "-t",
                                "kubernetes",
                                "--id",
                                applicationId,
                                "-a",
                                scenarioFile("batch", "application.config").getPath())
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Application CLI did not finish");
            String text = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), text);
            assertTrue(text.contains("Status: SUCCEEDED"), text);
        } finally {
            process.destroyForcibly();
            Files.deleteIfExists(output);
        }
    }

    private static String inlineSpecification(V1Pod master) {
        return master.getSpec().getContainers().get(0).getEnv().stream()
                .filter(env -> KubernetesConstants.SPECIFICATION_ENV.equals(env.getName()))
                .map(V1EnvVar::getValue)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing inline specification"));
    }
}
