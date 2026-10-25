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

package org.apache.seatunnel.engine.e2e.k8s;

import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageCmd;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.Info;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Yaml;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class KubernetesIT {
    private static final String namespace = "default";
    private static final String svcName = "seatunnel";
    private static final String stsName = "seatunnel";
    private static final String podName = "seatunnel-0";

    @Test
    public void testTcpDiscovery()
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        runDiscoveryTest("hazelcast-tcp-discovery.yaml");
    }

    @Test
    public void testKubernetesDiscovery()
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        runDiscoveryTest("hazelcast-kubernetes-discovery.yaml");
    }

    private void runDiscoveryTest(String hazelCastConfigFile)
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        ApiClient client = Config.defaultClient();
        AppsV1Api appsV1Api = new AppsV1Api(client);
        CoreV1Api coreV1Api = new CoreV1Api(client);
        DockerClient dockerClient = DockerClientFactory.lazyClient();
        String targetPath =
                PROJECT_ROOT_PATH
                        + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources";
        // If the Docker BaseDirectory is set as the root directory of the project, the image
        // created is too large, so choose to copy the files that need to be created as images
        // to the same level as the dockerfile.
        String pomPath = PROJECT_ROOT_PATH + "/pom.xml";
        MavenXpp3Reader pomReader = new MavenXpp3Reader();
        Model model = pomReader.read(new FileReader(pomPath), true);
        String artifactId = model.getArtifactId();
        String tag = artifactId + ":latest";
        Info info = dockerClient.infoCmd().exec();
        log.info("Docker's environmental information");
        log.info(info.toString());
        List<Image> matchedImages = dockerClient.listImagesCmd().withReferenceFilter(tag).exec();
        log.info(
                "Check image existence with withReferenceFilter({}): matched {} images, isEmpty={}",
                tag,
                matchedImages.size(),
                matchedImages.isEmpty());
        if (matchedImages.isEmpty()) {
            log.info("Image '{}' not found in Docker daemon, starting manual docker build...", tag);
            copyFileToCurrentResources(hazelCastConfigFile, targetPath);
            File file =
                    new File(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/seatunnel_dockerfile");
            BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(file);
            buildImageCmd.withTags(Collections.singleton(tag));
            String imageId = buildImageCmd.start().awaitImageId();
            Assertions.assertNotNull(imageId);
            log.info("Image '{}' built successfully, imageId={}", tag, imageId);
        } else {
            log.info(
                    "Image '{}' already exists in Docker daemon (matched {} images), skipping manual build",
                    tag,
                    matchedImages.size());
        }
        Configuration.setDefaultApiClient(client);
        V1Service yamlSvc =
                (V1Service)
                        Yaml.load(
                                new File(
                                        PROJECT_ROOT_PATH
                                                + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/seatunnel-service.yaml"));
        V1StatefulSet yamlStatefulSet =
                (V1StatefulSet)
                        Yaml.load(
                                new File(
                                        PROJECT_ROOT_PATH
                                                + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/seatunnel-statefulset.yaml"));
        // Drop resources left over from a previous (possibly aborted) run so the creates below
        // do not fail with a 409 Conflict.
        cleanupResources(appsV1Api, coreV1Api);
        try {
            coreV1Api.createNamespacedService(namespace, yamlSvc, null, null, null, null);
            appsV1Api.createNamespacedStatefulSet(
                    namespace, yamlStatefulSet, null, null, null, null);
            try {
                Awaitility.await()
                        .atMost(360, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    V1StatefulSet v1StatefulSet =
                                            appsV1Api.readNamespacedStatefulSet(
                                                    stsName, namespace, null);
                                    Assertions.assertEquals(
                                            2, v1StatefulSet.getStatus().getReadyReplicas());
                                });
            } catch (ConditionTimeoutException e) {
                // Dump pod state so a timeout is diagnosable instead of a bare
                // "expected: <2> but was: <null>" (the StatefulSet status is not populated
                // when the pods never become Ready).
                logPodStates(coreV1Api);
                throw e;
            }
            // submit job
            String command =
                    "/opt/seatunnel/bin/seatunnel.sh --config /opt/seatunnel/config/v2.batch.config.template";
            Process process =
                    Runtime.getRuntime()
                            .exec(
                                    "kubectl exec -it "
                                            + podName
                                            + " -n "
                                            + namespace
                                            + " -- "
                                            + command);
            Assertions.assertEquals(0, process.waitFor());
            // submit an error job
            String commandError =
                    "/opt/seatunnel/bin/seatunnel.sh --config /opt/seatunnel/config/v2.batch.config.template.error";
            process =
                    Runtime.getRuntime()
                            .exec(
                                    "kubectl exec -it "
                                            + podName
                                            + " -n "
                                            + namespace
                                            + " -- "
                                            + commandError);
            Assertions.assertEquals(1, process.waitFor());
        } finally {
            appsV1Api.deleteNamespacedStatefulSet(
                    stsName, namespace, null, null, null, null, null, null);
            coreV1Api.deleteNamespacedService(
                    svcName, namespace, null, null, null, null, null, null);
        }
    }

    private void cleanupResources(AppsV1Api appsV1Api, CoreV1Api coreV1Api) throws ApiException {
        try {
            appsV1Api.deleteNamespacedStatefulSet(
                    stsName, namespace, null, null, null, null, null, null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                throw e;
            }
        }
        try {
            coreV1Api.deleteNamespacedService(
                    svcName, namespace, null, null, null, null, null, null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                throw e;
            }
        }
        // The name stays reserved while the object terminates, so wait for the resources to be
        // actually gone before re-creating them.
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(
                        () -> {
                            try {
                                appsV1Api.readNamespacedStatefulSet(stsName, namespace, null);
                                return false;
                            } catch (ApiException e) {
                                return e.getCode() == 404;
                            }
                        });
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .until(
                        () -> {
                            try {
                                coreV1Api.readNamespacedService(svcName, namespace, null);
                                return false;
                            } catch (ApiException e) {
                                return e.getCode() == 404;
                            }
                        });
    }

    private void logPodStates(CoreV1Api coreV1Api) {
        try {
            V1PodList podList =
                    coreV1Api.listNamespacedPod(
                            namespace,
                            null,
                            null,
                            null,
                            null,
                            "app=seatunnel",
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            log.error("Seatunnel pods are not ready. Pod states:");
            if (podList.getItems() != null) {
                for (V1Pod pod : podList.getItems()) {
                    log.error(
                            "  pod={} phase={} conditions={} containerStatuses={}",
                            pod.getMetadata().getName(),
                            pod.getStatus() == null ? null : pod.getStatus().getPhase(),
                            pod.getStatus() == null ? null : pod.getStatus().getConditions(),
                            pod.getStatus() == null
                                    ? null
                                    : pod.getStatus().getContainerStatuses());
                }
            }
        } catch (Exception e) {
            // Best effort only - never let the diagnostic mask the real timeout failure.
            log.error("Failed to fetch pod states for diagnostics", e);
        }
    }

    private void copyFileToCurrentResources(String hazelCastConfigFile, String targetPath)
            throws IOException {
        File jarsPath = new File(targetPath + "/jars");
        jarsPath.mkdirs();
        File binPath = new File(targetPath + "/bin");
        binPath.mkdirs();
        File connectorsPath = new File(targetPath + "/connectors");
        connectorsPath.mkdirs();
        FileUtils.copyDirectory(
                new File(PROJECT_ROOT_PATH + "/config"), new File(targetPath + "/config"));
        // replace hazelcast.yaml and hazelcast-client.yaml
        Files.copy(
                Paths.get(targetPath + "/custom_config/" + hazelCastConfigFile),
                Paths.get(targetPath + "/config/hazelcast.yaml"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(targetPath + "/custom_config/hazelcast-client.yaml"),
                Paths.get(targetPath + "/config/hazelcast-client.yaml"),
                StandardCopyOption.REPLACE_EXISTING);
        // Copy the Hadoop3 Uber JAR from the local Maven repository to the Docker build context,
        // and rename it to an unversioned name (seatunnel-shade-hadoop3-uber.jar).
        //
        // Why unversioned: the Dockerfile COPY directive cannot dynamically resolve the host's
        // Maven local repository path (~/.m2/repository). The jar must be pre-copied into the
        // build context with a stable filename so the Dockerfile does not need to change when
        // the seatunnel-shade artifact version is bumped. When a new seatunnel-shade version
        // is released, only the Maven coordinate in pom.xml needs updating — the Dockerfile,
        // the container path, and this test code remain unchanged.
        //
        // Other E2E tests (non-k8s) use testcontainers which resolve Maven coordinates directly,
        // so they use the versioned jar name.
        Files.copy(
                Paths.get(MavenJarUtil.getHadoop3UberJarPath()),
                Paths.get(targetPath + "/jars/seatunnel-shade-hadoop3-uber.jar"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/target/seatunnel-starter.jar"),
                Paths.get(targetPath + "/jars/seatunnel-starter.jar"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-transforms-v2/target/seatunnel-transforms-v2.jar"),
                Paths.get(targetPath + "/jars/seatunnel-transforms-v2.jar"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/src/main/bin/seatunnel.sh"),
                Paths.get(targetPath + "/bin/seatunnel.sh"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/src/main/bin/seatunnel-cluster.sh"),
                Paths.get(targetPath + "/bin/seatunnel-cluster.sh"),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(targetPath + "/custom_config/plugin-mapping.properties"),
                Paths.get(targetPath + "/connectors/plugin-mapping.properties"),
                StandardCopyOption.REPLACE_EXISTING);
        fuzzyCopy(
                PROJECT_ROOT_PATH + "/seatunnel-connectors-v2/connector-fake/target/",
                targetPath + "/connectors/",
                "^connector-fake.*\\.jar$");
        fuzzyCopy(
                PROJECT_ROOT_PATH + "/seatunnel-connectors-v2/connector-console/target/",
                targetPath + "/connectors/",
                "^connector-console.*\\.jar$");
    }

    private void fuzzyCopy(String sourceUrl, String targetUrl, String pattern) throws IOException {
        File dir = new File(sourceUrl);
        File[] files = dir.listFiles();
        Assertions.assertNotNull(files);
        for (File file : files) {
            if (Pattern.matches(pattern, file.getName())) {
                Files.copy(
                        file.toPath(),
                        Paths.get(targetUrl + file.getName()),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
