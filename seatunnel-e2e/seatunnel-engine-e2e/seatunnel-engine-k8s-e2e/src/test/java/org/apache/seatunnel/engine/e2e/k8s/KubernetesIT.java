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

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageCmd;
import com.github.dockerjava.api.model.Info;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
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
        if (dockerClient.listImagesCmd().withImageNameFilter(tag).exec().isEmpty()) {
            copyFileToCurrentResources(hazelCastConfigFile, targetPath);
            File file =
                    new File(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/seatunnel_dockerfile");
            BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(file);
            buildImageCmd.withTags(Collections.singleton(tag));
            String imageId = buildImageCmd.start().awaitImageId();
            Assertions.assertNotNull(imageId);
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
        try {
            coreV1Api.createNamespacedService(namespace, yamlSvc, null, null, null, null);
            appsV1Api.createNamespacedStatefulSet(
                    namespace, yamlStatefulSet, null, null, null, null);
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
        Files.copy(
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(targetPath + "/jars/seatunnel-hadoop3-3.1.4-uber.jar"),
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
