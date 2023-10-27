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

package org.apache.seatunnel.engine.e2e;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import org.codehaus.plexus.util.StringUtils;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.ObjectUtils;

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
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class KubernetesIT {

    @Test
    public void test()
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        String pomPath = PROJECT_ROOT_PATH + "/pom.xml";
        String artifactId = null;
        String version = null;
        String tag = null;
        Path seatunnelGzSource = null;
        Path seatunnelGzTarget = null;
        Path hadoopJarsource = null;
        Path hadoopJarTarget = null;
        DockerClient dockerClient = DockerClientFactory.lazyClient();
        ApiClient client = Config.defaultClient();
        AppsV1Api appsV1Api = new AppsV1Api(client);
        CoreV1Api coreV1Api = new CoreV1Api(client);
        try {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(new FileReader(pomPath), true);
            artifactId = model.getArtifactId();
            version = model.getProperties().getProperty("revision");
            tag = artifactId + ":lastest";
            Info info = dockerClient.infoCmd().exec();
            log.info("Docker's environmental information");
            log.info(info.toString());
            if (dockerClient.listImagesCmd().withImageNameFilter(tag).exec().size() > 0) {
                dockerClient.removeImageCmd(tag).exec();
            }
            // If the Docker BaseDirectory is set as the root directory of the project, the image
            // created is too large, so choose to copy the files that need to be created as images
            // to the same level as the dockerfile. After completing the image creation, delete
            // these files locally
            seatunnelGzSource =
                    Paths.get(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-dist/target/apache-seatunnel-"
                                    + version
                                    + "-bin.tar.gz");
            seatunnelGzTarget =
                    Paths.get(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/apache-seatunnel-"
                                    + version
                                    + "-bin.tar.gz");
            Files.deleteIfExists(seatunnelGzTarget);
            Files.copy(seatunnelGzSource, seatunnelGzTarget);
            hadoopJarsource =
                    Paths.get(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar");
            hadoopJarTarget =
                    Paths.get(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel-hadoop3-3.1.4-uber.jar");
            Files.deleteIfExists(hadoopJarTarget);
            Files.copy(hadoopJarsource, hadoopJarTarget);
            File file =
                    new File(
                            PROJECT_ROOT_PATH
                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_dockerfile");
            BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(file);
            buildImageCmd.withBuildArg("SEATUNNEL_VERSION", version);
            buildImageCmd.withTag(tag);
            String imageId = buildImageCmd.start().awaitImageId();
            Assertions.assertNotNull(imageId);
            Configuration.setDefaultApiClient(client);
            V1Service yamlSvc =
                    (V1Service)
                            Yaml.load(
                                    new File(
                                            PROJECT_ROOT_PATH
                                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel-service.yaml"));
            V1StatefulSet yamlStatefulSet =
                    (V1StatefulSet)
                            Yaml.load(
                                    new File(
                                            PROJECT_ROOT_PATH
                                                    + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel-statefulset.yaml"));
            try {
                coreV1Api.readNamespacedService("seatunnel", "default", null);
                coreV1Api.deleteNamespacedService(
                        "seatunnel", "default", null, null, null, null, null, null);
            } catch (Exception e) {
                log.info("The service of seatunel does not exist");
            }
            try {
                appsV1Api.readNamespacedStatefulSet("seatunnel", "default", null);
                appsV1Api.deleteNamespacedStatefulSet(
                        "seatunnel", "default", null, null, null, null, null, null);
            } catch (Exception e) {
                log.info("The statefulset of seatunel does not exist");
            }
            coreV1Api.createNamespacedService("default", yamlSvc, null, null, null, null);
            appsV1Api.createNamespacedStatefulSet(
                    "default", yamlStatefulSet, null, null, null, null);
            Thread.sleep(5000);
            V1StatefulSet v1StatefulSet =
                    appsV1Api.readNamespacedStatefulSet("seatunnel", "default", null);
            // assert ready replicas
            Assertions.assertNotNull(v1StatefulSet.getStatus().getReadyReplicas());
            Assertions.assertEquals(v1StatefulSet.getStatus().getReadyReplicas(), 2);
            log.info(v1StatefulSet.toString());
        } finally {
            if (StringUtils.isNotBlank(artifactId)
                    && StringUtils.isNotBlank(version)
                    && ObjectUtils.isNotEmpty(seatunnelGzTarget)) {
                Files.deleteIfExists(seatunnelGzTarget);
            }
            if (ObjectUtils.isNotEmpty(hadoopJarTarget)) {
                Files.deleteIfExists(hadoopJarTarget);
            }
            try {
                appsV1Api.deleteNamespacedStatefulSet(
                        "seatunnel", "default", null, null, null, null, null, null);
            } catch (Exception e) {
                log.error("deleteNamespacedStatefulSet fail:", e);
            }
            try {
                coreV1Api.deleteNamespacedService(
                        "seatunnel", "default", null, null, null, null, null, null);
            } catch (Exception e) {
                log.error("deleteNamespacedService fail:", e);
            }
            Thread.sleep(60000);
            dockerClient.removeImageCmd(tag).exec();
        }
    }
}
