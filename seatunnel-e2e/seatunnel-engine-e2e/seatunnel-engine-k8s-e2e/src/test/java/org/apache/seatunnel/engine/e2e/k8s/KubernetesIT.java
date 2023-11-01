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
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class KubernetesIT {

    @Test
    public void test()
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        // If the Docker BaseDirectory is set as the root directory of the project, the image
        // created is too large, so choose to copy the files that need to be created as images
        // to the same level as the dockerfile. After completing the image creation, delete
        // these files locally
        copyFileToCurrentResources();
        DockerClient dockerClient = DockerClientFactory.lazyClient();
        String pomPath = PROJECT_ROOT_PATH + "/pom.xml";
        ApiClient client = Config.defaultClient();
        AppsV1Api appsV1Api = new AppsV1Api(client);
        CoreV1Api coreV1Api = new CoreV1Api(client);
        MavenXpp3Reader reader = new MavenXpp3Reader();
        Model model = reader.read(new FileReader(pomPath), true);
        String artifactId = model.getArtifactId();
        String tag = artifactId + ":lastest";
        Info info = dockerClient.infoCmd().exec();
        log.info("Docker's environmental information");
        log.info(info.toString());
        if (dockerClient.listImagesCmd().withImageNameFilter(tag).exec().size() > 0) {
            dockerClient.removeImageCmd(tag).exec();
        }
        File file =
                new File(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/seatunnel_dockerfile");
        BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(file);
        buildImageCmd.withTag(tag);
        String imageId = buildImageCmd.start().awaitImageId();
        Assertions.assertNotNull(imageId);
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
    }

    private void copyFileToCurrentResources() throws IOException {
        String targetPath = PROJECT_ROOT_PATH + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources/";
        Files.copy(Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-shade/seatunnel-hadoop3-3.1.4-uber/target/seatunnel-hadoop3-3.1.4-uber.jar"),
                Paths.get(targetPath + "jars/seatunnel-hadoop3-3.1.4-uber.jar")
        );
        Files.copy(Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/target/seatunnel-starter.jar"),
                Paths.get(targetPath + "jars/seatunnel-starter.jar")
        );
        Files.copy(Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-transforms-v2/target/seatunnel-transforms-v2.jar"),
                Paths.get(targetPath + "jars/seatunnel-transforms-v2.jar")
        );
        Files.copy(Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/src/main/bin/seatunnel.sh"),
                Paths.get(targetPath + "bin/seatunnel.sh")
        );
        Files.copy(Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-core/seatunnel-starter/src/main/bin/seatunnel-cluster.sh"),
                Paths.get(targetPath + "bin/seatunnel-cluster.sh")
        );
    }
}
