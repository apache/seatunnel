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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.regex.Pattern;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

@Slf4j
public class KubernetesIT {

    @Test
    public void test()
            throws IOException, XmlPullParserException, ApiException, InterruptedException {
        String targetPath =
                PROJECT_ROOT_PATH
                        + "/seatunnel-e2e/seatunnel-engine-e2e/seatunnel-engine-k8s-e2e/src/test/resources";
        // If the Docker BaseDirectory is set as the root directory of the project, the image
        // created is too large, so choose to copy the files that need to be created as images
        // to the same level as the dockerfile. After completing the image creation, delete
        // these files locally
        copyFileToCurrentResources(targetPath);
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
        String podName = "seatunnel-0";
        String namespace = "default";
        coreV1Api.createNamespacedService(namespace, yamlSvc, null, null, null, null);
        appsV1Api.createNamespacedStatefulSet(namespace, yamlStatefulSet, null, null, null, null);
        Thread.sleep(5000);
        V1StatefulSet v1StatefulSet =
                appsV1Api.readNamespacedStatefulSet("seatunnel", "default", null);
        // assert ready replicas
        Assertions.assertNotNull(v1StatefulSet.getStatus().getReadyReplicas());
        Assertions.assertEquals(v1StatefulSet.getStatus().getReadyReplicas(), 2);
        log.info(v1StatefulSet.toString());
        // submit job
        String command =
                "sh /opt/seatunnel/bin/seatunnel.sh --config /opt/seatunnel/config/v2.batch.config.template";
        Process process =
                Runtime.getRuntime()
                        .exec(
                                "kubectl exec -it "
                                        + podName
                                        + " -n "
                                        + namespace
                                        + " -- "
                                        + command);
        // Read the output of the command
        BufferedReader readerResult =
                new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = readerResult.readLine()) != null) {
            log.info(line);
        }
        // Close the reader and wait for the process to complete
        readerResult.close();
        process.waitFor();
    }

    private void copyFileToCurrentResources(String targetPath) throws IOException {
        File jarsPath = new File(targetPath + "/jars");
        jarsPath.mkdirs();
        File binPath = new File(targetPath + "/bin");
        binPath.mkdirs();
        File connectorsPath = new File(targetPath + "/connectors/seatunnel");
        connectorsPath.mkdirs();
        FileUtils.copyDirectory(
                new File(PROJECT_ROOT_PATH + "/config"), new File(targetPath + "/config"));
        // replace hazelcast.yaml and hazelcast-client.yaml
        Files.copy(
                Paths.get(targetPath + "/custom_config/hazelcast.yaml"),
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
                targetPath + "/connectors/seatunnel/",
                "^connector-fake.*\\.jar$");
        fuzzyCopy(
                PROJECT_ROOT_PATH + "/seatunnel-connectors-v2/connector-console/target/",
                targetPath + "/connectors/seatunnel/",
                "^connector-console.*\\.jar$");
    }

    private void fuzzyCopy(String sourceUrl, String targetUrl, String pattern) throws IOException {
        File dir = new File(sourceUrl);
        File[] files = dir.listFiles();
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
