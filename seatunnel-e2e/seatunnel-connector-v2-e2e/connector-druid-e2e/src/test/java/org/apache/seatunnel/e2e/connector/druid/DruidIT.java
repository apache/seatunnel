/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.druid;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class DruidIT extends TestSuiteBase implements TestResource {

    private static final String DRUID_SERVICE_NAME = "router";
    private static final int DRUID_SERVICE_PORT = 8888;
    private DockerComposeContainer environment;
    private String coordinatorURL;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        environment =
                new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
                        .withExposedService(
                                DRUID_SERVICE_NAME,
                                DRUID_SERVICE_PORT,
                                Wait.forListeningPort()
                                        .withStartupTimeout(Duration.ofSeconds(360)));
        environment.start();
        coordinatorURL = InetAddress.getLocalHost().getHostAddress() + ":8888";
        String resourceFilePath = "src/test/resources/fakesource_to_druid.conf";
        Path path = Paths.get(resourceFilePath);
        try {
            List<String> lines = Files.readAllLines(path);
            List<String> newLines =
                    lines.stream()
                            .map(
                                    line -> {
                                        if (line.contains("coordinatorUrl")) {
                                            return "    coordinatorUrl = "
                                                    + "\""
                                                    + coordinatorURL
                                                    + "\"";
                                        }
                                        return line;
                                    })
                            .collect(Collectors.toList());
            Files.write(path, newLines);
            System.out.println("Conf has been updated successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        environment.close();
    }

    @TestTemplate
    public void testDruidSink(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fakesource_to_druid.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        while (true) {
            String datasource = "testDataSource";
            String sqlQuery = "SELECT COUNT(*) FROM " + datasource;
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost request = new HttpPost(coordinatorURL + "/druid/v2/sql");
                String jsonRequest = "{\"query\": \"" + sqlQuery + "\"}";
                StringEntity entity = new StringEntity(jsonRequest);
                entity.setContentType("application/json");
                request.setEntity(entity);
                HttpResponse response = client.execute(request);

                String responseBody = EntityUtils.toString(response.getEntity());
                System.out.println(responseBody);
                if (!responseBody.contains("errorMessage")) {
                    assert (responseBody.contains("1000"));
                    break;
                }
                Thread.sleep(1000);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
