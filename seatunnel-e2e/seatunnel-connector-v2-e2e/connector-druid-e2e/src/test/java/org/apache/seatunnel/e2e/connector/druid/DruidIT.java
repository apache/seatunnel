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
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        disabledReason = "The RoaringBitmap version is not compatible in docker container")
public class DruidIT extends TestSuiteBase implements TestResource {

    private static final String datasource = "testDataSource";
    private static final String sqlQuery = "SELECT * FROM " + datasource;
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
        changeCoordinatorURLConf();
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
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost request = new HttpPost("http://" + coordinatorURL + "/druid/v2/sql");
                String jsonRequest = "{\"query\": \"" + sqlQuery + "\"}";
                StringEntity entity = new StringEntity(jsonRequest);
                entity.setContentType("application/json");
                request.setEntity(entity);
                HttpResponse response = client.execute(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                String expectedData =
                        "\"c_boolean\":\"true\",\"c_timestamp\":\"2020-02-02T02:02:02\",\"c_string\":\"NEW\",\"c_tinyint\":1,\"c_smallint\":2,\"c_int\":3,\"c_bigint\":4,\"c_float\":4.3,\"c_double\":5.3,\"c_decimal\":6.3";
                if (!responseBody.contains("errorMessage")) {
                    // Check sink data
                    Assertions.assertEquals(responseBody.contains(expectedData), true);
                    break;
                }
                Thread.sleep(1000);
            }
        }
    }

    private void changeCoordinatorURLConf() throws UnknownHostException {
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
            log.info("Conf has been updated successfully.");
        } catch (IOException e) {
            throw new RuntimeException("Change conf error", e);
        }
    }
}
