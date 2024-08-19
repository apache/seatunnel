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
import org.apache.seatunnel.e2e.common.container.EngineType;
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

    private static final String DATASOURCE = "testDataSource";
    private static final String MULTI_DATASOURCE_1 = "druid_sink_1";
    private static final String MULTI_DATASOURCE_2 = "druid_sink_2";
    private static final String SQL_QUERY_TEMPLATE = "SELECT * FROM ";
    private static final String CONF_PREFIX = "src/test/resources";
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
        changeCoordinatorURLConf(CONF_PREFIX + "/fakesource_to_druid.conf");
        changeCoordinatorURLConf(CONF_PREFIX + "/fakesource_to_druid_with_multi.conf");
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
            String responseBody = getSelectResponse(DATASOURCE);
            String expectedDataRow1 =
                    "\"c_boolean\":\"true\",\"c_timestamp\":\"2020-02-02T02:02:02\",\"c_string\":\"NEW\",\"c_tinyint\":1,\"c_smallint\":2,\"c_int\":3,\"c_bigint\":4,\"c_float\":4.3,\"c_double\":5.3,\"c_decimal\":6.3";
            String expectedDataRow2 =
                    "\"c_boolean\":\"false\",\"c_timestamp\":\"2012-12-21T12:34:56\",\"c_string\":\"AAA\",\"c_tinyint\":1,\"c_smallint\":1,\"c_int\":333,\"c_bigint\":323232,\"c_float\":3.1,\"c_double\":9.33333,\"c_decimal\":99999.99999999";
            String expectedDataRow3 =
                    "\"c_boolean\":\"true\",\"c_timestamp\":\"2016-03-12T11:29:33\",\"c_string\":\"BBB\",\"c_tinyint\":1,\"c_smallint\":2,\"c_int\":672,\"c_bigint\":546782,\"c_float\":7.9,\"c_double\":6.88888,\"c_decimal\":88888.45623489";
            String expectedDataRow4 =
                    "\"c_boolean\":\"false\",\"c_timestamp\":\"2014-04-28T09:13:27\",\"c_string\":\"CCC\",\"c_tinyint\":1,\"c_smallint\":1,\"c_int\":271,\"c_bigint\":683221,\"c_float\":4.8,\"c_double\":4.45271,\"c_decimal\":79277.68219012";

            if (!responseBody.contains("errorMessage")) {
                // Check sink data
                Assertions.assertEquals(responseBody.contains(expectedDataRow1), true);
                Assertions.assertEquals(responseBody.contains(expectedDataRow2), true);
                Assertions.assertEquals(responseBody.contains(expectedDataRow3), true);
                Assertions.assertEquals(responseBody.contains(expectedDataRow4), true);
                break;
            }
            Thread.sleep(1000);
        }
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK do not support multiple table read")
    @TestTemplate
    public void testDruidMultiSink(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/fakesource_to_druid_with_multi.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        // Check multi sink table 1
        while (true) {
            String responseBody = getSelectResponse(MULTI_DATASOURCE_1);
            String expectedDataRow =
                    "\"id\":1,\"val_bool\":\"true\",\"val_tinyint\":1,\"val_smallint\":2,\"val_int\":3,\"val_bigint\":4,\"val_float\":4.3,\"val_double\":5.3,\"val_decimal\":6.3,\"val_string\":\"NEW\"";

            if (!responseBody.contains("errorMessage")) {
                Assertions.assertEquals(responseBody.contains(expectedDataRow), true);
                break;
            }
            Thread.sleep(1000);
        }
        // Check multi sink table 2
        while (true) {
            String responseBody = getSelectResponse(MULTI_DATASOURCE_2);
            String expectedDataRow =
                    "\"id\":1,\"val_bool\":\"true\",\"val_tinyint\":1,\"val_smallint\":2,\"val_int\":3,\"val_bigint\":4,\"val_float\":4.3,\"val_double\":5.3,\"val_decimal\":6.3";
            if (!responseBody.contains("errorMessage")) {
                Assertions.assertEquals(responseBody.contains(expectedDataRow), true);
                break;
            }
            Thread.sleep(1000);
        }
    }

    private void changeCoordinatorURLConf(String resourceFilePath) throws UnknownHostException {
        coordinatorURL = InetAddress.getLocalHost().getHostAddress() + ":8888";
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

    private String getSelectResponse(String datasource) throws IOException {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost request = new HttpPost("http://" + coordinatorURL + "/druid/v2/sql");
            String jsonRequest = "{\"query\": \"" + SQL_QUERY_TEMPLATE + datasource + "\"}";
            StringEntity entity = new StringEntity(jsonRequest);
            entity.setContentType("application/json");
            request.setEntity(entity);
            HttpResponse response = client.execute(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            return responseBody;
        }
    }
}
