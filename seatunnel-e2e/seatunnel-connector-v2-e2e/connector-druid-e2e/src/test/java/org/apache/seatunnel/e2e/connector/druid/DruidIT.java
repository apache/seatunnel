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

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

public class DruidIT extends TestSuiteBase implements TestResource {

    private static final String DRUID_SERVICE_NAME = "router";
    private static final int DRUID_SERVICE_PORT = 8888;
    private DockerComposeContainer environment;

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

        String coordinatorUrl = "http://localhost:8888";
        String datasource = "testDataSource";
        String sqlQuery = "SELECT * FROM " + datasource;

        try (CloseableHttpClient client = HttpClients.createDefault()) {

            HttpPost request = new HttpPost(coordinatorUrl + "/druid/v2/sql");

            String jsonRequest = "{\"query\": \"" + sqlQuery + "\"}";
            StringEntity entity = new StringEntity(jsonRequest);
            entity.setContentType("application/json");
            request.setEntity(entity);

            HttpResponse response = client.execute(request);

            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println("！！！666");
            System.out.println(responseBody);

            JSONObject jsonResponse = new JSONObject(responseBody);
            JSONArray results = jsonResponse.getJSONObject("results").getJSONArray("data");

            for (int i = 0; i < results.length(); i++) {
                JSONObject row = results.getJSONObject(i);
                System.out.println(row.toString(2)); // 使用2作为参数来美化打印的JSON
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
