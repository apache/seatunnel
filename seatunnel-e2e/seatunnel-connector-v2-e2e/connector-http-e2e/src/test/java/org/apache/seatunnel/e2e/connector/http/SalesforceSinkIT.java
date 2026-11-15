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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

class SalesforceSinkIT extends TestSuiteBase implements TestResource {
    private static final String PATH =
            "/services/data/v59.0/composite/sobjects/Account/External_Id__c";
    private GenericContainer<?> server;
    private MockServerClient mock;

    @BeforeAll
    @Override
    public void startUp() {
        server =
                new GenericContainer<>(DockerImageName.parse("mockserver/mockserver:5.14.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("salesforce-mock")
                        .withExposedPorts(1080)
                        .waitingFor(Wait.forHttp("/").forStatusCode(404));
        server.start();
        mock = new MockServerClient(server.getHost(), server.getMappedPort(1080));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mock != null) {
            mock.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private void responses(String result) {
        mock.reset();
        mock.when(request().withMethod("POST").withPath("/services/oauth2/token"))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\"access_token\":\"test-token\",\"instance_url\":\"http://salesforce-mock:1080\"}"));
        mock.when(
                        request()
                                .withMethod("PATCH")
                                .withPath(PATH)
                                .withHeader("Authorization", "Bearer test-token"))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(result));
    }

    @TestTemplate
    void upsertsOneObjectAndFlushesAtEndOfInput(TestContainer container) throws Exception {
        responses(
                "[{\"id\":\"001A\",\"success\":true,\"created\":true,\"errors\":[]},"
                        + "{\"id\":\"001B\",\"success\":true,\"created\":true,\"errors\":[]}]");
        Container.ExecResult result = container.executeJob("/fake_to_salesforce.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        HttpRequest[] requests =
                mock.retrieveRecordedRequests(request().withMethod("PATCH").withPath(PATH));
        Assertions.assertTrue(requests.length > 0);
        ObjectMapper mapper = new ObjectMapper();
        for (HttpRequest sent : requests) {
            JsonNode body = mapper.readTree(sent.getBodyAsString());
            Assertions.assertTrue(body.path("allOrNone").asBoolean());
            Assertions.assertEquals(2, body.path("records").size());
            Assertions.assertEquals(
                    "first", body.path("records").get(0).path("External_Id__c").asText());
            Assertions.assertEquals(
                    "second", body.path("records").get(1).path("External_Id__c").asText());
            Assertions.assertEquals(
                    "Account",
                    body.path("records").get(0).path("attributes").path("type").asText());
        }
    }

    @TestTemplate
    void rejectsRecordErrorsInsteadOfCompletingTheJob(TestContainer container) throws Exception {
        responses(
                "[{\"id\":null,\"success\":false,\"errors\":[{\"statusCode\":\"REQUIRED_FIELD_MISSING\",\"message\":\"mock validation failure\"}]},"
                        + "{\"id\":null,\"success\":false,\"errors\":[{\"statusCode\":\"ALL_OR_NONE_OPERATION_ROLLED_BACK\",\"message\":\"rollback\"}]}]");
        Container.ExecResult result = container.executeJob("/fake_to_salesforce.conf");
        Assertions.assertNotEquals(0, result.getExitCode(), "A rejected upsert must fail the job");
        String logs = result.getStdout() + result.getStderr() + container.getServerLogs();
        Assertions.assertTrue(
                logs.contains("REQUIRED_FIELD_MISSING"),
                "The failure must expose the Salesforce record status code");
        Assertions.assertTrue(
                mock.retrieveRecordedRequests(request().withMethod("PATCH").withPath(PATH)).length
                        > 0);
    }
}
