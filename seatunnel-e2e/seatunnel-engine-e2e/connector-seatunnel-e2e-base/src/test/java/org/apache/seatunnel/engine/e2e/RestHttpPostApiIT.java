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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.junit.jupiter.api.TestTemplate;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Just test the rest api of seatunnel")
public class RestHttpPostApiIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "http://localhost:5801";
    private static final Long JOB_ID = System.currentTimeMillis() + 12345L;
    private static final String JOB_NAME = "test";

    @TestTemplate
    public void testSubmitJobWithCustomJobIdOrNot(TestContainer container) {
        Set<String> connectorNames =
                new HashSet<>(Arrays.asList("connector-fake", "connector-console"));
        container.copySpecifyConnectorJarsToContainer(connectorNames);
        given().header("Content-Type", "application/json")
                .body(
                        "{\"env\":{\"job.mode\":\"batch\"},\"source\":[{\"plugin_name\":\"FakeSource\",\"result_table_name\":\"fake\",\"row.num\":100,\"schema\":{\"fields\":{\"name\":\"string\",\"age\":\"int\",\"card\":\"int\"}}}],\"transform\":[],\"sink\":[{\"plugin_name\":\"Console\",\"source_table_name\":[\"fake\"]}]}")
                .when()
                .post(
                        HOST
                                + RestConstant.SUBMIT_JOB_URL
                                + "?jobId="
                                + JOB_ID
                                + "&jobName="
                                + JOB_NAME)
                .then()
                .statusCode(200)
                .body("jobId", equalTo(JOB_ID));

        given().header("Content-Type", "application/json")
                .body(
                        "{\"env\":{\"job.mode\":\"batch\"},\"source\":[{\"plugin_name\":\"FakeSource\",\"result_table_name\":\"fake\",\"row.num\":100,\"schema\":{\"fields\":{\"name\":\"string\",\"age\":\"int\",\"card\":\"int\"}}}],\"transform\":[],\"sink\":[{\"plugin_name\":\"Console\",\"source_table_name\":[\"fake\"]}]}")
                .when()
                .post(HOST + RestConstant.SUBMIT_JOB_URL + "?jobName=" + JOB_NAME)
                .then()
                .statusCode(200)
                .body("jobId", not(JOB_ID));
    }

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}
}
