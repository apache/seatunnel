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

package org.apache.seatunnel.engine.e2e.classloader;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.engine.e2e.SeaTunnelContainer;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import io.restassured.response.Response;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.equalTo;

public abstract class ClassLoaderITBase extends SeaTunnelContainer {

    private static final String CONF_FILE = "/classloader/fake_to_inmemory.conf";

    private static final String http = "http://";

    private static final String colon = ":";

    abstract boolean cacheMode();

    private static final Path config = Paths.get(SEATUNNEL_HOME, "config");

    private static final Path binPath = Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL);

    abstract String seatunnelConfigFileName();

    @Test
    public void testFakeSourceToInMemorySink() throws IOException, InterruptedException {
        LOG.info("test classloader with cache mode: {}", cacheMode());
        for (int i = 0; i < 10; i++) {
            // load in memory sink which already leak thread with classloader
            Container.ExecResult execResult = executeJob(server, CONF_FILE);
            Assertions.assertEquals(0, execResult.getExitCode());
            Assertions.assertTrue(containsDaemonThread());
            if (cacheMode()) {
                Assertions.assertTrue(3 >= getClassLoaderCount());
            } else {
                Assertions.assertTrue(3 + 2 * i >= getClassLoaderCount());
            }
        }
    }

    @Test
    public void testFakeSourceToInMemorySinkForRestApi() throws IOException, InterruptedException {
        LOG.info("test classloader with cache mode: {}", cacheMode());
        ContainerUtil.copyConnectorJarToContainer(
                server,
                CONF_FILE,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            Response response =
                                    given().get(
                                                    http
                                                            + server.getHost()
                                                            + colon
                                                            + server.getFirstMappedPort()
                                                            + "/hazelcast/rest/cluster");
                            response.then().statusCode(200);
                            Thread.sleep(10000);
                            Assertions.assertEquals(
                                    1, response.jsonPath().getList("members").size());
                        });
        for (int i = 0; i < 10; i++) {
            // load in memory sink which already leak thread with classloader
            given().body(
                            "{\n"
                                    + "\t\"env\": {\n"
                                    + "\t\t\"parallelism\": 10,\n"
                                    + "\t\t\"job.mode\": \"BATCH\"\n"
                                    + "\t},\n"
                                    + "\t\"source\": [\n"
                                    + "\t\t{\n"
                                    + "\t\t\t\"plugin_name\": \"FakeSource\",\n"
                                    + "\t\t\t\"result_table_name\": \"fake\",\n"
                                    + "\t\t\t\"parallelism\": 10,\n"
                                    + "\t\t\t\"schema\": {\n"
                                    + "\t\t\t\t\"fields\": {\n"
                                    + "\t\t\t\t\t\"name\": \"string\",\n"
                                    + "\t\t\t\t\t\"age\": \"int\",\n"
                                    + "\t\t\t\t\t\"score\": \"double\"\n"
                                    + "\t\t\t\t}\n"
                                    + "\t\t\t}\n"
                                    + "\t\t}\n"
                                    + "\t],\n"
                                    + "\t\"transform\": [],\n"
                                    + "\t\"sink\": [\n"
                                    + "\t\t{\n"
                                    + "\t\t\t\"plugin_name\": \"InMemory\",\n"
                                    + "\t\t\t\"source_table_name\": \"fake\"\n"
                                    + "\t\t}\n"
                                    + "\t]\n"
                                    + "}")
                    .header("Content-Type", "application/json; charset=utf-8")
                    .post(
                            http
                                    + server.getHost()
                                    + colon
                                    + server.getFirstMappedPort()
                                    + RestConstant.CONTEXT_PATH
                                    + RestConstant.SUBMIT_JOB_URL)
                    .then()
                    .statusCode(200);

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    given().get(
                                                    http
                                                            + server.getHost()
                                                            + colon
                                                            + server.getFirstMappedPort()
                                                            + RestConstant.CONTEXT_PATH
                                                            + RestConstant.FINISHED_JOBS_INFO
                                                            + "/FINISHED")
                                            .then()
                                            .statusCode(200)
                                            .body("[0].jobStatus", equalTo("FINISHED")));
            Thread.sleep(5000);
            Assertions.assertTrue(containsDaemonThread());
            if (cacheMode()) {
                Assertions.assertTrue(3 >= getClassLoaderCount());
            } else {
                Assertions.assertTrue(3 + 2 * i >= getClassLoaderCount());
            }
        }
    }

    private int getClassLoaderCount() throws IOException, InterruptedException {
        Map<String, Integer> objects = ContainerUtil.getJVMLiveObject(server);
        String className =
                "org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader";
        return objects.getOrDefault(className, 0);
    }

    private boolean containsDaemonThread() throws IOException, InterruptedException {
        List<String> threads = ContainerUtil.getJVMThreadNames(server);
        return threads.stream()
                .anyMatch(thread -> thread.contains("InMemorySinkWriter-daemon-thread"));
    }

    @Override
    @BeforeEach
    public void startUp() throws Exception {
        server =
                createSeaTunnelContainerWithFakeSourceAndInMemorySink(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/classloader/"
                                + seatunnelConfigFileName());
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
