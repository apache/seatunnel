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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Checks installed connector output against fixed HTTP fixtures, including exact typed values. A
 * malformed row, lost nested field or changed numeric/text mapping must fail the Assert sink.
 */
class CbsStatLineIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("nginx:1.27.5-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("statline-fixture")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("cbs_statline_nginx.conf"),
                                "/etc/nginx/conf.d/default.conf")
                        .withExposedPorts(80)
                        .waitingFor(Wait.forHttp("/health").forStatusCode(200));
        fixture.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /*
     * The job retains the two-row bound and checks every fixture value through contract_ok.
     */
    @TestTemplate
    void readsTwoObservationPages(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/cbs_statline_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
