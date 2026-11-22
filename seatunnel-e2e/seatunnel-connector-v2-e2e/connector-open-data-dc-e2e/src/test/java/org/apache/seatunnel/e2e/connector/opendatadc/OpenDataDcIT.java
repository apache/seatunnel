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

package org.apache.seatunnel.e2e.connector.opendatadc;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OpenDataDcIT extends TestSuiteBase implements TestResource {
    private static final String CONF = "/open_data_dc_to_assert.conf";
    private ArcGisCrashesFixture fixture;

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        fixture = new ArcGisCrashesFixture();
        fixture.start();
        Testcontainers.exposeHostPorts(fixture.getPort());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @TestTemplate
    public void continuesEmptyWindowsAndRetriesSameOffsetWithoutReplay(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.RECOVER);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        Assertions.assertEquals("FINISHED", container.getJobStatus(jobId));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "1=1 @1",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 12 AND OBJECTID <= 19 @0"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsWhenArcGisReturnsAnErrorWithHttp200(TestContainer container) throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.ARCGIS_ERROR);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("Open Data DC ArcGIS error: 400"));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsAfterThreeAttemptsForTheSameOffset(TestContainer container) throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.THROTTLE_EXHAUST);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("Open Data DC returned HTTP 429"));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsRatherThanTruncatesWhenEmptyWindowBudgetIsExhausted(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.EMPTY_EXHAUST);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("too many consecutive empty windows"));
        List<String> expected =
                new ArrayList<>(Arrays.asList("1=1 @0", "OBJECTID > 0 AND OBJECTID <= 19 @0"));
        for (int offset = 0; offset <= 64; offset += 2) {
            expected.add("OBJECTID > 7 AND OBJECTID <= 19 @" + offset);
        }
        Assertions.assertEquals(expected, fixture.getQueries());
    }

    private Container.ExecResult execute(TestContainer container, String jobId)
            throws IOException, InterruptedException {
        return container.executeJob(CONF, jobId, "open_data_dc.test.port=" + fixture.getPort());
    }
}
