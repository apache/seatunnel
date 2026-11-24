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

package org.apache.seatunnel.e2e.transform.udf;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Custom UDF is supported in Zeta")
@Slf4j
public class ExampleUdfIT extends TestSuiteBase {

    @TestTemplate
    public void testCustomUdf(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/custom_udf.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testUdfOpenFailureFailsJob(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(container.executeJob("/custom_udf_open_failure.conf"));
    }

    @TestTemplate
    public void testWhereUdfOpenFailureWithEmptyInput(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                runStartupJob(
                        container, 0, "SELECT name FROM dual WHERE FAIL_ON_OPEN(name) = 'never'"));
    }

    @TestTemplate
    public void testUdfOpensInUntakenCaseBranch(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                runStartupJob(
                        container,
                        1,
                        "SELECT CASE WHEN 1 = 0 THEN FAIL_ON_OPEN(name) ELSE name END AS name FROM dual"));
    }

    @TestTemplate
    public void testTimeZoneUdfOpenFailureWithEmptyInput(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                runStartupJob(
                        container,
                        0,
                        "SELECT TIMESTAMP '2025-01-01 00:00:00' AT TIME ZONE FAIL_ON_OPEN(name) AS ts FROM dual"));
    }

    @TestTemplate
    public void testLateralViewUdfOpenFailureWithEmptyInput(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                runStartupJob(
                        container,
                        0,
                        "SELECT name FROM dual LATERAL VIEW EXPLODE(ARRAY(FAIL_ON_OPEN(name))) AS expanded"));
    }

    @TestTemplate
    public void testUnusedUdfStaysClosedWithEmptyInput(TestContainer container)
            throws IOException, InterruptedException {
        assertSuccess(runStartupJob(container, 0, "SELECT name FROM dual"));
    }

    @TestTemplate
    public void testUdfLinkageFailureFailsJobWithEmptyInput(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                runStartupJob(
                        container,
                        0,
                        "SELECT CATALOG_CONTEXT(name) || LINKAGE_FAILURE(name) AS value FROM dual"),
                "requested UDF startup linkage failure");
    }

    @TestTemplate
    public void testCustomUdfContextLifecycle(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/custom_udf_context_lifecycle.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testUdfInputCatalogForComposedTableRules(TestContainer container)
            throws IOException, InterruptedException {
        assertSuccess(
                container.executeJob(
                        "/custom_udf_composed_tables.conf",
                        Collections.singletonList(
                                sqlVariable("secondRule", "CATALOG_CONTEXT(name)"))));
    }

    @TestTemplate
    public void testUdfOpenFailureInSecondComposedRule(TestContainer container)
            throws IOException, InterruptedException {
        assertStartupFailure(
                container.executeJob(
                        "/custom_udf_composed_tables.conf",
                        Collections.singletonList(
                                sqlVariable(
                                        "secondRule",
                                        "CASE WHEN 1 = 0 THEN FAIL_ON_OPEN(name) ELSE name END"))));
    }

    private Container.ExecResult runStartupJob(TestContainer container, int rowCount, String query)
            throws IOException, InterruptedException {
        return container.executeJob(
                "/custom_udf_startup.conf",
                Arrays.asList("sourceRows=" + rowCount, sqlVariable("startupQuery", query)));
    }

    private String sqlVariable(String name, String sql) {
        // The test container joins command arguments into a Bash command string.
        return "'" + name + "=" + sql.replace("'", "'\"'\"'") + "'";
    }

    private void assertSuccess(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());
    }

    private void assertStartupFailure(Container.ExecResult result) {
        assertStartupFailure(result, "requested UDF startup failure");
    }

    private void assertStartupFailure(Container.ExecResult result, String reason) {
        String output = result.getStdout() + result.getStderr();
        Assertions.assertNotEquals(0, result.getExitCode(), output);
        Assertions.assertTrue(output.contains(reason), output);
    }
}
