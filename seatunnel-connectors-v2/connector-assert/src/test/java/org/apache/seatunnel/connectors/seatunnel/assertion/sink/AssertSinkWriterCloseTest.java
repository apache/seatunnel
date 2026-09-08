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

package org.apache.seatunnel.connectors.seatunnel.assertion.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorException;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertTableRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers {@link AssertSinkWriter#close()}'s row-count and table-name rule evaluation: that it only
 * asserts the writer's own table when multiple tables share one config (SEATUNNEL-11995), and that
 * the underlying static, cross-subtask counters it depends on are isolated per {@link AssertSink}
 * instance rather than shared for the whole JVM lifetime (SEATUNNEL-12116).
 */
public class AssertSinkWriterCloseTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(new String[] {"id"}, new BasicType[] {BasicType.INT_TYPE});

    private static AssertFieldRule.AssertRule minRowRule(int minRows) {
        AssertFieldRule.AssertRule rule = new AssertFieldRule.AssertRule();
        rule.setRuleType(AssertFieldRule.AssertRuleType.MIN_ROW);
        rule.setRuleValue((double) minRows);
        return rule;
    }

    @Test
    public void testCloseOnlyAssertsOwnTableWhenMultipleTables() {
        // Writer A receives one row; table B never receives any row. The old implementation
        // slept 1s and asserted every table's rules in close(), so B's MIN_ROW=1 (0 rows
        // written) made the writer for table A fail. The fixed close() only asserts the
        // table this writer is responsible for, so it must succeed without depending on
        // other writers' progress.
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules = new HashMap<>();
        assertRowRules.put("tableA_mult", Collections.singletonList(minRowRule(1)));
        assertRowRules.put("tableB_other", Collections.singletonList(minRowRule(1)));

        AssertSinkWriter writerA =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        "tableA_mult",
                        "sink-instance-mult");
        writerA.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(writerA::close);
    }

    @Test
    public void testCloseThrowsWhenOwnTableRuleNotMet() {
        // MIN_ROW=2 for the writer's own table but only 1 row was written: close() must
        // still validate the writer's own table and fail.
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules = new HashMap<>();
        assertRowRules.put("tableA_fail", Collections.singletonList(minRowRule(2)));
        assertRowRules.put("tableB_other", Collections.singletonList(minRowRule(0)));

        AssertSinkWriter writerA =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        "tableA_fail",
                        "sink-instance-fail");
        writerA.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertThrows(AssertConnectorException.class, writerA::close);
    }

    /**
     * Two unrelated sink instances (different jobs, or two invocations of the same test) that
     * happen to use the same table name must not corrupt each other's row counts.
     *
     * <p>Reproduces SEATUNNEL-12116: before the {@code sinkInstanceId} scoping fix, the row counter
     * was a single static map keyed by table name alone and shared for the entire JVM lifetime, so
     * a second job reusing the same table name inherited the first job's already- accumulated count
     * and could spuriously trip MAX_ROW even though this job's own writer never exceeded it.
     */
    @Test
    public void testRowCountIsIsolatedAcrossUnrelatedSinkInstances() {
        String sharedTableName = "shared_table";
        AssertFieldRule.AssertRule maxRowOne = new AssertFieldRule.AssertRule();
        maxRowOne.setRuleType(AssertFieldRule.AssertRuleType.MAX_ROW);
        maxRowOne.setRuleValue(1.0);
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules =
                Collections.singletonMap(sharedTableName, Collections.singletonList(maxRowOne));

        AssertSinkWriter jobAWriter =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        sharedTableName,
                        "job-A");
        jobAWriter.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(
                jobAWriter::close, "job A wrote exactly 1 row against MAX_ROW=1");

        AssertSinkWriter jobBWriter =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        sharedTableName,
                        "job-B");
        jobBWriter.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(
                jobBWriter::close,
                "job B's own single row must not be inflated by job A's already-closed count"
                        + " for the same table name");
    }

    /**
     * Multiple parallel subtask writers of the same sink instance must keep sharing one row counter
     * per table, so MIN_ROW/MAX_ROW rules still see the table's true total rather than one
     * subtask's share. Guards against over-isolating by {@code sinkInstanceId} and accidentally
     * breaking the cross-subtask aggregation the static counter exists for.
     */
    @Test
    public void testRowCountStillAggregatesAcrossSubtasksOfSameSinkInstance() {
        String tableName = "parallel_table";
        AssertFieldRule.AssertRule minRowFive = minRowRule(5);
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules =
                Collections.singletonMap(tableName, Collections.singletonList(minRowFive));
        String sharedSinkInstanceId = "job-parallel";

        AssertSinkWriter subtask0 =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        tableName,
                        sharedSinkInstanceId);
        AssertSinkWriter subtask1 =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        assertRowRules,
                        new AssertTableRule(Collections.emptyList()),
                        tableName,
                        sharedSinkInstanceId);

        for (int i = 0; i < 3; i++) {
            subtask0.write(new SeaTunnelRow(new Object[] {i}));
        }
        for (int i = 0; i < 2; i++) {
            subtask1.write(new SeaTunnelRow(new Object[] {i}));
        }

        Assertions.assertDoesNotThrow(
                subtask1::close,
                "the combined 3+2=5 rows from both subtasks must satisfy MIN_ROW=5");
    }

    /**
     * The {@code assert_table} table-set rule must also isolate its observed-table-names state by
     * sink instance, for the same reason as the row counter above.
     */
    @Test
    public void testTableNamesRuleIsIsolatedAcrossUnrelatedSinkInstances() {
        AssertSinkWriter jobXWriter =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        new AssertTableRule(Collections.singletonList("table_x")),
                        "table_x",
                        "job-X");
        jobXWriter.write(rowForTable("table_x"));
        Assertions.assertDoesNotThrow(jobXWriter::close, "job X only ever saw its own table_x");

        AssertSinkWriter jobYWriter =
                new AssertSinkWriter(
                        ROW_TYPE,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        new AssertTableRule(Collections.singletonList("table_y")),
                        "table_y",
                        "job-Y");
        jobYWriter.write(rowForTable("table_y"));
        Assertions.assertDoesNotThrow(
                jobYWriter::close,
                "job Y's observed table set must not include job X's table_x from an unrelated"
                        + " sink instance");
    }

    private static SeaTunnelRow rowForTable(String tableId) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId(tableId);
        return row;
    }
}
