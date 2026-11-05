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
                        "tableA_mult");
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
                        "tableA_fail");
        writerA.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertThrows(AssertConnectorException.class, writerA::close);
    }

    /**
     * Two parallel writers of one table share the row counter, so the MIN_ROW rule for the whole
     * table must not fail when the first writer closes before the second one has written its rows.
     * Only the last writer to close evaluates the rules, and it sees the full total.
     */
    @Test
    public void testRowCountRulesAreEvaluatedByLastWriterOfTable() {
        String tableName = "shared_min_row_table";
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules =
                Collections.singletonMap(tableName, Collections.singletonList(minRowRule(2)));

        AssertSinkWriter firstWriter = newWriter(assertRowRules, tableName);
        AssertSinkWriter secondWriter = newWriter(assertRowRules, tableName);

        firstWriter.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(
                firstWriter::close,
                "the first writer must not judge MIN_ROW while the second writer is still open");

        secondWriter.write(new SeaTunnelRow(new Object[] {2}));
        Assertions.assertDoesNotThrow(
                secondWriter::close, "the last writer sees two rows in total, which meets MIN_ROW");
    }

    /**
     * The rules describe the whole table, so the aggregated count of all parallel writers is what
     * MAX_ROW is checked against: the last writer to close must fail when the total exceeds it.
     */
    @Test
    public void testLastWriterFailsWhenAggregatedRowCountViolatesRule() {
        String tableName = "shared_max_row_table";
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules =
                Collections.singletonMap(tableName, Collections.singletonList(maxRowRule(1)));

        AssertSinkWriter firstWriter = newWriter(assertRowRules, tableName);
        AssertSinkWriter secondWriter = newWriter(assertRowRules, tableName);

        firstWriter.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(firstWriter::close);

        secondWriter.write(new SeaTunnelRow(new Object[] {2}));
        Assertions.assertThrows(AssertConnectorException.class, secondWriter::close);
    }

    /**
     * Closing a writer twice must release its open-writer slot only once; otherwise a repeated
     * close could evaluate the rules while another writer of the table is still running.
     */
    @Test
    public void testRepeatedCloseReleasesOpenWriterSlotOnce() {
        String tableName = "shared_double_close_table";
        Map<String, List<AssertFieldRule.AssertRule>> assertRowRules =
                Collections.singletonMap(tableName, Collections.singletonList(minRowRule(3)));

        AssertSinkWriter firstWriter = newWriter(assertRowRules, tableName);
        AssertSinkWriter secondWriter = newWriter(assertRowRules, tableName);

        firstWriter.write(new SeaTunnelRow(new Object[] {1}));
        Assertions.assertDoesNotThrow(firstWriter::close);
        Assertions.assertDoesNotThrow(
                firstWriter::close, "a repeated close must not evaluate the rules early");

        secondWriter.write(new SeaTunnelRow(new Object[] {2}));
        // Two rows in total but MIN_ROW is three: the last writer still reports the violation.
        Assertions.assertThrows(AssertConnectorException.class, secondWriter::close);
    }

    private static AssertFieldRule.AssertRule maxRowRule(int maxRows) {
        AssertFieldRule.AssertRule rule = new AssertFieldRule.AssertRule();
        rule.setRuleType(AssertFieldRule.AssertRuleType.MAX_ROW);
        rule.setRuleValue((double) maxRows);
        return rule;
    }

    private static AssertSinkWriter newWriter(
            Map<String, List<AssertFieldRule.AssertRule>> assertRowRules, String tableName) {
        return new AssertSinkWriter(
                ROW_TYPE,
                Collections.emptyMap(),
                assertRowRules,
                new AssertTableRule(Collections.emptyList()),
                tableName);
    }
}
