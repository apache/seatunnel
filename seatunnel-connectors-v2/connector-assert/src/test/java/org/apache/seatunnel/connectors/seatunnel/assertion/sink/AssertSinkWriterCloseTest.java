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
}
