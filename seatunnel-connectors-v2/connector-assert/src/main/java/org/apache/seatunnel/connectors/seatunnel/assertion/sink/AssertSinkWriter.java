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

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.excecutor.AssertExecutor;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorException;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertTableRule;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.LongAccumulator;

public class AssertSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final Map<String, List<AssertFieldRule>> assertFieldRules;
    private final Map<String, List<AssertFieldRule.AssertRule>> assertRowRules;
    private final AssertTableRule assertTableRule;
    private static final AssertExecutor ASSERT_EXECUTOR = new AssertExecutor();
    private static final Map<String, LongAccumulator> LONG_ACCUMULATOR = new HashMap<>();
    private static final Set<String> TABLE_NAMES = new CopyOnWriteArraySet<>();

    public AssertSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            Map<String, List<AssertFieldRule>> assertFieldRules,
            Map<String, List<AssertFieldRule.AssertRule>> assertRowRules,
            AssertTableRule assertTableRule) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.assertFieldRules = assertFieldRules;
        this.assertRowRules = assertRowRules;
        this.assertTableRule = assertTableRule;
    }

    @Override
    public void write(SeaTunnelRow element) {
        TABLE_NAMES.add(element.getTableId());
        List<AssertFieldRule> assertFieldRule = null;
        String tableName;
        if (StringUtils.isNotEmpty(element.getTableId()) && assertFieldRules.size() > 1) {
            assertFieldRule = assertFieldRules.get(element.getTableId());
            tableName = element.getTableId();
        } else if (!assertFieldRules.isEmpty()) {
            assertFieldRule = assertFieldRules.values().iterator().next();
            tableName = assertFieldRules.keySet().iterator().next();
        } else {
            tableName = element.getTableId();
        }
        LONG_ACCUMULATOR
                .computeIfAbsent(tableName, (k) -> new LongAccumulator(Long::sum, 0))
                .accumulate(1);
        if (Objects.nonNull(assertFieldRule)) {
            ASSERT_EXECUTOR
                    .fail(element, seaTunnelRowType, assertFieldRule)
                    .ifPresent(
                            failRule -> {
                                throw new AssertConnectorException(
                                        AssertConnectorErrorCode.RULE_VALIDATION_FAILED,
                                        "row :" + element + " fail rule: " + failRule);
                            });
        }
    }

    @Override
    public void close() {
        if (!assertRowRules.isEmpty()) {
            assertRowRules.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .forEach(
                            entry -> {
                                List<AssertFieldRule.AssertRule> assertRules = entry.getValue();
                                assertRules.stream()
                                        .filter(
                                                assertRule -> {
                                                    switch (assertRule.getRuleType()) {
                                                        case MAX_ROW:
                                                            return !(LONG_ACCUMULATOR
                                                                            .get(entry.getKey())
                                                                            .longValue()
                                                                    <= assertRule.getRuleValue());
                                                        case MIN_ROW:
                                                            return !(LONG_ACCUMULATOR
                                                                            .get(entry.getKey())
                                                                            .longValue()
                                                                    >= assertRule.getRuleValue());
                                                        default:
                                                            return false;
                                                    }
                                                })
                                        .findFirst()
                                        .ifPresent(
                                                failRule -> {
                                                    throw new AssertConnectorException(
                                                            AssertConnectorErrorCode
                                                                    .RULE_VALIDATION_FAILED,
                                                            "row num :"
                                                                    + (LONG_ACCUMULATOR
                                                                                    .get(
                                                                                            entry
                                                                                                    .getKey())
                                                                                    .longValue()
                                                                            + " fail rule: "
                                                                            + failRule));
                                                });
                            });
        }
        if (!assertTableRule.getTableNames().isEmpty()
                && !new HashSet<>(assertTableRule.getTableNames()).equals(TABLE_NAMES)) {
            throw new AssertConnectorException(
                    AssertConnectorErrorCode.RULE_VALIDATION_FAILED,
                    "table names: "
                            + TABLE_NAMES
                            + " is not equal to "
                            + assertTableRule.getTableNames());
        }
    }
}
