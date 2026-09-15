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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.excecutor.AssertExecutor;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.assertion.exception.AssertConnectorException;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertTableRule;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;

public class AssertSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final Map<String, List<AssertFieldRule>> assertFieldRules;
    private final Map<String, List<AssertFieldRule.AssertRule>> assertRowRules;
    private final AssertTableRule assertTableRule;
    private static final AssertExecutor ASSERT_EXECUTOR = new AssertExecutor();
    private static final Map<String, LongAccumulator> LONG_ACCUMULATOR = new ConcurrentHashMap<>();
    private static final Set<String> TABLE_NAMES = new CopyOnWriteArraySet<>();

    /**
     * Number of writers still open in this JVM, per table. The row counters above are shared by
     * every parallel writer of a table (Flink subtasks, Zeta parallel tasks in one node), so the
     * MIN_ROW / MAX_ROW rules describe the whole table and can only be judged once all of its
     * writers have finished. The last writer of a table to close evaluates them; an earlier close
     * would see a partial total and fail spuriously, which is the flaky behaviour reported for
     * Flink in apache/seatunnel#12116.
     */
    private static final Map<String, AtomicInteger> OPEN_WRITERS = new ConcurrentHashMap<>();

    private final String catalogTableName;

    /** Key of this writer in {@link #OPEN_WRITERS}; ConcurrentHashMap does not accept null. */
    private final String openWritersKey;

    /** Guards the open-writer count so a repeated close releases it exactly once. */
    private boolean closed;

    public AssertSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            Map<String, List<AssertFieldRule>> assertFieldRules,
            Map<String, List<AssertFieldRule.AssertRule>> assertRowRules,
            AssertTableRule assertTableRule,
            String catalogTableName) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.assertFieldRules = assertFieldRules;
        this.assertRowRules = assertRowRules;
        this.assertTableRule = assertTableRule;
        this.catalogTableName = catalogTableName;
        this.openWritersKey = catalogTableName == null ? "" : catalogTableName;
        OPEN_WRITERS.computeIfAbsent(openWritersKey, key -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public void write(SeaTunnelRow element) {
        TABLE_NAMES.add(element.getTableId());
        List<AssertFieldRule> assertFieldRule = null;
        String tableName = null;
        if (assertFieldRules.size() == 1) {
            assertFieldRule = assertFieldRules.values().iterator().next();
        }
        if (assertRowRules.size() == 1) {
            tableName = assertRowRules.keySet().iterator().next();
        }

        if (StringUtils.isEmpty(tableName) && StringUtils.isNotEmpty(element.getTableId())) {
            tableName = element.getTableId();
        } else {
            tableName = catalogTableName;
        }

        if (Objects.isNull(assertFieldRule)) {
            assertFieldRule = assertFieldRules.get(tableName);
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
        if (closed) {
            return;
        }
        closed = true;
        if (!releaseAndCheckLastWriter()) {
            // Another writer of this table is still running in this JVM and may still add rows,
            // so the shared counters are not final yet. That writer evaluates the rules when it
            // closes.
            return;
        }
        if (!assertRowRules.isEmpty()) {
            assertRowRules.entrySet().stream()
                    .filter(
                            entry ->
                                    !entry.getValue().isEmpty()
                                            && (assertRowRules.size() == 1
                                                    || entry.getKey()
                                                            .equals(this.catalogTableName)))
                    .forEach(
                            entry -> {
                                List<AssertFieldRule.AssertRule> assertRules = entry.getValue();
                                assertRules.stream()
                                        .filter(
                                                assertRule -> {
                                                    long count;
                                                    if (LONG_ACCUMULATOR.containsKey(
                                                            entry.getKey())) {
                                                        count =
                                                                LONG_ACCUMULATOR
                                                                        .get(entry.getKey())
                                                                        .longValue();
                                                    } else {
                                                        count = 0;
                                                    }
                                                    switch (assertRule.getRuleType()) {
                                                        case MAX_ROW:
                                                            return !(count
                                                                    <= assertRule.getRuleValue());
                                                        case MIN_ROW:
                                                            return !(count
                                                                    >= assertRule.getRuleValue());
                                                        default:
                                                            return false;
                                                    }
                                                })
                                        .findFirst()
                                        .ifPresent(
                                                failRule -> {
                                                    long count;
                                                    if (LONG_ACCUMULATOR.containsKey(
                                                            entry.getKey())) {
                                                        count =
                                                                LONG_ACCUMULATOR
                                                                        .get(entry.getKey())
                                                                        .longValue();
                                                    } else {
                                                        count = 0;
                                                    }
                                                    throw new AssertConnectorException(
                                                            AssertConnectorErrorCode
                                                                    .RULE_VALIDATION_FAILED,
                                                            "row num :"
                                                                    + count
                                                                    + " fail rule: "
                                                                    + failRule);
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

    /**
     * Releases this writer's slot in {@link #OPEN_WRITERS} and reports whether it was the last open
     * writer of its table in this JVM. The entry is dropped at zero so a writer recreated later,
     * for example after a restart, starts a fresh count instead of reusing a stale one.
     *
     * @return true when no other writer of the same table is still open
     */
    private boolean releaseAndCheckLastWriter() {
        AtomicInteger openWriters = OPEN_WRITERS.get(openWritersKey);
        if (openWriters == null || openWriters.decrementAndGet() > 0) {
            return openWriters == null;
        }
        OPEN_WRITERS.remove(openWritersKey, openWriters);
        return true;
    }
}
