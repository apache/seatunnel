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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MySqlIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MySqlIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSnapshotOnlyAllowsExactlyOnce() {
        RequiredOption.ConditionalRequiredOptions exactlyOnceRule =
                new MySqlIncrementalSourceFactory()
                        .optionRule().getRequiredOptions().stream()
                                .filter(RequiredOption.ConditionalRequiredOptions.class::isInstance)
                                .map(RequiredOption.ConditionalRequiredOptions.class::cast)
                                .filter(
                                        rule ->
                                                rule.getRequiredOption()
                                                        .contains(SourceOptions.EXACTLY_ONCE))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new AssertionError(
                                                        "exactly_once conditional rule is missing"));
        Set<Object> startupModes = new HashSet<>();
        Expression expression = exactlyOnceRule.getExpression();
        while (expression != null) {
            Assertions.assertEquals(
                    MySqlIncrementalSourceOptions.STARTUP_MODE,
                    expression.getCondition().getOption());
            startupModes.add(expression.getCondition().getExpectValue());
            expression = expression.getNext();
        }

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(StartupMode.INITIAL, StartupMode.SNAPSHOT_ONLY)),
                startupModes);
    }
}
