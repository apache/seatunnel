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

package org.apache.seatunnel.connectors.seatunnel.file.s3;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.Condition;
import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.sink.S3FileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.s3.source.S3FileSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class S3FileFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new S3FileSourceFactory()).optionRule());
        Assertions.assertNotNull((new S3FileSinkFactory()).optionRule());
    }

    @Test
    void sourceOptionRuleShouldContainFileSplitOptions() {
        OptionRule rule = new S3FileSourceFactory().optionRule();
        Assertions.assertTrue(
                optionRuleContains(rule, FileBaseSourceOptions.ENABLE_FILE_SPLIT),
                "S3File source optionRule should include enable_file_split");
        Assertions.assertTrue(
                optionRuleContains(rule, FileBaseSourceOptions.FILE_SPLIT_SIZE),
                "S3File source optionRule should include file_split_size");

        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        rule,
                        FileBaseSourceOptions.FILE_FORMAT_TYPE,
                        FileBaseSourceOptions.ENABLE_FILE_SPLIT),
                "S3File source optionRule should expose enable_file_split for split-capable formats");

        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        rule,
                        FileBaseSourceOptions.ENABLE_FILE_SPLIT,
                        FileBaseSourceOptions.FILE_SPLIT_SIZE),
                "S3File source optionRule should expose file_split_size when enable_file_split=true");
    }

    private static boolean optionRuleContains(OptionRule rule, Option<?> option) {
        if (rule.getOptionalOptions().contains(option)) {
            return true;
        }
        return rule.getRequiredOptions().stream().anyMatch(ro -> ro.getOptions().contains(option));
    }

    private static boolean hasConditionalRequiredOption(
            OptionRule rule, Option<?> conditionalOption, Option<?> requiredOption) {
        return rule.getRequiredOptions().stream()
                .filter(ro -> ro instanceof RequiredOption.ConditionalRequiredOptions)
                .map(ro -> (RequiredOption.ConditionalRequiredOptions) ro)
                .anyMatch(
                        cro ->
                                expressionContainsOption(cro.getExpression(), conditionalOption)
                                        && cro.getRequiredOption().contains(requiredOption));
    }

    private static boolean expressionContainsOption(Expression expression, Option<?> option) {
        Expression currentExpression = expression;
        while (currentExpression != null) {
            if (conditionContainsOption(currentExpression.getCondition(), option)) {
                return true;
            }
            currentExpression = currentExpression.getNext();
        }
        return false;
    }

    private static boolean conditionContainsOption(Condition<?> condition, Option<?> option) {
        Condition<?> currentCondition = condition;
        while (currentCondition != null) {
            if (currentCondition.getOption().equals(option)) {
                return true;
            }
            currentCondition = currentCondition.getNext();
        }
        return false;
    }
}
