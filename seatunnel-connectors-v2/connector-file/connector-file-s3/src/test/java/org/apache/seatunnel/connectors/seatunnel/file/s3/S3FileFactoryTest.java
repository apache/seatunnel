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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Condition;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.s3.sink.S3FileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.s3.source.S3FileSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

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

    @Test
    void credentialsProviderConditionalRequiresAccessKeys() {
        OptionRule sourceRule = new S3FileSourceFactory().optionRule();
        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        sourceRule,
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS,
                        S3FileBaseOptions.S3_ACCESS_KEY),
                "S3File source optionRule should require access_key when the credentials provider is SimpleAWSCredentialsProvider");
        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        sourceRule,
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS,
                        S3FileBaseOptions.S3_SECRET_KEY),
                "S3File source optionRule should require secret_key when the credentials provider is SimpleAWSCredentialsProvider");

        OptionRule sinkRule = new S3FileSinkFactory().optionRule();
        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        sinkRule,
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS,
                        S3FileBaseOptions.S3_ACCESS_KEY),
                "S3File sink optionRule should require access_key when the credentials provider is SimpleAWSCredentialsProvider");
        Assertions.assertTrue(
                hasConditionalRequiredOption(
                        sinkRule,
                        S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS,
                        S3FileBaseOptions.S3_SECRET_KEY),
                "S3File sink optionRule should require secret_key when the credentials provider is SimpleAWSCredentialsProvider");
    }

    @Test
    void sinkOptionRuleRequiresFilePath() {
        OptionRule optionRule = new S3FileSinkFactory().optionRule();
        Map<String, Object> config = sinkConfig();
        config.remove(S3FileBaseOptions.FILE_PATH.key());

        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule));

        config.put(S3FileBaseOptions.FILE_PATH.key(), "/tmp/seatunnel");
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    @Test
    void sinkOptionRuleRequiresBucket() {
        OptionRule optionRule = new S3FileSinkFactory().optionRule();
        Map<String, Object> config = sinkConfig();
        config.remove(S3FileBaseOptions.S3_BUCKET.key());

        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule));

        config.put(S3FileBaseOptions.S3_BUCKET.key(), "s3a://seatunnel-test");
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    private static Map<String, Object> sinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(S3FileBaseOptions.FILE_PATH.key(), "/tmp/seatunnel");
        config.put(S3FileBaseOptions.S3_BUCKET.key(), "s3a://seatunnel-test");
        config.put(S3FileBaseOptions.FS_S3A_ENDPOINT.key(), "s3.example.com");
        config.put(
                S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER_CLASS.key(),
                S3FileBaseOptions.INSTANCE_PROFILE_CREDENTIALS_PROVIDER);
        return config;
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
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
