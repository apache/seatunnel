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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.ConditionRule;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.configuration.util.RequiredOption;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Validates the {@code table_transform} entries of a multi-table transform.
 *
 * <p>An entry is a complete rule for the table it selects, so it is validated against {@code
 * tableRule} on its own. The transform-level rule is optional while entries exist, but has to be
 * either absent or complete.
 *
 * <p>Register it via {@code Conditions.extension(TransformCommonOptions.MULTI_TABLES, new
 * TableTransformRuleValidator(tableRule))}.
 */
public class TableTransformRuleValidator implements ConditionExtension<List<Map<String, Object>>> {

    private final OptionRule tableRule;

    public TableTransformRuleValidator(OptionRule tableRule) {
        this.tableRule = tableRule;
    }

    @Override
    public String description() {
        return "each 'table_transform' entry must satisfy the per-table rule, and the transform level "
                + "rule must be either absent or complete";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> tableTransforms) {
        if (tableTransforms != null) {
            for (Map<String, Object> tableTransform : tableTransforms) {
                validateTableTransform(tableTransform);
            }
        }
        // The transform-level rule may be absent, but not partially configured.
        if (isTransformLevelRuleConfigured(config)) {
            try {
                ConfigValidator.of(config).validate(tableRule);
            } catch (OptionValidationException e) {
                throw new OptionValidationException(
                        "The transform level rule must be either absent or complete. %s",
                        e.getRawMessage());
            }
        }
        return true;
    }

    /** Whether any option declared by the per-table rule is configured at the transform level. */
    private boolean isTransformLevelRuleConfigured(ReadonlyConfig config) {
        for (Option<?> option : declaredOptions(tableRule)) {
            if (config.getOptional(option).isPresent()) {
                return true;
            }
        }
        return false;
    }

    /** Collects all options declared by a rule, including the ones declared in nested rules. */
    private static List<Option<?>> declaredOptions(OptionRule rule) {
        List<Option<?>> options = new ArrayList<>(rule.getOptionalOptions());
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            options.addAll(requiredOption.getOptions());
        }
        for (ConditionRule conditionRule : rule.getConditionRules()) {
            options.addAll(declaredOptions(conditionRule.getOptionRule()));
        }
        return options;
    }

    private void validateTableTransform(Map<String, Object> tableTransform) {
        ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(tableTransform);
        String tablePath = tableConfig.get(TransformCommonOptions.TABLE_PATH);
        if (tablePath == null || tablePath.trim().isEmpty()) {
            throw new OptionValidationException(
                    "Each 'table_transform' entry must select its table with a non-blank 'table_path'");
        }
        try {
            ConfigValidator.of(tableConfig).validate(tableRule);
        } catch (OptionValidationException e) {
            throw new OptionValidationException(
                    "Invalid 'table_transform' entry for table [%s]: %s",
                    tablePath, e.getRawMessage());
        }
    }
}
