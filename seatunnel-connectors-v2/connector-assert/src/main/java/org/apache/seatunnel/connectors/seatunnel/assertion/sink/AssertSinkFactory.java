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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.configuration.util.Conditions.extension;
import static org.apache.seatunnel.api.configuration.util.Conditions.mapNotEmpty;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.CATALOG_TABLE_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.ROW_RULES;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions.RULES;

@AutoService(Factory.class)
public class AssertSinkFactory implements TableSinkFactory {

    /**
     * Valid top-level keys inside the {@code rules} map. At least one of these must be present;
     * otherwise the configuration parses successfully at the map level but yields no actual
     * assertion rule, which is a user error.
     */
    private static final Set<String> VALID_RULE_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            ConnectorCommonOptions.TABLE_CONFIGS.key(),
                            ConnectorCommonOptions.TABLE_NAMES.key(),
                            ROW_RULES,
                            FIELD_RULES,
                            CATALOG_TABLE_RULES));

    /**
     * Validates that the {@code rules} map contains at least one recognized rule key so that the
     * assertion configuration is not empty. Migrated from the imperative check in {@link
     * AssertSink} constructor.
     */
    private static class AssertRulesValidator implements ConditionExtension<Map<String, Object>> {
        @Override
        public String description() {
            return "must contain at least one of: " + VALID_RULE_KEYS;
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Map<String, Object> value) {
            if (value == null || value.isEmpty()) {
                return false;
            }
            for (String key : VALID_RULE_KEYS) {
                if (value.containsKey(key)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public String factoryIdentifier() {
        return "Assert";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(RULES, mapNotEmpty(RULES), extension(RULES, new AssertRulesValidator()))
                .optional(MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new AssertSink(context.getOptions(), context.getCatalogTable());
    }
}
