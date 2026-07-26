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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.fake.config.MultipleTableFakeSourceConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.configuration.util.Conditions.extension;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterOrEqual;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessOrEqual;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BINARY_VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_DAY_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_MONTH_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_YEAR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ROWS;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.STRING_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.STRING_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_HOUR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_MINUTE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_SECOND_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_MIN;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.VECTOR_FLOAT_MAX;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.VECTOR_FLOAT_MIN;

@AutoService(Factory.class)
public class FakeSourceFactory implements TableSourceFactory, SupportSourceDryRunValidation {
    @Override
    public String factoryIdentifier() {
        return "FakeSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .exclusive(ConnectorCommonOptions.TABLE_CONFIGS, ConnectorCommonOptions.SCHEMA)
                .optional(
                        STRING_FAKE_MODE,
                        TINYINT_FAKE_MODE,
                        SMALLINT_FAKE_MODE,
                        INT_FAKE_MODE,
                        BIGINT_FAKE_MODE,
                        FLOAT_FAKE_MODE,
                        DOUBLE_FAKE_MODE,
                        ROWS,
                        ROW_NUM,
                        SPLIT_NUM,
                        SPLIT_READ_INTERVAL,
                        MAP_SIZE,
                        ARRAY_SIZE,
                        BYTES_LENGTH,
                        VECTOR_DIMENSION,
                        BINARY_VECTOR_DIMENSION,
                        DATE_YEAR_TEMPLATE,
                        DATE_MONTH_TEMPLATE,
                        DATE_DAY_TEMPLATE,
                        TIME_HOUR_TEMPLATE,
                        TIME_MINUTE_TEMPLATE,
                        TIME_SECOND_TEMPLATE)
                .conditional(STRING_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, STRING_TEMPLATE)
                .conditional(
                        TINYINT_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, TINYINT_TEMPLATE)
                .conditional(
                        SMALLINT_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, SMALLINT_TEMPLATE)
                .conditional(INT_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, INT_TEMPLATE)
                .conditional(BIGINT_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, BIGINT_TEMPLATE)
                .conditional(FLOAT_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, FLOAT_TEMPLATE)
                .conditional(DOUBLE_FAKE_MODE, FakeSourceOptions.FakeMode.TEMPLATE, DOUBLE_TEMPLATE)
                .optional(
                        TINYINT_MIN,
                        greaterOrEqual(TINYINT_MIN, TINYINT_MIN.defaultValue())
                                .and(lessOrEqual(TINYINT_MIN, TINYINT_MAX.defaultValue())))
                .optional(
                        TINYINT_MAX,
                        greaterOrEqual(TINYINT_MAX, TINYINT_MIN.defaultValue())
                                .and(lessOrEqual(TINYINT_MAX, TINYINT_MAX.defaultValue())))
                .optional(
                        SMALLINT_MIN,
                        greaterOrEqual(SMALLINT_MIN, SMALLINT_MIN.defaultValue())
                                .and(lessOrEqual(SMALLINT_MIN, SMALLINT_MAX.defaultValue())))
                .optional(
                        SMALLINT_MAX,
                        greaterOrEqual(SMALLINT_MAX, SMALLINT_MIN.defaultValue())
                                .and(lessOrEqual(SMALLINT_MAX, SMALLINT_MAX.defaultValue())))
                .optional(
                        INT_MIN,
                        greaterOrEqual(INT_MIN, INT_MIN.defaultValue())
                                .and(lessOrEqual(INT_MIN, INT_MAX.defaultValue())))
                .optional(
                        INT_MAX,
                        greaterOrEqual(INT_MAX, INT_MIN.defaultValue())
                                .and(lessOrEqual(INT_MAX, INT_MAX.defaultValue())))
                .optional(
                        BIGINT_MIN,
                        greaterOrEqual(BIGINT_MIN, BIGINT_MIN.defaultValue())
                                .and(lessOrEqual(BIGINT_MIN, BIGINT_MAX.defaultValue())))
                .optional(
                        BIGINT_MAX,
                        greaterOrEqual(BIGINT_MAX, BIGINT_MIN.defaultValue())
                                .and(lessOrEqual(BIGINT_MAX, BIGINT_MAX.defaultValue())))
                .optional(
                        FLOAT_MIN,
                        greaterOrEqual(FLOAT_MIN, FLOAT_MIN.defaultValue())
                                .and(lessOrEqual(FLOAT_MIN, FLOAT_MAX.defaultValue())))
                .optional(
                        FLOAT_MAX,
                        greaterOrEqual(FLOAT_MAX, FLOAT_MIN.defaultValue())
                                .and(lessOrEqual(FLOAT_MAX, FLOAT_MAX.defaultValue())))
                .optional(
                        DOUBLE_MIN,
                        greaterOrEqual(DOUBLE_MIN, DOUBLE_MIN.defaultValue())
                                .and(lessOrEqual(DOUBLE_MIN, DOUBLE_MAX.defaultValue())))
                .optional(
                        DOUBLE_MAX,
                        greaterOrEqual(DOUBLE_MAX, DOUBLE_MIN.defaultValue())
                                .and(lessOrEqual(DOUBLE_MAX, DOUBLE_MAX.defaultValue())))
                .optional(
                        VECTOR_FLOAT_MIN,
                        greaterOrEqual(VECTOR_FLOAT_MIN, VECTOR_FLOAT_MIN.defaultValue())
                                .and(
                                        lessOrEqual(
                                                VECTOR_FLOAT_MIN, VECTOR_FLOAT_MAX.defaultValue())))
                .optional(
                        VECTOR_FLOAT_MAX,
                        greaterOrEqual(VECTOR_FLOAT_MAX, VECTOR_FLOAT_MIN.defaultValue())
                                .and(
                                        lessOrEqual(
                                                VECTOR_FLOAT_MAX, VECTOR_FLOAT_MAX.defaultValue())))
                .valueConstraint(
                        extension(
                                ConnectorCommonOptions.TABLE_CONFIGS,
                                new TableConfigsValidationExtension()))
                .build();
    }

    private static final class TableConfigsValidationExtension
            implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each tables_configs entry must satisfy the FakeSource option rules";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> tableConfigs)
                throws OptionValidationException {
            if (tableConfigs == null) {
                return true;
            }
            OptionRule childRule = new FakeSourceFactory().optionRule();
            for (int index = 0; index < tableConfigs.size(); index++) {
                Map<String, Object> childConfig = tableConfigs.get(index);
                if (childConfig == null) {
                    throw new OptionValidationException(
                            "Invalid tables_configs[%s]: child config must not be null", index);
                }
                if (childConfig.containsKey(ConnectorCommonOptions.TABLE_CONFIGS.key())) {
                    throw new OptionValidationException(
                            "Invalid tables_configs[%s]: nested tables_configs is not supported",
                            index);
                }
                try {
                    ConfigValidator.of(ReadonlyConfig.fromMap(childConfig)).validate(childRule);
                } catch (OptionValidationException e) {
                    throw new OptionValidationException(
                            "Invalid tables_configs[%s]: %s", index, e.getRawMessage());
                }
            }
            return true;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new FakeSource(context.getOptions());
    }

    @Override
    public List<CatalogTable> inferSchemaForDryRun(TableSourceFactoryContext context) {
        return new MultipleTableFakeSourceConfig(context.getOptions())
                .getFakeConfigs().stream()
                        .map(FakeConfig::getCatalogTable)
                        .collect(Collectors.toList());
    }

    @Override
    public void validateConnectionForDryRun(
            TableSourceFactoryContext context, List<CatalogTable> catalogTables) {
        // FakeSource generates data in memory and has no external system to connect to,
        // so schema inference above is the entire Layer 1 validation.
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return FakeSource.class;
    }
}
