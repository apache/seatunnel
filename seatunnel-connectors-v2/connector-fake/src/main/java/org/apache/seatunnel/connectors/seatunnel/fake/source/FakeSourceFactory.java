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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BIGINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BINARY_VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_DAY_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_MONTH_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DATE_YEAR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.DOUBLE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.FLOAT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.INT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROWS;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SMALLINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.STRING_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_HOUR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_MINUTE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TIME_SECOND_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.TINYINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeOption.VECTOR_DIMENSION;

@AutoService(Factory.class)
public class FakeSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "FakeSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(ConnectorCommonOptions.SCHEMA)
                .optional(STRING_FAKE_MODE)
                .conditional(STRING_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, STRING_TEMPLATE)
                .optional(TINYINT_FAKE_MODE)
                .conditional(TINYINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, TINYINT_TEMPLATE)
                .optional(SMALLINT_FAKE_MODE)
                .conditional(SMALLINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, SMALLINT_TEMPLATE)
                .optional(INT_FAKE_MODE)
                .conditional(INT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, INT_TEMPLATE)
                .optional(BIGINT_FAKE_MODE)
                .conditional(BIGINT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, BIGINT_TEMPLATE)
                .optional(FLOAT_FAKE_MODE)
                .conditional(FLOAT_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, FLOAT_TEMPLATE)
                .optional(DOUBLE_FAKE_MODE)
                .conditional(DOUBLE_FAKE_MODE, FakeOption.FakeMode.TEMPLATE, DOUBLE_TEMPLATE)
                .optional(
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
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new FakeSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return FakeSource.class;
    }
}
