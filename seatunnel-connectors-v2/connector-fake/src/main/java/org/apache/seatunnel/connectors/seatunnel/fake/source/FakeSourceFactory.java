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
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ARRAY_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BIGINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BINARY_VECTOR_DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.BYTES_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_DAY_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_MONTH_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DATE_YEAR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.DOUBLE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.FLOAT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.INT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.MAP_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ROWS;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.ROW_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SMALLINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SPLIT_NUM;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.SPLIT_READ_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.STRING_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.STRING_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_HOUR_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_MINUTE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TIME_SECOND_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_FAKE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.TINYINT_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions.VECTOR_DIMENSION;

@AutoService(Factory.class)
public class FakeSourceFactory implements TableSourceFactory {
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
