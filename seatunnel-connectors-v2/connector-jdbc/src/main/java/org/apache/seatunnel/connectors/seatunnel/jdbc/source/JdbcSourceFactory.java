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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
@AutoService(Factory.class)
public class JdbcSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        JdbcSourceConfig config = JdbcSourceConfig.of(context.getOptions());
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getUrl(),
                        config.getJdbcConnectionConfig().getDialect(),
                        config.getJdbcConnectionConfig().getCompatibleMode(),
                        config.getJdbcConnectionConfig());
        jdbcDialect.connectionUrlParse(
                config.getJdbcConnectionConfig().getUrl(),
                config.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new JdbcSource(config);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(JdbcSourceOptions.URL, JdbcSourceOptions.DRIVER)
                .optional(
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        JdbcSourceOptions.CONNECTION_CHECK_TIMEOUT_SEC,
                        JdbcSourceOptions.FETCH_SIZE,
                        JdbcSourceOptions.PARTITION_COLUMN,
                        JdbcSourceOptions.PARTITION_UPPER_BOUND,
                        JdbcSourceOptions.PARTITION_LOWER_BOUND,
                        JdbcSourceOptions.PARTITION_NUM,
                        JdbcSourceOptions.COMPATIBLE_MODE,
                        JdbcSourceOptions.STRING_SPLIT_MODE,
                        JdbcSourceOptions.STRING_SPLIT_MODE_COLLATE,
                        JdbcSourceOptions.PROPERTIES,
                        JdbcSourceOptions.QUERY,
                        JdbcSourceOptions.USE_SELECT_COUNT,
                        JdbcSourceOptions.SKIP_ANALYZE,
                        JdbcSourceOptions.USE_REGEX,
                        JdbcSourceOptions.TABLE_PATH,
                        JdbcSourceOptions.WHERE_CONDITION,
                        JdbcSourceOptions.TABLE_LIST,
                        JdbcSourceOptions.SPLIT_SIZE,
                        JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        JdbcSourceOptions.SPLIT_SAMPLE_SHARDING_THRESHOLD,
                        JdbcSourceOptions.SPLIT_INVERSE_SAMPLING_RATE,
                        JdbcSourceOptions.DECIMAL_TYPE_NARROWING,
                        JdbcSourceOptions.INT_TYPE_NARROWING,
                        JdbcSourceOptions.DIALECT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return JdbcSource.class;
    }
}
