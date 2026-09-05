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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.CONSISTENCY_LEVEL;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.CQL;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.DATACENTER;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.KEYSPACE;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.USERNAME;

@AutoService(Factory.class)
public class CassandraSourceFactory implements TableSourceFactory {

    private static final String CONSISTENCY_LEVEL_REGEX =
            "^(ANY|ONE|TWO|THREE|QUORUM|ALL|LOCAL_QUORUM|EACH_QUORUM|SERIAL|LOCAL_SERIAL|LOCAL_ONE)$";

    @Override
    public String factoryIdentifier() {
        return "Cassandra";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, Conditions.notBlank(HOST))
                .required(KEYSPACE, Conditions.notBlank(KEYSPACE))
                .exclusive(CQL, ConnectorCommonOptions.TABLE_CONFIGS)
                .bundled(USERNAME, PASSWORD)
                .optional(CQL, Conditions.notBlank(CQL))
                .optional(
                        ConnectorCommonOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_CONFIGS),
                        Conditions.extension(
                                ConnectorCommonOptions.TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(DATACENTER)
                .optional(
                        CONSISTENCY_LEVEL,
                        Conditions.matches(CONSISTENCY_LEVEL, CONSISTENCY_LEVEL_REGEX))
                .build();
    }

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each 'tables_configs' entry must contain a non-blank 'cql'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
                throws OptionValidationException {
            if (entries == null || entries.isEmpty()) {
                return true;
            }
            for (int i = 0; i < entries.size(); i++) {
                Map<String, Object> tableConfig = entries.get(i);
                Object cql = tableConfig == null ? null : tableConfig.get(CQL.key());
                if (!(cql instanceof String) || ((String) cql).trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'cql' must not be blank", i);
                }
            }
            return true;
        }
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CassandraParameters cassandraParameters = new CassandraParameters();
        cassandraParameters.buildWithConfig(context.getOptions());
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new CassandraSource(cassandraParameters, context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CassandraSource.class;
    }
}
