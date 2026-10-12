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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory for the OceanBase CDC source.
 *
 * <p>The first implementation deliberately wraps the MySQL CDC connector because that is the stable
 * and testable path already adopted by Flink CDC for OceanBase Binlog Service.
 */
@AutoService(Factory.class)
public class OceanBaseIncrementalSourceFactory extends MySqlIncrementalSourceFactory {

    private static final String MYSQL_COMPATIBLE_MODE = "mysql";

    /**
     * Return the identifier used in SeaTunnel source config.
     *
     * @return OceanBase CDC identifier
     */
    @Override
    public String factoryIdentifier() {
        return OceanBaseIncrementalSource.IDENTIFIER;
    }

    /**
     * Return the concrete source class used for OceanBase CDC jobs.
     *
     * @return OceanBase CDC source class
     */
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OceanBaseIncrementalSource.class;
    }

    /**
     * Allow the OceanBase catalog selector while restricting this MySQL-binlog wrapper to MySQL
     * compatible mode.
     */
    @Override
    public OptionRule optionRule() {
        return getOptionRuleBuilder()
                .optional(
                        JdbcCommonOptions.COMPATIBLE_MODE,
                        Conditions.matches(JdbcCommonOptions.COMPATIBLE_MODE, "(?i)mysql"))
                .build();
    }

    /**
     * Restore the source by reusing MySQL-compatible table discovery and checkpoint merge logic.
     *
     * @param context source factory context
     * @param restoreTables restored table structures from checkpoint state
     * @param <T> emitted record type
     * @param <SplitT> split type
     * @param <StateT> state type
     * @return restorable OceanBase CDC table source
     */
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            ReadonlyConfig config = mysqlCompatibleConfig(context.getOptions());
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new OceanBaseIncrementalSource<>(
                            config, buildCatalogTables(context, config, restoreTables));
        };
    }

    /**
     * Add the catalog selector required by OceanBase catalog discovery when users omit it.
     *
     * <p>Validation permits only MySQL mode because the incremental runtime uses MySQL binlog
     * semantics.
     */
    ReadonlyConfig mysqlCompatibleConfig(ReadonlyConfig config) {
        String compatibleMode =
                config.getOptional(JdbcCommonOptions.COMPATIBLE_MODE).orElse(MYSQL_COMPATIBLE_MODE);
        if (!MYSQL_COMPATIBLE_MODE.equalsIgnoreCase(compatibleMode)) {
            throw new IllegalArgumentException(
                    "OceanBase-CDC supports only MySQL compatible mode, but compatible_mode is "
                            + compatibleMode);
        }
        Map<String, Object> options = new HashMap<>(config.getSourceMap());
        options.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), MYSQL_COMPATIBLE_MODE);
        return ReadonlyConfig.fromMap(options);
    }
}
