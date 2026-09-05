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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.MySqlIncrementalSourceFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Factory for the OceanBase CDC source.
 *
 * <p>The first implementation deliberately wraps the MySQL CDC connector because that is the stable
 * and testable path already adopted by Flink CDC for OceanBase Binlog Service.
 */
@AutoService(Factory.class)
@Slf4j
public class OceanBaseIncrementalSourceFactory extends MySqlIncrementalSourceFactory {

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
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
            }
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(config, context.getClassLoader());
            boolean enableSchemaChange =
                    context.getOptions()
                            .getOptional(SourceOptions.SCHEMA_CHANGES_ENABLED)
                            .orElse(
                                    context.getOptions()
                                            .getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                                            .map(
                                                    e ->
                                                            e.getOrDefault(
                                                                    MySqlSourceConfigFactory
                                                                            .SCHEMA_CHANGE_KEY,
                                                                    SourceOptions
                                                                            .SCHEMA_CHANGES_ENABLED
                                                                            .defaultValue()
                                                                            .toString()))
                                            .map(Boolean::parseBoolean)
                                            .orElse(
                                                    SourceOptions.SCHEMA_CHANGES_ENABLED
                                                            .defaultValue()));
            if (!restoreTables.isEmpty() && enableSchemaChange) {
                catalogTables = mergeTableStruct(catalogTables, restoreTables);
            }

            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions().getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, false));
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new OceanBaseIncrementalSource<>(config, catalogTables);
        };
    }
}
