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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.apache.kafka.connect.data.Struct;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OracleIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "Oracle-CDC";

    public OracleIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OracleSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return OracleSourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        configFactory.schemaList(config.get(OracleSourceOptions.SCHEMA_NAMES));
        configFactory.useSelectCount(config.get(OracleSourceOptions.USE_SELECT_COUNT));
        configFactory.skipAnalyze(config.get(OracleSourceOptions.SKIP_ANALYZE));
        configFactory.originUrl(config.get(JdbcCatalogOptions.BASE_URL));
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        // todo:table metadata change reservation
        Map<TableId, Struct> tableIdStructMap = tableChanges();
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
        }

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OracleDialect((OracleSourceConfigFactory) configFactory, catalogTables);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new RedoLogOffsetFactory(
                (OracleSourceConfigFactory) configFactory, (OracleDialect) dataSourceDialect);
    }

    private Map<TableId, Struct> tableChanges() {
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        OracleDialect dialect =
                new OracleDialect((OracleSourceConfigFactory) configFactory, catalogTables);
        List<TableId> discoverTables = dialect.discoverDataCollections(jdbcSourceConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        ConnectTableChangeSerializer connectTableChangeSerializer =
                new ConnectTableChangeSerializer(schemaNameAdjuster);
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(jdbcSourceConfig)) {
            return discoverTables.stream()
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    (tableId) -> {
                                        TableChanges tableChanges = new TableChanges();
                                        tableChanges.create(
                                                dialect.queryTableSchema(jdbcConnection, tableId)
                                                        .getTable());
                                        return connectTableChangeSerializer
                                                .serialize(tableChanges)
                                                .get(0);
                                    }));
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }
}
