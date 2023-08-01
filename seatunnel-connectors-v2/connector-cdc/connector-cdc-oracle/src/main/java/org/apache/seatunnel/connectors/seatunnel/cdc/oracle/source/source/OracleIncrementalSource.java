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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
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
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.offset.RedoLogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils.OracleConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils.OracleTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import com.google.auto.service.AutoService;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.NoArgsConstructor;

import java.time.ZoneId;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class OracleIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "Oracle-CDC";

    public OracleIncrementalSource(
            ReadonlyConfig options, SeaTunnelDataType<SeaTunnelRow> dataType) {
        super(options, dataType);
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
        configFactory.originUrl(config.get(JdbcCatalogOptions.BASE_URL));
        configFactory.chunkKeyColumn(
                config.get(OracleSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN));
        return configFactory;
    }

    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
        }
        SeaTunnelDataType<SeaTunnelRow> physicalRowType;
        if (dataType == null) {
            OracleSourceConfig oracleSourceConfig =
                    (OracleSourceConfig) this.configFactory.create(0);
            TableId tableId =
                    this.dataSourceDialect.discoverDataCollections(oracleSourceConfig).get(0);
            Table table;
            try (OracleConnection oracleConnection =
                    OracleConnectionUtils.createOracleConnection(
                            oracleSourceConfig.getDbzConfiguration())) {
                table =
                        ((OracleDialect) dataSourceDialect)
                                .queryTableSchema(oracleConnection, tableId)
                                .getTable();
            } catch (Exception e) {
                throw new SeaTunnelException(e);
            }
            physicalRowType = OracleTypeUtils.convertFromTable(table);
        } else {
            physicalRowType = dataType;
        }
        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setPhysicalRowType(physicalRowType)
                        .setResultTypeInfo(physicalRowType)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OracleDialect((OracleSourceConfigFactory) configFactory);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new RedoLogOffsetFactory(
                (OracleSourceConfigFactory) configFactory, (OracleDialect) dataSourceDialect);
    }
}
