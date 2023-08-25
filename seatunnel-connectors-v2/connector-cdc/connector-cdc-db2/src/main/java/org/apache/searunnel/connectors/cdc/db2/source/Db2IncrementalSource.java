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

package org.apache.searunnel.connectors.cdc.db2.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
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
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfig;
import org.apache.searunnel.connectors.cdc.db2.config.Db2SourceConfigFactory;
import org.apache.searunnel.connectors.cdc.db2.source.offset.LsnOffsetFactory;

import com.google.auto.service.AutoService;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import lombok.NoArgsConstructor;

import java.time.ZoneId;

import static org.apache.searunnel.connectors.cdc.db2.utils.Db2ConnectionUtils.createDb2Connection;
import static org.apache.searunnel.connectors.cdc.db2.utils.Db2TypeUtils.convertFromTable;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class Db2IncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "Db2-CDC";

    public Db2IncrementalSource(ReadonlyConfig options, SeaTunnelDataType<SeaTunnelRow> dataType) {
        super(options, dataType);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return Db2SourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return Db2SourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        Db2SourceConfigFactory configFactory = new Db2SourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo(config.get(JdbcCatalogOptions.BASE_URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(urlInfo.getPort());
        return configFactory;
    }

    @SuppressWarnings("unchecked")
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
            Db2SourceConfig Db2SourceConfig = (Db2SourceConfig) this.configFactory.create(0);
            TableId tableId =
                    this.dataSourceDialect.discoverDataCollections(Db2SourceConfig).get(0);
            Table table;
            try (Db2Connection db2Connection =
                    createDb2Connection(Db2SourceConfig.getDbzConfiguration())) {
                table =
                        ((Db2Dialect) dataSourceDialect)
                                .queryTableSchema(db2Connection, tableId)
                                .getTable();
            } catch (Exception e) {
                throw new SeaTunnelException(e);
            }
            physicalRowType = convertFromTable(table);
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
        return new Db2Dialect((Db2SourceConfigFactory) configFactory);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new LsnOffsetFactory(
                (Db2SourceConfigFactory) configFactory, (Db2Dialect) dataSourceDialect);
    }
}
