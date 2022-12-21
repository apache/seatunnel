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

package org.apache.seatunnel.connectors.cdc.dameng.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffsetFactory;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;

import com.google.auto.service.AutoService;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.time.ZoneId;

@AutoService(SeaTunnelSource.class)
public class DamengIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> {
    static final String IDENTIFIER = "Dameng-CDC";

    private DamengSourceConfig sourceConfig;
    private DamengPooledDataSourceFactory connectionPoolFactory;

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        DamengSourceConfigFactory configFactory = new DamengSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);

        this.sourceConfig = configFactory.create(0);

        return configFactory;
    }

    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config) {
        // TODO: support multi-table
        TableId tableId = dataSourceDialect.discoverDataCollections(sourceConfig).get(0);
        Configuration jdbcConfig = sourceConfig.getDbzConnectorConfig().getJdbcConfig();
        try (DamengConnection damengConnection = new DamengConnection(jdbcConfig)) {
            Table table = ((DamengDialect) dataSourceDialect).queryTableSchema(damengConnection, tableId).getTable();
            SeaTunnelRowType seaTunnelRowType = DamengUtils.convert(table);
            return (DebeziumDeserializationSchema<T>) SeaTunnelRowDebeziumDeserializeSchema.builder()
                .setPhysicalRowType(seaTunnelRowType)
                .setResultTypeInfo(seaTunnelRowType)
                .setServerTimeZone(ZoneId.of(config.get(JdbcSourceOptions.SERVER_TIME_ZONE)))
                .build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DamengDialect createDataSourceDialect(ReadonlyConfig config) {
        return new DamengDialect(sourceConfig);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new LogMinerOffsetFactory(sourceConfig, (DamengDialect) dataSourceDialect);
    }
}
