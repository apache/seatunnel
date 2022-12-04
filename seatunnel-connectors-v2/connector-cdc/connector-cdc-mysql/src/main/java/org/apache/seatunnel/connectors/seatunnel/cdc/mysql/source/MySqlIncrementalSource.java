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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.MySqlCatalog;

import com.google.auto.service.AutoService;

import java.time.ZoneId;

@AutoService(SeaTunnelSource.class)
public class MySqlIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> {
    @Override
    public String getPluginName() {
        return "MySQL-CDC";
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.serverId(config.get(JdbcSourceOptions.SERVER_ID));
        configFactory.fromReadonlyConfig(readonlyConfig);
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config) {
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        String baseUrl = config.get(JdbcCatalogOptions.BASE_URL);
        // TODO: support multi-table
        // TODO: support metadata keys
        MySqlCatalog mySqlCatalog = new MySqlCatalog("mysql", jdbcSourceConfig.getDatabaseList().get(0), jdbcSourceConfig.getUsername(), jdbcSourceConfig.getPassword(), baseUrl);
        CatalogTable table = mySqlCatalog.getTable(TablePath.of(jdbcSourceConfig.getDatabaseList().get(0), jdbcSourceConfig.getTableList().get(0)));
        SeaTunnelRowType physicalRowType = table.getTableSchema().toPhysicalRowDataType();
        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>) SeaTunnelRowDebeziumDeserializeSchema.builder()
            .setPhysicalRowType(physicalRowType)
            .setResultTypeInfo(physicalRowType)
            .setServerTimeZone(ZoneId.of(zoneId))
            .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new MySqlDialect((MySqlSourceConfigFactory) configFactory);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new BinlogOffsetFactory((MySqlSourceConfigFactory) configFactory, (JdbcDataSourceDialect) dataSourceDialect);
    }
}
