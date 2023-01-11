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

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils.createOracleConnection;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleTypeUtils.convertFromTable;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config.OracleSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffsetFactory;

import com.google.auto.service.AutoService;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

import java.time.ZoneId;

@AutoService(SeaTunnelSource.class)
public class OracleIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> implements SupportParallelism {

    static final String IDENTIFIER = "Oracle-CDC";

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config) {
        OracleSourceConfig oracleSourceConfig = (OracleSourceConfig) this.configFactory.create(0);
        TableId tableId = this.dataSourceDialect.discoverDataCollections(oracleSourceConfig).get(0);

        OracleConnectorConfig dbzConnectorConfig = oracleSourceConfig.getDbzConnectorConfig();

        OracleConnection oracleConnection = createOracleConnection(dbzConnectorConfig.getJdbcConfig());

        Table table = ((OracleDialect) dataSourceDialect).queryTableSchema(oracleConnection, tableId).getTable();

        SeaTunnelRowType seaTunnelRowType = convertFromTable(table);

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>) SeaTunnelRowDebeziumDeserializeSchema.builder()
            .setPhysicalRowType(seaTunnelRowType)
            .setResultTypeInfo(seaTunnelRowType)
            .setServerTimeZone(ZoneId.of(zoneId))
            .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OracleDialect((OracleSourceConfigFactory) configFactory);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new RedoLogOffsetFactory((OracleSourceConfigFactory) configFactory, (OracleDialect) dataSourceDialect);
    }
}
