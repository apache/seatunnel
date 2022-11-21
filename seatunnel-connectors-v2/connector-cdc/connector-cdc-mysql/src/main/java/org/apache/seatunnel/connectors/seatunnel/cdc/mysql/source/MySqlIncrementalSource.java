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
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffsetFactory;

public class MySqlIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> {
    @Override
    public String getPluginName() {
        return "MySQL-CDC";
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.serverId(config.get(MySqlSourceOptions.SERVER_ID));
        return configFactory;
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new BinlogOffsetFactory((MySqlSourceConfigFactory) configFactory);
    }

    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(ReadonlyConfig config) {
        // TODO: seatunnel row
        return null;
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new MySqlDialect((MySqlSourceConfigFactory) configFactory);
    }
}
