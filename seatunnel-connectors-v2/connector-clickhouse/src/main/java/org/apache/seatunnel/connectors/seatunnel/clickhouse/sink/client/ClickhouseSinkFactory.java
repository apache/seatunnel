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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.CLICKHOUSE_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.HOST;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.BULK_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SHARDING_KEY;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SPLIT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.SUPPORT_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSinkOptions.TABLE;

@AutoService(Factory.class)
public class ClickhouseSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new ClickhouseSink(catalogTable, readonlyConfig);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE, USERNAME, PASSWORD)
                .optional(
                        SERVER_TIME_ZONE,
                        CLICKHOUSE_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE,
                        SCHEMA_SAVE_MODE,
                        DATA_SAVE_MODE,
                        CUSTOM_SQL,
                        SAVE_MODE_CREATE_TEMPLATE)
                .build();
    }
}
