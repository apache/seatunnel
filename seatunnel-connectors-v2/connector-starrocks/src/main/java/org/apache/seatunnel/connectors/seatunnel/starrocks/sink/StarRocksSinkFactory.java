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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;

import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class StarRocksSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "StarRocks";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(StarRocksOptions.USERNAME, StarRocksOptions.PASSWORD)
                .required(StarRocksSinkOptions.DATABASE, StarRocksOptions.BASE_URL)
                .required(StarRocksSinkOptions.NODE_URLS)
                .optional(
                        StarRocksSinkOptions.TABLE,
                        StarRocksSinkOptions.LABEL_PREFIX,
                        StarRocksSinkOptions.BATCH_MAX_SIZE,
                        StarRocksSinkOptions.BATCH_MAX_BYTES,
                        StarRocksSinkOptions.BATCH_INTERVAL_MS,
                        StarRocksSinkOptions.MAX_RETRIES,
                        StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS,
                        StarRocksSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        StarRocksSinkOptions.STARROCKS_CONFIG,
                        StarRocksSinkOptions.ENABLE_UPSERT_DELETE,
                        StarRocksSinkOptions.SAVE_MODE,
                        StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .build();
    }

    @Override
    public TableSink createSink(TableFactoryContext context) {
        SinkConfig sinkConfig = SinkConfig.of(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        if (StringUtils.isBlank(sinkConfig.getTable())) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }
        return () -> new StarRocksSink(sinkConfig, catalogTable);
    }
}
