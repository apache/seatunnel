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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class StarRocksSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        StarRocksSourceOptions.NODE_URLS,
                        StarRocksSourceOptions.USERNAME,
                        StarRocksSourceOptions.PASSWORD,
                        StarRocksSourceOptions.DATABASE,
                        StarRocksSourceOptions.TABLE,
                        SinkConnectorCommonOptions.SCHEMA)
                .optional(
                        StarRocksSourceOptions.MAX_RETRIES,
                        StarRocksSourceOptions.QUERY_TABLET_SIZE,
                        StarRocksSourceOptions.SCAN_FILTER,
                        StarRocksSourceOptions.SCAN_MEM_LIMIT,
                        StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC,
                        StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN,
                        StarRocksSourceOptions.SCAN_BATCH_ROWS,
                        StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return StarRocksSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        SourceConfig starRocksSourceConfig = new SourceConfig(config);
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(config);
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new StarRocksSource(starRocksSourceConfig, catalogTable);
    }
}
