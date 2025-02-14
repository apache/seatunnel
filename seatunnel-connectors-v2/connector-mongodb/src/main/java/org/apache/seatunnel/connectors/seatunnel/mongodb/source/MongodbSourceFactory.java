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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class MongodbSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongodbConfig.URI,
                        MongodbConfig.DATABASE,
                        MongodbConfig.COLLECTION,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        MongodbConfig.PROJECTION,
                        MongodbConfig.MATCH_QUERY,
                        MongodbConfig.SPLIT_SIZE,
                        MongodbConfig.SPLIT_KEY,
                        MongodbConfig.CURSOR_NO_TIMEOUT,
                        MongodbConfig.FETCH_SIZE,
                        MongodbConfig.MAX_TIME_MIN)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>>
            getSourceClass() {
        return MongodbSource.class;
    }

    @Override
    public TableSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>> createSource(
            TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig options = context.getOptions();
            CatalogTable table;
            if (options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                table = CatalogTableUtil.buildWithConfig(options);
            } else {
                table = CatalogTableUtil.buildSimpleTextTable();
            }
            return new MongodbSource(table, options);
        };
    }
}
