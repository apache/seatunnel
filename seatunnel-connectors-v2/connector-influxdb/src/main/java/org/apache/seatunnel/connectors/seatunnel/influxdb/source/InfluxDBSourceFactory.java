/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class InfluxDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        InfluxDBSourceOptions.URL,
                        InfluxDBSourceOptions.SQL,
                        InfluxDBSourceOptions.DATABASES,
                        ConnectorCommonOptions.SCHEMA)
                .bundled(InfluxDBSourceOptions.USERNAME, InfluxDBSourceOptions.PASSWORD)
                .bundled(
                        InfluxDBSourceOptions.LOWER_BOUND,
                        InfluxDBSourceOptions.UPPER_BOUND,
                        InfluxDBSourceOptions.PARTITION_NUM,
                        InfluxDBSourceOptions.SPLIT_COLUMN)
                .optional(
                        InfluxDBSourceOptions.EPOCH,
                        InfluxDBSourceOptions.SQL_WHERE,
                        InfluxDBSourceOptions.CONNECT_TIMEOUT_MS,
                        InfluxDBSourceOptions.QUERY_TIMEOUT_SEC)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new InfluxDBSource(
                                CatalogTableUtil.buildWithConfig(context.getOptions()),
                                SourceConfig.loadConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InfluxDBSource.class;
    }
}
