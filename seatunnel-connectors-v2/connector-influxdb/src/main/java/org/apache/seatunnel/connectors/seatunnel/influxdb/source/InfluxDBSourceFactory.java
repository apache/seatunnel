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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.DATABASES;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.EPOCH;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.QUERY_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.PARTITION_NUM;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.SPLIT_COLUMN;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.TABLE_CONFIGS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig.UPPER_BOUND;

@AutoService(Factory.class)
public class InfluxDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(URL)
                .optional(SQL)
                .optional(DATABASES)
                .optional(TableSchemaOptions.SCHEMA)
                .optional(USERNAME)
                .optional(PASSWORD)
                .optional(LOWER_BOUND)
                .optional(UPPER_BOUND)
                .optional(PARTITION_NUM)
                .optional(SPLIT_COLUMN)
                .optional(EPOCH)
                .optional(CONNECT_TIMEOUT_MS)
                .optional(QUERY_TIMEOUT_SEC)
                .optional(TABLE_CONFIGS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InfluxDBSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new InfluxDBSource(context.getOptions());
    }
}
