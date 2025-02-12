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
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

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
                .required(URL, SQL, DATABASES, ConnectorCommonOptions.SCHEMA)
                .bundled(USERNAME, PASSWORD)
                .bundled(LOWER_BOUND, UPPER_BOUND, PARTITION_NUM, SPLIT_COLUMN)
                .optional(EPOCH, CONNECT_TIMEOUT_MS, QUERY_TIMEOUT_SEC)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InfluxDBSource.class;
    }
}
