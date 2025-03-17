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

package org.apache.seatunnel.connectors.seatunnel.influxdb.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.DATABASES;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_MEASUREMENT;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_TAGS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.RETRY_BACKOFF_MULTIPLIER_MS;

@AutoService(Factory.class)
@Slf4j
public class InfluxDBSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DATABASES)
                .bundled(USERNAME, PASSWORD)
                .optional(
                        CONNECT_TIMEOUT_MS,
                        KEY_MEASUREMENT,
                        KEY_TAGS,
                        KEY_TIME,
                        BATCH_SIZE,
                        MAX_RETRIES,
                        RETRY_BACKOFF_MULTIPLIER_MS,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        if (!config.getOptional(KEY_MEASUREMENT).isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(KEY_MEASUREMENT.key(), catalogTable.getTableId().toTablePath().getFullName());
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        SinkConfig sinkConfig = new SinkConfig(config.toConfig());
        return () -> new InfluxDBSink(sinkConfig, catalogTable);
    }
}
