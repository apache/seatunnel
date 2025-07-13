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
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

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
                .required(InfluxDBSinkOptions.URL, InfluxDBSinkOptions.DATABASES)
                .bundled(InfluxDBSinkOptions.USERNAME, InfluxDBSinkOptions.PASSWORD)
                .optional(
                        InfluxDBSinkOptions.CONNECT_TIMEOUT_MS,
                        InfluxDBSinkOptions.KEY_MEASUREMENT,
                        InfluxDBSinkOptions.KEY_TAGS,
                        InfluxDBSinkOptions.KEY_TIME,
                        InfluxDBSinkOptions.BATCH_SIZE,
                        InfluxDBSinkOptions.MAX_RETRIES,
                        InfluxDBSinkOptions.WRITE_TIMEOUT,
                        InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS,
                        InfluxDBSinkOptions.RETENTION_POLICY,
                        InfluxDBSinkOptions.QUERY_TIMEOUT_SEC,
                        InfluxDBSinkOptions.EPOCH,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        if (!config.getOptional(InfluxDBSinkOptions.KEY_MEASUREMENT).isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(
                    InfluxDBSinkOptions.KEY_MEASUREMENT.key(),
                    catalogTable.getTableId().toTablePath().getFullName());
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        SinkConfig sinkConfig = new SinkConfig(config);
        return () -> new InfluxDBSink(sinkConfig, catalogTable);
    }
}
