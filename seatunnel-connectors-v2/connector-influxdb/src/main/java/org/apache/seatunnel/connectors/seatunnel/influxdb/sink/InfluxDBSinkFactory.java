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

import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.DATABASES;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.URL;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_MEASUREMENT;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_TAGS;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.KEY_TIME;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig.RETRY_BACKOFF_MULTIPLIER_MS;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class InfluxDBSinkFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(
                URL,
                DATABASES,
                KEY_MEASUREMENT
            )
            .bundled(USERNAME, PASSWORD)
            .optional(
                CONNECT_TIMEOUT_MS,
                KEY_TAGS,
                KEY_TIME,
                BATCH_SIZE,
                BATCH_INTERVAL_MS,
                MAX_RETRIES,
                RETRY_BACKOFF_MULTIPLIER_MS
            )
            .build();
    }
}
