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

package org.apache.seatunnel.connectors.seatunnel.iotdb.sink;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.CommonConfig.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.BATCH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.CONNECTION_TIMEOUT_IN_MS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.DEFAULT_THRIFT_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.ENABLE_RPC_COMPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.KEY_DEVICE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.KEY_MEASUREMENT_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.KEY_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.MAX_RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.MAX_THRIFT_FRAME_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.RETRY_BACKOFF_MULTIPLIER_MS;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.STORAGE_GROUP;
import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig.ZONE_ID;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class IoTDBSinkFactory implements TableSinkFactory{
    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(NODE_URLS, USERNAME, PASSWORD, KEY_DEVICE)
                .optional(KEY_TIMESTAMP, KEY_MEASUREMENT_FIELDS, STORAGE_GROUP, BATCH_SIZE, BATCH_INTERVAL_MS,
                        MAX_RETRIES, RETRY_BACKOFF_MULTIPLIER_MS, MAX_RETRY_BACKOFF_MS, DEFAULT_THRIFT_BUFFER_SIZE,
                        MAX_THRIFT_FRAME_SIZE, ZONE_ID, ENABLE_RPC_COMPRESSION, CONNECTION_TIMEOUT_IN_MS)
                .build();
    }
}
