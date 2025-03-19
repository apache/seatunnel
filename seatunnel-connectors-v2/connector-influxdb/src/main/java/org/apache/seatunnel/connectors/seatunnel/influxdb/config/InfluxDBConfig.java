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

package org.apache.seatunnel.connectors.seatunnel.influxdb.config;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;

@Data
public class InfluxDBConfig implements Serializable {

    private static final String DEFAULT_FORMAT = "MSGPACK";
    private String url;
    private String username;
    private String password;
    private String database;
    private String format = DEFAULT_FORMAT;
    private int queryTimeOut;
    private long connectTimeOut;
    private String epoch;

    public InfluxDBConfig(ReadonlyConfig config) {
        this.url = config.get(InfluxDBCommonOptions.URL);
        this.username = config.get(InfluxDBCommonOptions.USERNAME);
        this.password = config.get(InfluxDBCommonOptions.PASSWORD);
        this.database = config.get(InfluxDBCommonOptions.DATABASES);
        this.epoch = config.get(InfluxDBCommonOptions.EPOCH);
        this.connectTimeOut = config.get(InfluxDBCommonOptions.CONNECT_TIMEOUT_MS);
        this.queryTimeOut = config.get(InfluxDBCommonOptions.QUERY_TIMEOUT_SEC);
    }

    @VisibleForTesting
    public InfluxDBConfig(String url) {
        this.url = url;
    }
}
