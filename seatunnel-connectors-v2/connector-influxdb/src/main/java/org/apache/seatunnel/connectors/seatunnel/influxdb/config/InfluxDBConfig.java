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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;

import java.io.Serializable;

@Data
public class InfluxDBConfig implements Serializable {

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server password");

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server url");

    public static final Option<Long> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .longType()
                    .defaultValue(15000L)
                    .withDescription("the influxdb client connect timeout ms");

    public static final Option<Integer> QUERY_TIMEOUT_SEC =
            Options.key("query_timeout_sec")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the influxdb client query timeout ms");

    public static final Option<String> DATABASES =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server database");

    public static final Option<String> EPOCH =
            Options.key("epoch")
                    .stringType()
                    .defaultValue("n")
                    .withDescription("the influxdb server query epoch");

    private static final String DEFAULT_FORMAT = "MSGPACK";
    private String url;
    private String username;
    private String password;
    private String database;
    private String format = DEFAULT_FORMAT;
    private int queryTimeOut = QUERY_TIMEOUT_SEC.defaultValue();
    private long connectTimeOut = CONNECT_TIMEOUT_MS.defaultValue();
    private String epoch = EPOCH.defaultValue();

    public InfluxDBConfig(ReadonlyConfig config) {
        this.url = config.get(URL);

        if (config.getOptional(USERNAME).isPresent()) {
            this.username = config.get(USERNAME);
        }

        if (config.getOptional(PASSWORD).isPresent()) {
            this.password = config.get(PASSWORD);
        }

        if (config.getOptional(DATABASES).isPresent()) {
            this.database = config.get(DATABASES);
        }

        if (config.getOptional(EPOCH).isPresent()) {
            this.epoch = config.get(EPOCH);
        }

        if (config.getOptional(CONNECT_TIMEOUT_MS).isPresent()) {
            this.connectTimeOut = config.get(CONNECT_TIMEOUT_MS);
        }

        if (config.getOptional(QUERY_TIMEOUT_SEC).isPresent()) {
            this.queryTimeOut = config.get(QUERY_TIMEOUT_SEC);
        }
    }

    @VisibleForTesting
    public InfluxDBConfig(String url) {
        this.url = url;
    }
}
