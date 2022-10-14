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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class InfluxDBConfig implements Serializable {

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String URL = "url";
    private static final String CONNECT_TIMEOUT_MS = "connect_timeout_ms";
    private static final String QUERY_TIMEOUT_SEC = "query_timeout_sec";

    public static final String SQL = "sql";
    public static final String SQL_WHERE = "where";

    public static final String DATABASES = "database";
    public static final String SPLIT_COLUMN = "split_column";
    private static final String PARTITION_NUM = "partition_num";
    private static final String UPPER_BOUND = "upper_bound";
    private static final String LOWER_BOUND = "lower_bound";


    private static final String DEFAULT_FORMAT = "MSGPACK";
    private static final String EPOCH = "epoch";

    public static final String DEFAULT_PARTITIONS = "0";
    private static final int DEFAULT_QUERY_TIMEOUT_SEC = 3;
    private static final long DEFAULT_CONNECT_TIMEOUT_MS = 15000;

    private static final String DEFAULT_EPOCH = "n";

    private String url;
    private String username;
    private String password;
    private String sql;
    private int partitionNum = 0;
    private String splitKey;
    private long lowerBound;
    private long upperBound;
    private String database;

    private String format = DEFAULT_FORMAT;
    private int queryTimeOut = DEFAULT_QUERY_TIMEOUT_SEC;
    private long connectTimeOut = DEFAULT_CONNECT_TIMEOUT_MS;

    private String epoch = DEFAULT_EPOCH;

    List<Integer> columnsIndex;

    public InfluxDBConfig(Config config) {
        this.url = config.getString(URL);
        this.sql = config.getString(SQL);

        if (config.hasPath(USERNAME)) {
            this.username = config.getString(USERNAME);
        }
        if (config.hasPath(PASSWORD)) {
            this.password = config.getString(PASSWORD);
        }
        if (config.hasPath(PARTITION_NUM)) {
            this.partitionNum = config.getInt(PARTITION_NUM);
        }
        if (config.hasPath(UPPER_BOUND)) {
            this.upperBound = config.getInt(UPPER_BOUND);
        }
        if (config.hasPath(LOWER_BOUND)) {
            this.lowerBound = config.getInt(LOWER_BOUND);
        }
        if (config.hasPath(SPLIT_COLUMN)) {
            this.splitKey = config.getString(SPLIT_COLUMN);
        }
        if (config.hasPath(DATABASES)) {
            this.database = config.getString(DATABASES);
        }
        if (config.hasPath(EPOCH)) {
            this.epoch = config.getString(EPOCH);
        }
        if (config.hasPath(CONNECT_TIMEOUT_MS)) {
            this.connectTimeOut = config.getLong(CONNECT_TIMEOUT_MS);
        }
        if (config.hasPath(QUERY_TIMEOUT_SEC)) {
            this.queryTimeOut = config.getInt(QUERY_TIMEOUT_SEC);
        }
    }

    @VisibleForTesting
    public InfluxDBConfig(String url) {
        this.url = url;
    }
}
