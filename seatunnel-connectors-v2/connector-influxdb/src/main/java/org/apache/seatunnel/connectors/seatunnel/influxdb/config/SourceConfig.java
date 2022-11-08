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

import lombok.Getter;

import java.util.List;

@Getter
public class SourceConfig extends InfluxDBConfig{
    public static final String SQL = "sql";
    public static final String SQL_WHERE = "where";
    public static final String SPLIT_COLUMN = "split_column";
    private static final String PARTITION_NUM = "partition_num";
    private static final String UPPER_BOUND = "upper_bound";
    private static final String LOWER_BOUND = "lower_bound";
    public static final String DEFAULT_PARTITIONS = "0";
    private String sql;
    private int partitionNum = 0;
    private String splitKey;
    private long lowerBound;
    private long upperBound;

    List<Integer> columnsIndex;

    public SourceConfig(Config config) {
        super(config);
    }

    public static SourceConfig loadConfig(Config config) {
        SourceConfig sourceConfig = new SourceConfig(config);

        sourceConfig.sql = config.getString(SQL);

        if (config.hasPath(PARTITION_NUM)) {
            sourceConfig.partitionNum = config.getInt(PARTITION_NUM);
        }
        if (config.hasPath(UPPER_BOUND)) {
            sourceConfig.upperBound = config.getInt(UPPER_BOUND);
        }
        if (config.hasPath(LOWER_BOUND)) {
            sourceConfig.lowerBound = config.getInt(LOWER_BOUND);
        }
        if (config.hasPath(SPLIT_COLUMN)) {
            sourceConfig.splitKey = config.getString(SPLIT_COLUMN);
        }
        return sourceConfig;
    }

}
