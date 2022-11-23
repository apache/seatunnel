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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;

import java.util.List;

@Getter
public class SourceConfig extends InfluxDBConfig {

    public static final Option<String> SQL = Options.key("sql")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server query sql");

    public static final Option<String> SQL_WHERE = Options.key("where")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server query sql where condition");

    public static final Option<String> SPLIT_COLUMN = Options.key("split_column")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb column which is used as split key");

    public static final Option<String> PARTITION_NUM = Options.key("partition_num")
        .stringType()
        .defaultValue("0")
        .withDescription("the influxdb server partition num");

    public static final Option<String> UPPER_BOUND = Options.key("upper_bound")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server upper bound");

    public static final Option<String> LOWER_BOUND = Options.key("lower_bound")
        .stringType()
        .noDefaultValue()
        .withDescription("the influxdb server lower bound");

    public static final String DEFAULT_PARTITIONS = PARTITION_NUM.defaultValue();
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

        sourceConfig.sql = config.getString(SQL.key());

        if (config.hasPath(PARTITION_NUM.key())) {
            sourceConfig.partitionNum = config.getInt(PARTITION_NUM.key());
        }
        if (config.hasPath(UPPER_BOUND.key())) {
            sourceConfig.upperBound = config.getInt(UPPER_BOUND.key());
        }
        if (config.hasPath(LOWER_BOUND.key())) {
            sourceConfig.lowerBound = config.getInt(LOWER_BOUND.key());
        }
        if (config.hasPath(SPLIT_COLUMN.key())) {
            sourceConfig.splitKey = config.getString(SPLIT_COLUMN.key());
        }
        return sourceConfig;
    }

}
