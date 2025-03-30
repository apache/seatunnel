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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;

import java.util.List;

@Getter
public class SourceConfig extends InfluxDBConfig {

    public static final int DEFAULT_PARTITIONS = InfluxDBSourceOptions.PARTITION_NUM.defaultValue();
    private String sql;
    private int partitionNum = 0;
    private String splitKey;
    private long lowerBound;
    private long upperBound;

    List<Integer> columnsIndex;

    public SourceConfig(ReadonlyConfig config) {
        super(config);
    }

    public static SourceConfig loadConfig(ReadonlyConfig config) {
        SourceConfig sourceConfig = new SourceConfig(config);
        sourceConfig.sql = config.get(InfluxDBSourceOptions.SQL);
        sourceConfig.partitionNum = config.get(InfluxDBSourceOptions.PARTITION_NUM);
        if (config.getOptional(InfluxDBSourceOptions.UPPER_BOUND).isPresent()) {
            sourceConfig.upperBound = config.get(InfluxDBSourceOptions.UPPER_BOUND);
        }
        if (config.getOptional(InfluxDBSourceOptions.LOWER_BOUND).isPresent()) {
            sourceConfig.lowerBound = config.get(InfluxDBSourceOptions.LOWER_BOUND);
        }
        if (config.getOptional(InfluxDBSourceOptions.SPLIT_COLUMN).isPresent()) {
            sourceConfig.splitKey = config.get(InfluxDBSourceOptions.SPLIT_COLUMN);
        }
        return sourceConfig;
    }
}
