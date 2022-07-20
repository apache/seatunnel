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

package org.apache.seatunnel.connectors.seatunnel.tidb.config;

import static org.apache.seatunnel.connectors.seatunnel.tidb.config.JdbcConfig.buildJdbcConnectionOptions;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

@Data
@AllArgsConstructor
public class TiDBSourceOptions implements Serializable {
    private JdbcConnectionOptions jdbcConnectionOptions;
    private String partitionColumn;
    private Long partitionUpperBound;
    private Long partitionLowerBound;

    private Integer parallelism;

    public TiDBSourceOptions(Config config) {
        this.jdbcConnectionOptions = buildJdbcConnectionOptions(config);
        if (config.hasPath(JdbcConfig.PARTITION_COLUMN)) {
            this.partitionColumn = config.getString(JdbcConfig.PARTITION_COLUMN);
        }
        if (config.hasPath(JdbcConfig.PARTITION_UPPER_BOUND)) {
            this.partitionUpperBound = config.getLong(JdbcConfig.PARTITION_UPPER_BOUND);
        }
        if (config.hasPath(JdbcConfig.PARTITION_LOWER_BOUND)) {
            this.partitionLowerBound = config.getLong(JdbcConfig.PARTITION_LOWER_BOUND);
        }
        if (config.hasPath(JdbcConfig.PARALLELISM)) {
            this.parallelism = config.getInt(JdbcConfig.PARALLELISM);
        }
    }

    public JdbcConnectionOptions getJdbcConnectionOptions() {
        return jdbcConnectionOptions;
    }

    public Optional<String> getPartitionColumn() {
        return Optional.ofNullable(partitionColumn);
    }

    public Optional<Long> getPartitionUpperBound() {
        return Optional.ofNullable(partitionUpperBound);
    }

    public Optional<Long> getPartitionLowerBound() {
        return Optional.ofNullable(partitionLowerBound);
    }

    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(parallelism);
    }
}
