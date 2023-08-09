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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Optional;

@Data
@Builder(builderClassName = "Builder")
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    public String query;
    public String compatibleMode;
    private String partitionColumn;
    private BigDecimal partitionUpperBound;
    private BigDecimal partitionLowerBound;
    private int fetchSize;
    private Integer partitionNumber;

    public static JdbcSourceConfig of(ReadonlyConfig config) {
        JdbcSourceConfig.Builder builder = JdbcSourceConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.query(config.get(JdbcOptions.QUERY));
        builder.fetchSize(config.get(JdbcOptions.FETCH_SIZE));
        config.getOptional(JdbcOptions.COMPATIBLE_MODE).ifPresent(builder::compatibleMode);
        config.getOptional(JdbcOptions.PARTITION_COLUMN).ifPresent(builder::partitionColumn);
        config.getOptional(JdbcOptions.PARTITION_UPPER_BOUND)
                .ifPresent(builder::partitionUpperBound);
        config.getOptional(JdbcOptions.PARTITION_LOWER_BOUND)
                .ifPresent(builder::partitionLowerBound);
        config.getOptional(JdbcOptions.PARTITION_NUM).ifPresent(builder::partitionNumber);
        return builder.build();
    }

    public JdbcConnectionConfig getJdbcConnectionConfig() {
        return jdbcConnectionConfig;
    }

    public Optional<String> getPartitionColumn() {
        return Optional.ofNullable(partitionColumn);
    }

    public Optional<BigDecimal> getPartitionUpperBound() {
        return Optional.ofNullable(partitionUpperBound);
    }

    public Optional<BigDecimal> getPartitionLowerBound() {
        return Optional.ofNullable(partitionLowerBound);
    }

    public Optional<Integer> getPartitionNumber() {
        return Optional.ofNullable(partitionNumber);
    }

    public int getFetchSize() {
        return fetchSize;
    }
}
