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
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.StringSplitMode;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder(builderClassName = "Builder")
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private List<JdbcSourceTableConfig> tableConfigList;
    private String whereConditionClause;
    public String compatibleMode;
    private int fetchSize;

    private boolean useDynamicSplitter;
    private int splitSize;
    private double splitEvenDistributionFactorUpperBound;
    private double splitEvenDistributionFactorLowerBound;
    private int splitSampleShardingThreshold;
    private int splitInverseSamplingRate;
    private boolean decimalTypeNarrowing;
    private boolean handleBlobAsString;

    private StringSplitMode stringSplitMode;

    private String stringSplitModeCollate;

    public static JdbcSourceConfig of(ReadonlyConfig config) {
        JdbcSourceConfig.Builder builder = JdbcSourceConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.tableConfigList(JdbcSourceTableConfig.of(config));
        builder.fetchSize(config.get(JdbcSourceOptions.FETCH_SIZE));
        config.getOptional(JdbcSourceOptions.COMPATIBLE_MODE).ifPresent(builder::compatibleMode);

        boolean isOldVersion =
                config.getOptional(JdbcSourceOptions.QUERY).isPresent()
                        && config.getOptional(JdbcSourceOptions.PARTITION_COLUMN).isPresent();
        builder.useDynamicSplitter(!isOldVersion);
        builder.stringSplitMode(config.get(JdbcSourceOptions.STRING_SPLIT_MODE));
        builder.stringSplitModeCollate(config.get(JdbcSourceOptions.STRING_SPLIT_MODE_COLLATE));
        builder.splitSize(config.get(JdbcSourceOptions.SPLIT_SIZE));
        builder.splitEvenDistributionFactorUpperBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND));
        builder.splitEvenDistributionFactorLowerBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND));
        builder.splitSampleShardingThreshold(
                config.get(JdbcSourceOptions.SPLIT_SAMPLE_SHARDING_THRESHOLD));
        builder.splitInverseSamplingRate(config.get(JdbcSourceOptions.SPLIT_INVERSE_SAMPLING_RATE));

        builder.decimalTypeNarrowing(config.get(JdbcSourceOptions.DECIMAL_TYPE_NARROWING));
        builder.handleBlobAsString(config.get(JdbcSourceOptions.HANDLE_BLOB_AS_STRING));

        config.getOptional(JdbcSourceOptions.WHERE_CONDITION)
                .ifPresent(
                        whereConditionClause -> {
                            if (!whereConditionClause.toLowerCase().startsWith("where")) {
                                throw new IllegalArgumentException(
                                        "The where condition clause must start with 'where'. value: "
                                                + whereConditionClause);
                            }
                            builder.whereConditionClause(whereConditionClause);
                        });

        return builder.build();
    }
}
