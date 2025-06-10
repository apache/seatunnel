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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import com.clickhouse.client.ClickHouseNode;
import lombok.Builder;
import lombok.Data;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.List;

@Data
@Builder(builderClassName = "Builder")
public class ClickhouseSourceConfig implements Serializable {

    private String serverTimeZone;
    private List<ClickHouseNode> nodes;
    private String sql;
    private String partitionColumn;
    private String partitionUpperBound;
    private String partitionLowerBound;
    private Integer partitionNum;

    public static ClickhouseSourceConfig of(ReadonlyConfig config) {
        Builder builder = ClickhouseSourceConfig.builder();

        builder.serverTimeZone(config.get(ClickhouseBaseOptions.SERVER_TIME_ZONE));
        builder.sql(config.get(ClickhouseSourceOptions.SQL));
        builder.partitionColumn(config.get(ClickhouseSourceOptions.PARTITION_COLUMN));
        builder.partitionUpperBound(config.get(ClickhouseSourceOptions.PARTITION_UPPER_BOUND));
        builder.partitionLowerBound(config.get(ClickhouseSourceOptions.PARTITION_LOWER_BOUND));
        builder.partitionNum(config.get(ClickhouseSourceOptions.PARTITION_NUM));

        return builder.build();
    }
}
