/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.druid.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@Data
@AllArgsConstructor
public class DruidSourceOptions implements Serializable {
    private String URL;
    private String datasource ;
    private String startTimestamp ;
    private String endTimestamp ;
    private List<String> columns ;

    private String partitionColumn;
    private Long partitionUpperBound;
    private Long partitionLowerBound;
    private Integer parallelism;

    public DruidSourceOptions(Config pluginConfig) {
        this.URL = pluginConfig.getString(DruidSourceConfig.URL);
        this.datasource = pluginConfig.getString(DruidSourceConfig.DATASOURCE);
        this.columns = pluginConfig.hasPath(DruidSourceConfig.COLUMNS) ? pluginConfig.getStringList(DruidSourceConfig.COLUMNS) : null;
        this.startTimestamp = pluginConfig.hasPath(DruidSourceConfig.START_TIMESTAMP) ? pluginConfig.getString(DruidSourceConfig.START_TIMESTAMP) : null;
        this.endTimestamp = pluginConfig.hasPath(DruidSourceConfig.END_TIMESTAMP) ? pluginConfig.getString(DruidSourceConfig.END_TIMESTAMP) : null;
    }
}
