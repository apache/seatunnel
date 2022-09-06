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
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class DruidSinkOptions implements Serializable {
    private  String coordinatorURL;
    private  String datasource;
    private  String timestampColumn;
    private  String timestampFormat;
    private  String  timestampMissingValue;
    private List<String> columns;
    private  int parallelism;

    private static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";
    private static final String DEFAULT_TIMESTAMP_FORMAT = "auto";
    private static final DateTime DEFAULT_TIMESTAMP_MISSING_VALUE = null;
    private static final int DEFAULT_PARALLELISM = 1;

    public DruidSinkOptions(Config pluginConfig) {
        this.coordinatorURL = pluginConfig.getString(DruidSinkConfig.COORDINATOR_URL);
        this.datasource = pluginConfig.getString(DruidSinkConfig.DATASOURCE);
        this.columns = pluginConfig.getStringList(DruidSinkConfig.COLUMNS);
        this.timestampColumn = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_COLUMN) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_COLUMN) : null;
        this.timestampFormat = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_FORMAT) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_FORMAT) : null;
        this.timestampMissingValue = pluginConfig.hasPath(DruidSinkConfig.TIMESTAMP_MISSING_VALUE) ? pluginConfig.getString(DruidSinkConfig.TIMESTAMP_MISSING_VALUE) : null;
    }
}
