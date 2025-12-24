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

package org.apache.seatunnel.connectors.selectdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Properties;

@Setter
@Getter
@ToString
public class SelectDBConfig {

    private String loadUrl;
    private String jdbcUrl;
    private String clusterName;
    private String username;
    private String password;
    private String tableIdentifier;
    private Boolean enableDelete;
    private String labelPrefix;
    private boolean enable2PC;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Integer flushQueueSize;
    private Properties StageLoadProps;

    public static SelectDBConfig loadConfig(ReadonlyConfig pluginConfig) {
        SelectDBConfig selectdbConfig = new SelectDBConfig();
        selectdbConfig.setLoadUrl(pluginConfig.get(SelectDBSinkOptions.LOAD_URL));
        selectdbConfig.setJdbcUrl(pluginConfig.get(SelectDBSinkOptions.JDBC_URL));
        selectdbConfig.setClusterName(pluginConfig.get(SelectDBSinkOptions.CLUSTER_NAME));
        selectdbConfig.setUsername(pluginConfig.get(SelectDBSinkOptions.USERNAME));
        selectdbConfig.setPassword(pluginConfig.get(SelectDBSinkOptions.PASSWORD));
        selectdbConfig.setTableIdentifier(pluginConfig.get(SelectDBSinkOptions.TABLE_IDENTIFIER));
        if (pluginConfig.getOptional(SelectDBSinkOptions.SELECTDB_SINK_CONFIG_PREFIX).isPresent()) {
            Properties properties = new Properties();
            properties.putAll(pluginConfig.get(SelectDBSinkOptions.SELECTDB_SINK_CONFIG_PREFIX));
            selectdbConfig.setStageLoadProps(properties);
        }
        selectdbConfig.setLabelPrefix(pluginConfig.get(SelectDBSinkOptions.SINK_LABEL_PREFIX));
        selectdbConfig.setMaxRetries(pluginConfig.get(SelectDBSinkOptions.SINK_MAX_RETRIES));
        selectdbConfig.setEnable2PC(pluginConfig.get(SelectDBSinkOptions.SINK_ENABLE_2PC));
        selectdbConfig.setBufferSize(pluginConfig.get(SelectDBSinkOptions.SINK_BUFFER_SIZE));
        selectdbConfig.setBufferCount(pluginConfig.get(SelectDBSinkOptions.SINK_BUFFER_COUNT));
        selectdbConfig.setEnableDelete(pluginConfig.get(SelectDBSinkOptions.SINK_ENABLE_DELETE));
        selectdbConfig.setFlushQueueSize(
                pluginConfig.get(SelectDBSinkOptions.SINK_FLUSH_QUEUE_SIZE));
        return selectdbConfig;
    }
}
