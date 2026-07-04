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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class HugeGraphSourceConfig implements Serializable {

    // connection config
    private String host;
    private int port;
    private String graphName;
    private String graphSpace;
    private String username;
    private String password;
    private int maxRetries;
    private int retryBackoffMs;

    // source-specific config
    private String label;
    private HugeGraphSourceOptions.LabelType type;
    private List<String> properties;
    private int pageSize;
    private Integer limit;

    public static HugeGraphSourceConfig of(ReadonlyConfig config) {
        HugeGraphSourceConfig sourceConfig = new HugeGraphSourceConfig();

        sourceConfig.setHost(config.get(HugeGraphOptions.HOST));
        sourceConfig.setPort(config.get(HugeGraphOptions.PORT));
        sourceConfig.setGraphName(config.get(HugeGraphOptions.GRAPH_NAME));

        config.getOptional(HugeGraphOptions.GRAPH_SPACE).ifPresent(sourceConfig::setGraphSpace);
        config.getOptional(HugeGraphOptions.USERNAME).ifPresent(sourceConfig::setUsername);
        config.getOptional(HugeGraphOptions.PASSWORD).ifPresent(sourceConfig::setPassword);

        sourceConfig.setMaxRetries(
                config.getOptional(HugeGraphOptions.MAX_RETRIES)
                        .orElse(HugeGraphOptions.MAX_RETRIES.defaultValue()));
        sourceConfig.setRetryBackoffMs(
                config.getOptional(HugeGraphOptions.RETRY_BACKOFF_MS)
                        .orElse(HugeGraphOptions.RETRY_BACKOFF_MS.defaultValue()));

        sourceConfig.setLabel(config.get(HugeGraphSourceOptions.LABEL));
        sourceConfig.setType(config.get(HugeGraphSourceOptions.TYPE));
        sourceConfig.setPageSize(
                config.getOptional(HugeGraphSourceOptions.PAGE_SIZE)
                        .orElse(HugeGraphSourceOptions.PAGE_SIZE.defaultValue()));

        config.getOptional(HugeGraphSourceOptions.PROPERTIES)
                .ifPresent(sourceConfig::setProperties);
        config.getOptional(HugeGraphSourceOptions.LIMIT).ifPresent(sourceConfig::setLimit);

        return sourceConfig;
    }
}
