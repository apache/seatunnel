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

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DATABASE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_BATCH_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.FENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.PASSWORD;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.USERNAME;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.DORIS_SINK_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_BUFFER_COUNT;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_CHECK_INTERVAL;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_ENABLE_2PC;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_LABEL_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisSinkOptions.SINK_MAX_RETRIES;

@Setter
@Getter
@ToString
public class DorisSinkConfig implements Serializable {

    // common option
    private String frontends;
    private String database;
    private String table;
    private String username;
    private String password;
    private Integer queryPort;
    private int batchSize;

    // sink option
    private Boolean enable2PC;
    private Boolean enableDelete;
    private String labelPrefix;
    private Integer checkInterval;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Properties streamLoadProps;
    private boolean needsUnsupportedTypeCasting;

    // create table option
    private String createTableTemplate;

    public static DorisSinkConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static DorisSinkConfig of(ReadonlyConfig config) {

        DorisSinkConfig dorisSinkConfig = new DorisSinkConfig();

        // common option
        dorisSinkConfig.setFrontends(config.get(FENODES));
        dorisSinkConfig.setUsername(config.get(USERNAME));
        dorisSinkConfig.setPassword(config.get(PASSWORD));
        dorisSinkConfig.setQueryPort(config.get(QUERY_PORT));
        dorisSinkConfig.setStreamLoadProps(parseStreamLoadProperties(config));
        dorisSinkConfig.setDatabase(config.get(DATABASE));
        dorisSinkConfig.setTable(config.get(TABLE));
        dorisSinkConfig.setBatchSize(config.get(DORIS_BATCH_SIZE));

        // sink option
        dorisSinkConfig.setEnable2PC(config.get(SINK_ENABLE_2PC));
        dorisSinkConfig.setLabelPrefix(config.get(SINK_LABEL_PREFIX));
        dorisSinkConfig.setCheckInterval(config.get(SINK_CHECK_INTERVAL));
        dorisSinkConfig.setMaxRetries(config.get(SINK_MAX_RETRIES));
        dorisSinkConfig.setBufferSize(config.get(SINK_BUFFER_SIZE));
        dorisSinkConfig.setBufferCount(config.get(SINK_BUFFER_COUNT));
        dorisSinkConfig.setEnableDelete(config.get(SINK_ENABLE_DELETE));
        dorisSinkConfig.setNeedsUnsupportedTypeCasting(config.get(NEEDS_UNSUPPORTED_TYPE_CASTING));

        // create table option
        dorisSinkConfig.setCreateTableTemplate(config.get(SAVE_MODE_CREATE_TEMPLATE));

        return dorisSinkConfig;
    }

    private static Properties parseStreamLoadProperties(ReadonlyConfig config) {
        Properties streamLoadProps = new Properties();
        if (config.getOptional(DORIS_SINK_CONFIG_PREFIX).isPresent()) {
            Map<String, String> map = config.getOptional(DORIS_SINK_CONFIG_PREFIX).get();
            map.forEach(
                    (key, value) -> {
                        streamLoadProps.put(key.toLowerCase(), value);
                    });
        }
        return streamLoadProps;
    }
}
