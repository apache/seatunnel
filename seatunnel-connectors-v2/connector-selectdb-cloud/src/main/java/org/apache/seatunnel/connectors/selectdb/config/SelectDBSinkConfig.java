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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.DATABASE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.SELECTDB_BATCH_SIZE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.FENODES;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.TABLE;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions.*;

@Setter
@Getter
@ToString
public class SelectDBSinkConfig implements Serializable {

    // common option
    private String frontends;
    private String database;
    private String table;
    private String username;
    private String password;
    private Integer queryPort;
    private int batchSize;
    @Deprecated
    private String tableIdentifier;

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

    public static SelectDBSinkConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static SelectDBSinkConfig of(ReadonlyConfig config) {

        SelectDBSinkConfig selectdbSinkConfig = new SelectDBSinkConfig();

        // common option
        selectdbSinkConfig.setFrontends(config.get(FENODES));
        selectdbSinkConfig.setUsername(config.get(USERNAME));
        selectdbSinkConfig.setPassword(config.get(PASSWORD));
        selectdbSinkConfig.setQueryPort(config.get(QUERY_PORT));
        selectdbSinkConfig.setStreamLoadProps(parseStreamLoadProperties(config));
        selectdbSinkConfig.setDatabase(config.get(DATABASE));
        selectdbSinkConfig.setTable(config.get(TABLE));
        selectdbSinkConfig.setBatchSize(config.get(SELECTDB_BATCH_SIZE));
        selectdbSinkConfig.setTableIdentifier(config.get(TABLE_IDENTIFIER));

        // sink option
        selectdbSinkConfig.setEnable2PC(config.get(SINK_ENABLE_2PC));
        selectdbSinkConfig.setLabelPrefix(config.get(SINK_LABEL_PREFIX));
        selectdbSinkConfig.setCheckInterval(config.get(SINK_CHECK_INTERVAL));
        selectdbSinkConfig.setMaxRetries(config.get(SINK_MAX_RETRIES));
        selectdbSinkConfig.setBufferSize(config.get(SINK_BUFFER_SIZE));
        selectdbSinkConfig.setBufferCount(config.get(SINK_BUFFER_COUNT));
        selectdbSinkConfig.setEnableDelete(config.get(SINK_ENABLE_DELETE));
        selectdbSinkConfig.setNeedsUnsupportedTypeCasting(config.get(NEEDS_UNSUPPORTED_TYPE_CASTING));

        // create table option
        selectdbSinkConfig.setCreateTableTemplate(config.get(SAVE_MODE_CREATE_TEMPLATE));

        return selectdbSinkConfig;
    }

    private static Properties parseStreamLoadProperties(ReadonlyConfig config) {
        Properties streamLoadProps = new Properties();
        if (config.getOptional(SELECTDB_SINK_CONFIG_PREFIX).isPresent()) {
            Map<String, String> map = config.getOptional(SELECTDB_SINK_CONFIG_PREFIX).get();
            map.forEach(
                    (key, value) -> {
                        streamLoadProps.put(key.toLowerCase(), value);
                    });
        }
        return streamLoadProps;
    }

}
