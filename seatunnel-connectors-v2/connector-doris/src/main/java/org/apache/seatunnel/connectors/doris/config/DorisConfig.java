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
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_DESERIALIZE_ARROW_ASYNC;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_DESERIALIZE_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_EXEC_MEM_LIMIT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_FILTER_QUERY;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_READ_FIELD;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_REQUEST_QUERY_TIMEOUT_S;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_REQUEST_READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_REQUEST_RETRIES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_SINK_CONFIG_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.DORIS_TABLET_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.FENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.PASSWORD;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SAVE_MODE_CREATE_TEMPLATE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_BUFFER_COUNT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_BUFFER_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_CHECK_INTERVAL;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_ENABLE_2PC;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_ENABLE_DELETE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_LABEL_PREFIX;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.SINK_MAX_RETRIES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.TABLE;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.USERNAME;

@Setter
@Getter
@ToString
public class DorisConfig implements Serializable {

    public static final String IDENTIFIER = "Doris";

    // common option
    private String frontends;
    private String database;
    private String table;
    private String username;
    private String password;
    private Integer queryPort;
    private int batchSize;

    // source option
    private String readField;
    private String filterQuery;
    private Integer tabletSize;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private Boolean deserializeArrowAsync;
    private int deserializeQueueSize;
    private Long execMemLimit;
    private boolean useOldApi;

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

    public static DorisConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static DorisConfig of(ReadonlyConfig config) {

        DorisConfig dorisConfig = new DorisConfig();

        // common option
        dorisConfig.setFrontends(config.get(FENODES));
        dorisConfig.setUsername(config.get(USERNAME));
        dorisConfig.setPassword(config.get(PASSWORD));
        dorisConfig.setQueryPort(config.get(QUERY_PORT));
        dorisConfig.setStreamLoadProps(parseStreamLoadProperties(config));
        dorisConfig.setDatabase(config.get(DATABASE));
        dorisConfig.setTable(config.get(TABLE));

        // source option
        dorisConfig.setReadField(config.get(DORIS_READ_FIELD));
        dorisConfig.setFilterQuery(config.get(DORIS_FILTER_QUERY));
        dorisConfig.setTabletSize(config.get(DORIS_TABLET_SIZE));
        dorisConfig.setRequestConnectTimeoutMs(config.get(DORIS_REQUEST_CONNECT_TIMEOUT_MS));
        dorisConfig.setRequestQueryTimeoutS(config.get(DORIS_REQUEST_QUERY_TIMEOUT_S));
        dorisConfig.setRequestReadTimeoutMs(config.get(DORIS_REQUEST_READ_TIMEOUT_MS));
        dorisConfig.setRequestRetries(config.get(DORIS_REQUEST_RETRIES));
        dorisConfig.setDeserializeArrowAsync(config.get(DORIS_DESERIALIZE_ARROW_ASYNC));
        dorisConfig.setDeserializeQueueSize(config.get(DORIS_DESERIALIZE_QUEUE_SIZE));
        dorisConfig.setBatchSize(config.get(DORIS_BATCH_SIZE));
        dorisConfig.setExecMemLimit(config.get(DORIS_EXEC_MEM_LIMIT));

        // sink option
        dorisConfig.setEnable2PC(config.get(SINK_ENABLE_2PC));
        dorisConfig.setLabelPrefix(config.get(SINK_LABEL_PREFIX));
        dorisConfig.setCheckInterval(config.get(SINK_CHECK_INTERVAL));
        dorisConfig.setMaxRetries(config.get(SINK_MAX_RETRIES));
        dorisConfig.setBufferSize(config.get(SINK_BUFFER_SIZE));
        dorisConfig.setBufferCount(config.get(SINK_BUFFER_COUNT));
        dorisConfig.setEnableDelete(config.get(SINK_ENABLE_DELETE));
        dorisConfig.setNeedsUnsupportedTypeCasting(config.get(NEEDS_UNSUPPORTED_TYPE_CASTING));

        // create table option
        dorisConfig.setCreateTableTemplate(config.get(SAVE_MODE_CREATE_TEMPLATE));

        return dorisConfig;
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
