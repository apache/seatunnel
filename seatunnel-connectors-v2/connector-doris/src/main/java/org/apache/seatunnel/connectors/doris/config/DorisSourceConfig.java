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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.List;

import static org.apache.seatunnel.connectors.doris.config.DorisOptions.FENODES;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.PASSWORD;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.QUERY_PORT;
import static org.apache.seatunnel.connectors.doris.config.DorisOptions.USERNAME;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_DESERIALIZE_ARROW_ASYNC;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_DESERIALIZE_QUEUE_SIZE;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_REQUEST_QUERY_TIMEOUT_S;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_REQUEST_READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.doris.config.DorisSourceOptions.DORIS_REQUEST_RETRIES;

@Data
@SuperBuilder
public class DorisSourceConfig implements Serializable {

    private String frontends;
    private Integer queryPort;
    private String username;
    private String password;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private Boolean deserializeArrowAsync;
    private int deserializeQueueSize;
    private boolean useOldApi;
    private List<DorisTableConfig> tableConfigList;

    public static DorisSourceConfig of(ReadonlyConfig config) {
        DorisSourceConfigBuilder<?, ?> builder = DorisSourceConfig.builder();
        builder.tableConfigList(DorisTableConfig.of(config));
        builder.frontends(config.get(FENODES));
        builder.queryPort(config.get(QUERY_PORT));
        builder.username(config.get(USERNAME));
        builder.password(config.get(PASSWORD));
        builder.requestConnectTimeoutMs(config.get(DORIS_REQUEST_CONNECT_TIMEOUT_MS));
        builder.requestReadTimeoutMs(config.get(DORIS_REQUEST_READ_TIMEOUT_MS));
        builder.requestQueryTimeoutS(config.get(DORIS_REQUEST_QUERY_TIMEOUT_S));
        builder.requestRetries(config.get(DORIS_REQUEST_RETRIES));
        builder.deserializeArrowAsync(config.get(DORIS_DESERIALIZE_ARROW_ASYNC));
        builder.deserializeQueueSize(config.get(DORIS_DESERIALIZE_QUEUE_SIZE));
        return builder.build();
    }
}
