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

package org.apache.seatunnel.connectors.doris.common;

import org.apache.seatunnel.common.PropertiesUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Data;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Data
@Builder
public class DorisOptions {
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long DEFAULT_BATCH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

    private static final int DEFAULT_HTTP_MAX_TOTAL = 200;
    private static final int DEFAULT_HTTP_PER_ROUTE = 50;
    private static final int DEFAULT_HTTP_CONNECT_TIMEOUT = 5 * 60 * 1000;
    private static final int DEFAULT_HTTP_REQUEST_TIMEOUT = 5 * 60 * 1000;
    private static final int DEFAULT_HTTP_WAIT_TIMEOUT = 5 * 60 * 1000;
    /**
     * For connection
     */
    private String feAddresses;
    private String username;
    private String password;
    private String tableName;
    private String databaseName;
    private Properties parameters;
    /**
     * For write
     */
    @Builder.Default
    private int batchSize = DEFAULT_BATCH_SIZE;
    @Builder.Default
    private int maxRetries = DEFAULT_MAX_RETRIES;
    @Builder.Default
    private long batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;

    /**
     * For http client
     */
    @Builder.Default
    private int httpMaxTotal = DEFAULT_HTTP_MAX_TOTAL;
    @Builder.Default
    private int httpPerRoute = DEFAULT_HTTP_PER_ROUTE;
    @Builder.Default
    private int httpConnectTimeout = DEFAULT_HTTP_CONNECT_TIMEOUT;
    @Builder.Default
    private int httpRequestTimeout = DEFAULT_HTTP_REQUEST_TIMEOUT;
    @Builder.Default
    private int httpWaitTimeout = DEFAULT_HTTP_WAIT_TIMEOUT;

    public static DorisOptions fromPluginConfig(Config pluginConfig) {
        DorisOptionsBuilder builder = DorisOptions.builder()
            .feAddresses(pluginConfig.getString(DorisOption.DORIS_FE_ADDRESSES))
            .username(pluginConfig.getString(DorisOption.DORIS_USERNAME))
            .password(pluginConfig.getString(DorisOption.DORIS_PASSWORD))
            .tableName(pluginConfig.getString(DorisOption.DORIS_TABLE_NAME))
            .databaseName(pluginConfig.getString(DorisOption.DORIS_DATABASE_NAME));

        Properties properties = new Properties();
        PropertiesUtil.setProperties(pluginConfig, properties, DorisOption.DORIS_LOAD_EXTRA_PARAMETER, false);
        builder.parameters(properties);

        if (pluginConfig.hasPath(DorisOption.DORIS_BATCH_SIZE)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_BATCH_SIZE));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_MAX_RETRY)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_MAX_RETRY));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_BATCH_INTERVAL_MS)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_BATCH_INTERVAL_MS));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_HTTP_MAX_TOTAL)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_HTTP_MAX_TOTAL));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_HTTP_PER_ROUTE)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_HTTP_PER_ROUTE));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_CONNECT_TIMEOUT)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_CONNECT_TIMEOUT));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_REQUEST_TIMEOUT)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_REQUEST_TIMEOUT));
        }

        if (pluginConfig.hasPath(DorisOption.DORIS_WAIT_TIMEOUT)) {
            builder.batchSize(pluginConfig.getInt(DorisOption.DORIS_WAIT_TIMEOUT));
        }

        return builder.build();
    }

    private interface DorisOption {
        String DORIS_FE_ADDRESSES = "fenodes";
        String DORIS_USERNAME = "user";
        String DORIS_PASSWORD = "password";
        String DORIS_TABLE_NAME = "table";
        String DORIS_DATABASE_NAME = "database";
        String DORIS_LOAD_EXTRA_PARAMETER = "doris.";

        String DORIS_BATCH_SIZE = "batch_size";
        String DORIS_MAX_RETRY = "max_retries";
        String DORIS_BATCH_INTERVAL_MS = "interval";

        String DORIS_HTTP_MAX_TOTAL = "http.max_total";
        String DORIS_HTTP_PER_ROUTE = "http.per_route";
        String DORIS_CONNECT_TIMEOUT = "http.connect_timeout";
        String DORIS_REQUEST_TIMEOUT = "http.request_time";
        String DORIS_WAIT_TIMEOUT = "http.wait_timeout";
    }
}
