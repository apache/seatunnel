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

package org.apache.seatunnel.connectors.seatunnel.starrocks.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class SourceConfig extends CommonConfig {

    private static final long DEFAULT_SCAN_MEM_LIMIT = 1024 * 1024 * 1024L;

    public SourceConfig(
            @NonNull List<String> nodeUrls,
            @NonNull String username,
            @NonNull String password,
            @NonNull String database,
            @NonNull String table) {
        super(nodeUrls, username, password, database, table);
    }

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("number of retry requests sent to StarRocks");

    public static final Option<Integer> QUERY_TABLET_SIZE =
            Options.key("request_tablet_size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The number of Tablets corresponding to an Partition");

    public static final Option<String> SCAN_FILTER =
            Options.key("scan_filter").stringType().defaultValue("").withDescription("SQL filter");

    public static final Option<Integer> SCAN_CONNECT_TIMEOUT =
            Options.key("scan_connect_timeout_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("scan connect timeout");

    public static final Option<Integer> SCAN_BATCH_ROWS =
            Options.key("scan_batch_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("scan batch rows");

    public static final Option<Integer> SCAN_KEEP_ALIVE_MIN =
            Options.key("scan_keep_alive_min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Max keep alive time min");

    public static final Option<Integer> SCAN_QUERY_TIMEOUT_SEC =
            Options.key("scan_query_timeout_sec")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("Query timeout for a single query");

    public static final Option<Long> SCAN_MEM_LIMIT =
            Options.key("scan_mem_limit")
                    .longType()
                    .defaultValue(DEFAULT_SCAN_MEM_LIMIT)
                    .withDescription("Memory byte limit for a single query");

    public static final Option<String> STARROCKS_SCAN_CONFIG_PREFIX =
            Options.key("scan.params.")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The parameter of the scan data from be");

    private int maxRetries = MAX_RETRIES.defaultValue();
    private int requestTabletSize = QUERY_TABLET_SIZE.defaultValue();
    private String scanFilter = SCAN_FILTER.defaultValue();
    private long memLimit = SCAN_MEM_LIMIT.defaultValue();
    private int queryTimeoutSec = SCAN_QUERY_TIMEOUT_SEC.defaultValue();
    private int keepAliveMin = SCAN_KEEP_ALIVE_MIN.defaultValue();
    private int batchRows = SCAN_BATCH_ROWS.defaultValue();
    private int connectTimeoutMs = SCAN_CONNECT_TIMEOUT.defaultValue();
    private final Map<String, String> sourceOptionProps = new HashMap<>();

    public static SourceConfig loadConfig(Config pluginConfig) {
        SourceConfig sourceConfig =
                new SourceConfig(
                        pluginConfig.getStringList(NODE_URLS.key()),
                        pluginConfig.getString(USERNAME.key()),
                        pluginConfig.getString(PASSWORD.key()),
                        pluginConfig.getString(DATABASE.key()),
                        pluginConfig.getString(TABLE.key()));

        if (pluginConfig.hasPath(MAX_RETRIES.key())) {
            sourceConfig.setMaxRetries(pluginConfig.getInt(MAX_RETRIES.key()));
        }
        if (pluginConfig.hasPath(QUERY_TABLET_SIZE.key())) {
            sourceConfig.setRequestTabletSize(pluginConfig.getInt(QUERY_TABLET_SIZE.key()));
        }
        if (pluginConfig.hasPath(SCAN_FILTER.key())) {
            sourceConfig.setScanFilter(pluginConfig.getString(SCAN_FILTER.key()));
        }
        if (pluginConfig.hasPath(SCAN_CONNECT_TIMEOUT.key())) {
            sourceConfig.setConnectTimeoutMs(pluginConfig.getInt(SCAN_CONNECT_TIMEOUT.key()));
        }
        if (pluginConfig.hasPath(SCAN_BATCH_ROWS.key())) {
            sourceConfig.setBatchRows(pluginConfig.getInt(SCAN_BATCH_ROWS.key()));
        }
        if (pluginConfig.hasPath(SCAN_KEEP_ALIVE_MIN.key())) {
            sourceConfig.setKeepAliveMin(pluginConfig.getInt(SCAN_KEEP_ALIVE_MIN.key()));
        }
        if (pluginConfig.hasPath(SCAN_QUERY_TIMEOUT_SEC.key())) {
            sourceConfig.setQueryTimeoutSec(pluginConfig.getInt(SCAN_QUERY_TIMEOUT_SEC.key()));
        }
        if (pluginConfig.hasPath(SCAN_MEM_LIMIT.key())) {
            sourceConfig.setMemLimit(pluginConfig.getLong(SCAN_MEM_LIMIT.key()));
        }
        parseSourceOptionProperties(pluginConfig, sourceConfig);
        return sourceConfig;
    }

    private static void parseSourceOptionProperties(
            Config pluginConfig, SourceConfig sourceConfig) {
        Config sourceOptionConfig =
                TypesafeConfigUtils.extractSubConfig(
                        pluginConfig, STARROCKS_SCAN_CONFIG_PREFIX.key(), false);
        sourceOptionConfig
                .entrySet()
                .forEach(
                        entry -> {
                            final String configKey = entry.getKey().toLowerCase();
                            sourceConfig.sourceOptionProps.put(
                                    configKey, (String) entry.getValue().unwrapped());
                        });
    }
}
