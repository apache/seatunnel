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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class SourceConfig extends StarRocksConfig {

    public SourceConfig(ReadonlyConfig config) {
        super(config);
        this.maxRetries = config.get(StarRocksSourceOptions.MAX_RETRIES);
        this.requestTabletSize = config.get(StarRocksSourceOptions.QUERY_TABLET_SIZE);
        this.scanFilter = config.get(StarRocksSourceOptions.SCAN_FILTER);
        this.connectTimeoutMs = config.get(StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT);
        this.batchRows = config.get(StarRocksSourceOptions.SCAN_BATCH_ROWS);
        this.keepAliveMin = config.get(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        this.queryTimeoutSec = config.get(StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC);
        this.memLimit = config.get(StarRocksSourceOptions.SCAN_MEM_LIMIT);

        String prefix = StarRocksSourceOptions.STARROCKS_SCAN_CONFIG_PREFIX.key();
        config.toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                this.sourceOptionProps.put(
                                        key.substring(prefix.length()).toLowerCase(), value);
                            }
                        });
    }

    private int maxRetries = StarRocksSourceOptions.MAX_RETRIES.defaultValue();
    private int requestTabletSize = StarRocksSourceOptions.QUERY_TABLET_SIZE.defaultValue();
    private String scanFilter = StarRocksSourceOptions.SCAN_FILTER.defaultValue();
    private long memLimit = StarRocksSourceOptions.SCAN_MEM_LIMIT.defaultValue();
    private int queryTimeoutSec = StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC.defaultValue();
    private int keepAliveMin = StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN.defaultValue();
    private int batchRows = StarRocksSourceOptions.SCAN_BATCH_ROWS.defaultValue();
    private int connectTimeoutMs = StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT.defaultValue();
    private Map<String, String> sourceOptionProps = new HashMap<>();
}
