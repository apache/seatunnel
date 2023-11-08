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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.kudu.client.AsyncKuduClient;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class KuduSourceConfig extends CommonConfig {

    public static final Option<Long> QUERY_TIMEOUT =
            Options.key("scan_token_query_timeout")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription(
                            "The timeout for connecting scan token. If not set, it will be the same as operationTimeout");

    public static final Option<Integer> SCAN_BATCH_SIZE_BYTES =
            Options.key("scan_token_batch_size_bytes")
                    .intType()
                    .defaultValue(1024 * 1024)
                    .withDescription(
                            "Kudu scan bytes. The maximum number of bytes read at a time, the default is 1MB");

    public static final Option<String> FILTER =
            Options.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu scan filter expressions");

    private int batchSizeBytes;

    protected Long queryTimeout;

    private String filter;

    public KuduSourceConfig(ReadonlyConfig config) {
        super(config);
        this.batchSizeBytes = config.get(SCAN_BATCH_SIZE_BYTES);
        this.queryTimeout = config.get(QUERY_TIMEOUT);
        this.filter = config.get(FILTER);
    }
}
