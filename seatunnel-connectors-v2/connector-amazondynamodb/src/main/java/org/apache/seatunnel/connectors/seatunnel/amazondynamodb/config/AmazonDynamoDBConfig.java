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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AmazonDynamoDBConfig implements Serializable {

    private String url;

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    private String table;

    private Config schema;

    public int batchSize;
    public int scanItemLimit;
    public int parallelScanThreads;
    private int maxRetries;
    private long retryBaseDelayMs;
    private long retryMaxDelayMs;

    public AmazonDynamoDBConfig(ReadonlyConfig config) {
        this.url = config.get(AmazonDynamoDBBaseOptions.URL);
        this.region = config.get(AmazonDynamoDBBaseOptions.REGION);
        this.accessKeyId = config.get(AmazonDynamoDBBaseOptions.ACCESS_KEY_ID);
        this.secretAccessKey = config.get(AmazonDynamoDBBaseOptions.SECRET_ACCESS_KEY);
        this.table = config.get(AmazonDynamoDBBaseOptions.TABLE);
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            this.schema =
                    ReadonlyConfig.fromMap(config.get(ConnectorCommonOptions.SCHEMA)).toConfig();
        }
        this.batchSize = config.get(AmazonDynamoDBSinkOptions.BATCH_SIZE);
        this.scanItemLimit = config.get(AmazonDynamoDBSourceOptions.SCAN_ITEM_LIMIT);
        this.parallelScanThreads = config.get(AmazonDynamoDBSourceOptions.PARALLEL_SCAN_THREADS);
        this.maxRetries = config.get(AmazonDynamoDBSinkOptions.MAX_RETRIES);
        if (this.maxRetries < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "max_retries must be a non-negative integer, but got: %d",
                            this.maxRetries));
        }
        this.retryBaseDelayMs = config.get(AmazonDynamoDBSinkOptions.RETRY_BASE_DELAY_MS);
        this.retryMaxDelayMs = config.get(AmazonDynamoDBSinkOptions.RETRY_MAX_DELAY_MS);
    }
}
