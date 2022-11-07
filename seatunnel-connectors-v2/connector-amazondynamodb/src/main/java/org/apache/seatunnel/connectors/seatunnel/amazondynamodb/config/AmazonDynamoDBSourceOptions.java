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

import org.apache.seatunnel.connectors.seatunnel.common.config.CommonConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AmazonDynamoDBSourceOptions implements Serializable {

    private static final int DEFAULT_BATCH_SIZE = 25;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 1000;

    private String url;

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    private String table;

    private Config schema;

    public int batchSize = DEFAULT_BATCH_SIZE;
    public int batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;

    public AmazonDynamoDBSourceOptions(Config config) {
        this.url = config.getString(AmazonDynamoDBConfig.URL);
        this.region = config.getString(AmazonDynamoDBConfig.REGION);
        this.accessKeyId = config.getString(AmazonDynamoDBConfig.ACCESS_KEY_ID);
        this.secretAccessKey = config.getString(AmazonDynamoDBConfig.SECRET_ACCESS_KEY);
        this.table = config.getString(AmazonDynamoDBConfig.TABLE);

        if (config.hasPath(CommonConfig.SCHEMA)) {
            this.schema = config.getConfig(CommonConfig.SCHEMA);
        }
        if (config.hasPath(AmazonDynamoDBConfig.BATCH_SIZE)) {
            this.batchSize = config.getInt(AmazonDynamoDBConfig.BATCH_SIZE);
        }
        if (config.hasPath(AmazonDynamoDBConfig.DEFAULT_BATCH_INTERVAL_MS)) {
            this.batchIntervalMs = config.getInt(AmazonDynamoDBConfig.DEFAULT_BATCH_INTERVAL_MS);
        }
    }
}
