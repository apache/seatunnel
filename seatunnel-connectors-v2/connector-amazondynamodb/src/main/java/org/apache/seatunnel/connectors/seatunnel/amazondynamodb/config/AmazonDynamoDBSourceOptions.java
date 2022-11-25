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

import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AmazonDynamoDBSourceOptions implements Serializable {

    private String url;

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    private String table;

    private Config schema;

    public int batchSize = AmazonDynamoDBConfig.BATCH_SIZE.defaultValue();
    public int batchIntervalMs = AmazonDynamoDBConfig.BATCH_INTERVAL_MS.defaultValue();

    public AmazonDynamoDBSourceOptions(Config config) {
        this.url = config.getString(AmazonDynamoDBConfig.URL.key());
        this.region = config.getString(AmazonDynamoDBConfig.REGION.key());
        this.accessKeyId = config.getString(AmazonDynamoDBConfig.ACCESS_KEY_ID.key());
        this.secretAccessKey = config.getString(AmazonDynamoDBConfig.SECRET_ACCESS_KEY.key());
        this.table = config.getString(AmazonDynamoDBConfig.TABLE.key());
        if (config.hasPath(SeaTunnelSchema.SCHEMA.key())) {
            this.schema = config.getConfig(SeaTunnelSchema.SCHEMA.key());
        }
        if (config.hasPath(AmazonDynamoDBConfig.BATCH_SIZE.key())) {
            this.batchSize = config.getInt(AmazonDynamoDBConfig.BATCH_SIZE.key());
        }
        if (config.hasPath(AmazonDynamoDBConfig.BATCH_INTERVAL_MS.key())) {
            this.batchIntervalMs = config.getInt(AmazonDynamoDBConfig.BATCH_INTERVAL_MS.key());
        }
    }
}
