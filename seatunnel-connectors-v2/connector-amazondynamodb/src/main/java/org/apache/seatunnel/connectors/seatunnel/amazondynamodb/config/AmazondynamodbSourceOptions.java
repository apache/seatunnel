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
public class AmazondynamodbSourceOptions implements Serializable {

    private String url;

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    private String table;

    private Config schema;

    public AmazondynamodbSourceOptions(Config config) {
        if (config.hasPath(AmazondynamodbConfig.URL)) {
            this.url = config.getString(AmazondynamodbConfig.URL);
        }
        if (config.hasPath(AmazondynamodbConfig.REGION)) {
            this.region = config.getString(AmazondynamodbConfig.REGION);
        }
        if (config.hasPath(AmazondynamodbConfig.ACCESS_KEY_ID)) {
            this.accessKeyId = config.getString(AmazondynamodbConfig.ACCESS_KEY_ID);
        }
        if (config.hasPath(AmazondynamodbConfig.SECRET_ACCESS_KEY)) {
            this.secretAccessKey = config.getString(AmazondynamodbConfig.SECRET_ACCESS_KEY);
        }
        if (config.hasPath(AmazondynamodbConfig.QUERY)) {
            this.table = config.getString(AmazondynamodbConfig.QUERY);
        }
        if (config.hasPath(CommonConfig.SCHEMA)) {
            this.schema = config.getConfig(CommonConfig.SCHEMA);
        }
    }
}
