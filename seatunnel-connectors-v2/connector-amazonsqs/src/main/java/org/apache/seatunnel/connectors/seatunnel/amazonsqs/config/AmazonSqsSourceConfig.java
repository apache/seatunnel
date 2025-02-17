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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class AmazonSqsSourceConfig implements Serializable {

    private String url;

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    private String messageGroupId;

    private boolean deleteMessage;

    private Config schema;

    public AmazonSqsSourceConfig(ReadonlyConfig config) {
        this.url = config.get(AmazonSqsSourceOptions.URL);
        this.region = config.get(AmazonSqsSourceOptions.REGION);
        this.accessKeyId = config.get(AmazonSqsSourceOptions.ACCESS_KEY_ID);
        this.secretAccessKey = config.get(AmazonSqsSourceOptions.SECRET_ACCESS_KEY);
        this.messageGroupId = config.get(AmazonSqsSourceOptions.MESSAGE_GROUP_ID);
        this.deleteMessage = config.get(AmazonSqsSourceOptions.DELETE_MESSAGE);
        this.schema = ReadonlyConfig.fromMap(config.get(AmazonSqsSourceOptions.SCHEMA)).toConfig();
        ;
    }
}
