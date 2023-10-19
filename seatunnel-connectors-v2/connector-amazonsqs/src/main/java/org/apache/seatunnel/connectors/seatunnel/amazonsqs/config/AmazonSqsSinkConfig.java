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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class AmazonSqsSinkConfig implements Serializable {

    public String url;
    public String region;
    public String accessKeyId;
    public String secretAccessKey;

    public static AmazonSqsSinkConfig of(ReadonlyConfig config) {
        AmazonSqsSinkConfigBuilder builder = AmazonSqsSinkConfig.builder();
        builder.url(config.get(AmazonSqsOptions.URL));
        builder.region(config.get(AmazonSqsOptions.REGION));
        config.getOptional(AmazonSqsOptions.ACCESS_KEY_ID).ifPresent(builder::accessKeyId);
        config.getOptional(AmazonSqsOptions.SECRET_ACCESS_KEY).ifPresent(builder::secretAccessKey);
        return builder.build();
    }
}
