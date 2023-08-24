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
package org.apache.seatunnel.connectors.seatunnel.milvus.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Setter
@Getter
@ToString
public class MilvusSinkConfig implements Serializable {

    private String milvusHost;
    private Integer milvusPort;
    private String collectionName;
    private String partitionField;
    private String userName;
    private String password;
    private String openaiEngine;
    private String openaiApiKey;
    private String embeddingsFields;

    public static MilvusSinkConfig of(ReadonlyConfig config) {
        MilvusSinkConfig sinkConfig = new MilvusSinkConfig();
        sinkConfig.setMilvusHost(config.get(MilvusOptions.MILVUS_HOST));
        sinkConfig.setMilvusPort(config.get(MilvusOptions.MILVUS_PORT));
        sinkConfig.setCollectionName(config.get(MilvusOptions.COLLECTION_NAME));
        config.getOptional(MilvusOptions.PARTITION_FIELD).ifPresent(sinkConfig::setPartitionField);
        config.getOptional(MilvusOptions.USERNAME).ifPresent(sinkConfig::setUserName);
        config.getOptional(MilvusOptions.PASSWORD).ifPresent(sinkConfig::setPassword);
        config.getOptional(MilvusOptions.OPENAI_ENGINE).ifPresent(sinkConfig::setOpenaiEngine);
        config.getOptional(MilvusOptions.OPENAI_API_KEY).ifPresent(sinkConfig::setOpenaiApiKey);
        config.getOptional(MilvusOptions.EMBEDDINGS_FIELDS)
                .ifPresent(sinkConfig::setEmbeddingsFields);

        return sinkConfig;
    }
}
