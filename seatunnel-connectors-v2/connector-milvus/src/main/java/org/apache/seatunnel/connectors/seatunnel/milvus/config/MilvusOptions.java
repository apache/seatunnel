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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class MilvusOptions implements Serializable {

    private String milvusHost;
    private Integer milvusPort;
    private String collectionName;
    private String partitionField;
    private String userName;
    private String password;
    private String openaiEngine;
    private String openaiApiKey;
    private String embeddingsFields;

    public MilvusOptions(Config config) {
        this.milvusHost = config.getString(MilvusConfig.MILVUS_HOST.key());
        if (config.hasPath(MilvusConfig.MILVUS_PORT.key())) {
            this.milvusPort = config.getInt(MilvusConfig.MILVUS_PORT.key());
        } else {
            this.milvusPort = MilvusConfig.MILVUS_PORT.defaultValue();
        }
        this.collectionName = config.getString(MilvusConfig.COLLECTION_NAME.key());
        this.userName = config.getString(MilvusConfig.USERNAME.key());
        this.password = config.getString(MilvusConfig.PASSWORD.key());

        if (config.hasPath(MilvusConfig.PARTITION_FIELD.key())) {
            this.partitionField = config.getString(MilvusConfig.PARTITION_FIELD.key());
        }
        if (config.hasPath(MilvusConfig.OPENAI_ENGINE.key())) {
            this.openaiEngine = config.getString(MilvusConfig.OPENAI_ENGINE.key());
        } else {
            this.openaiEngine = MilvusConfig.OPENAI_ENGINE.defaultValue();
        }
        if (config.hasPath(MilvusConfig.OPENAI_API_KEY.key())) {
            this.openaiApiKey = config.getString(MilvusConfig.OPENAI_API_KEY.key());
        }
        if (config.hasPath(MilvusConfig.EMBEDDINGS_FIELDS.key())) {
            this.embeddingsFields = config.getString(MilvusConfig.EMBEDDINGS_FIELDS.key());
        }
    }
}
