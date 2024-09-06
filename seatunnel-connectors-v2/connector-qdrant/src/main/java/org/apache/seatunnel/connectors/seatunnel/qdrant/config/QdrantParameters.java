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

package org.apache.seatunnel.connectors.seatunnel.qdrant.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import lombok.Data;

import java.io.Serializable;

@Data
public class QdrantParameters implements Serializable {
    private String host;
    private int port;
    private String apiKey;
    private String collectionName;
    private boolean useTls;

    public QdrantParameters(ReadonlyConfig config) {
        this.host = config.get(QdrantConfig.HOST);
        this.port = config.get(QdrantConfig.PORT);
        this.apiKey = config.get(QdrantConfig.API_KEY);
        this.collectionName = config.get(QdrantConfig.COLLECTION_NAME);
        this.useTls = config.get(QdrantConfig.USE_TLS);
    }

    public QdrantClient buildQdrantClient() {
        return new QdrantClient(QdrantGrpcClient.newBuilder(host, port, useTls).build());
    }
}
