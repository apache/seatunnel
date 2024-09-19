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

package org.apache.seatunnel.connectors.seatunnel.weaviate.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

@Builder
@Getter
public class WeaviateParameters implements Serializable {
    private String url;
    private String apiKey;
    private String className;
    private Map<String, String> header;
    private int connectionTimeout;
    private int readTimeout;
    private int writeTimeout;

    // sink schema_save_mode data_save_mode enable_auto_id enable_upsert
    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private Boolean enableAutoId;
    private Boolean enableUpsert;

    private Boolean enableVector = WeaviateSinkConfig.ENABLE_VECTOR.defaultValue();
    private String vectorField;

    public static WeaviateParameters buildWithConfig(ReadonlyConfig config) {
        WeaviateParametersBuilder builder = WeaviateParameters.builder();
        builder.url(config.get(WeaviateCommonConfig.URL));
        // Default value exists
        builder.connectionTimeout(config.get(WeaviateCommonConfig.CONNECTION_TIMEOUT));
        builder.readTimeout(config.get(WeaviateCommonConfig.CONNECTION_REQUEST_TIMEOUT));
        builder.writeTimeout(config.get(WeaviateCommonConfig.SOCKET_TIMEOUT));

        if (config.getOptional(WeaviateCommonConfig.KEY).isPresent()) {
            builder.apiKey(config.get(WeaviateCommonConfig.KEY));
        }
        if (config.getOptional(WeaviateCommonConfig.CLASS_NAME).isPresent()) {
            builder.className(config.get(WeaviateCommonConfig.CLASS_NAME));
        }

        // Sink
        if (config.getOptional(WeaviateSinkConfig.ENABLE_AUTO_ID).isPresent()) {
            builder.enableAutoId(config.get(WeaviateSinkConfig.ENABLE_AUTO_ID));
        }

        if (config.getOptional(WeaviateSinkConfig.ENABLE_UPSERT).isPresent()) {
            builder.enableUpsert(config.get(WeaviateSinkConfig.ENABLE_UPSERT));
        }

        if (config.getOptional(WeaviateSinkConfig.VECTOR_FIELD).isPresent()) {
            builder.vectorField(config.get(WeaviateSinkConfig.VECTOR_FIELD));
        }

        if (config.getOptional(WeaviateSinkConfig.ENABLE_VECTOR).isPresent()) {
            builder.enableVector(config.get(WeaviateSinkConfig.ENABLE_VECTOR));
        }

        return builder.build();
    }
}
