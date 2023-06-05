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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.COLLECTION_NAME;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.DIMENSION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.EMBEDDINGS_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.MILVUS_HOST;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.MILVUS_PORT;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.OPENAI_API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.OPENAI_ENGINE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.PARTITION_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusConfig.USERNAME;

@AutoService(Factory.class)
public class MilvusSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Milvus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MILVUS_HOST, MILVUS_PORT, COLLECTION_NAME, USERNAME, PASSWORD)
                .optional(
                        PARTITION_FIELD,
                        OPENAI_ENGINE,
                        OPENAI_API_KEY,
                        DIMENSION,
                        EMBEDDINGS_FIELDS)
                .build();
    }
}
