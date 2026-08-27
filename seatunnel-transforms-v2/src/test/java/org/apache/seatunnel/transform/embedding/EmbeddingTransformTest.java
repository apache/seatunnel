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

package org.apache.seatunnel.transform.embedding;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class EmbeddingTransformTest {

    @Test
    void testOutputColumns() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        String sourceConfig =
                "{\"path\":\"/seatunnel/test_csv_data.csv\",\"bucket\":\"s3a://ltchen\",\"fs.s3a.endpoint\":\"tos-s3-cn-beijing.volces.com\",\"fs.s3a.aws.credentials.provider\":\"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\",\"file_format_type\":\"csv\",\"access_key\":\"xxx\",\"secret_key\":\"xxx\",\"csv_use_header_line\":true,\"field_delimiter\":\",\",\"schema\":{\"fields\":{\"id\":\"int\",\"code\":\"int\",\"data\":\"string\",\"success\":\"boolean\"},\"primaryKey\":{\"name\":\"id\",\"columnNames\":[\"id\"]}},\"plugin_name\":\"S3File\"}";
        Map<String, Object> sourceConfigMap =
                objectMapper.readValue(sourceConfig, new TypeReference<Map<String, Object>>() {});
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(sourceConfigMap);
        CatalogTable inputCatalogTable = CatalogTableUtil.buildWithConfig("S3File", readonlyConfig);

        int dimension = 1024;
        String embeddingConfig =
                "{\"model_provider\":\"AMAZON\",\"model\":\"amazon.titan-embed-text-v2:0\",\"aws_region\": \"us-east-1\", \"api_key\":\"xxx\",\"secret_key\":\"xxx\",\"api_path\": \"https://aws.amazon.com/bedrock/amazon-models\", \"dimension\": "
                        + dimension
                        + ",\"vectorization_fields\":{\"data_vector\":\"data\"},\"plugin_name\":\"Embedding\"}";
        Map<String, Object> embeddingConfigMap =
                objectMapper.readValue(
                        embeddingConfig, new TypeReference<Map<String, Object>>() {});
        ReadonlyConfig config = ReadonlyConfig.fromMap(embeddingConfigMap);
        EmbeddingTransform embeddingTransform = new EmbeddingTransform(config, inputCatalogTable);

        Column[] columns = embeddingTransform.getOutputColumns();
        for (Column column : columns) {
            Assertions.assertEquals(dimension, column.getScale());
        }
    }
}
