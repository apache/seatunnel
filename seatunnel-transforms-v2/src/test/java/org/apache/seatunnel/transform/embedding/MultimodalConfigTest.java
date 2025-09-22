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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;
import org.apache.seatunnel.transform.nlpmodel.ModelTransformConfig;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingTransform;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingTransformConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MultimodalConfigTest {

    private CatalogTable createTestCatalogTable() {
        Column[] columns = {
            PhysicalColumn.of("text_field", BasicType.STRING_TYPE, 255L, true, null, ""),
            PhysicalColumn.of("image_field", BasicType.STRING_TYPE, 255L, true, null, ""),
            PhysicalColumn.of("video_field", BasicType.STRING_TYPE, 255L, true, null, ""),
            PhysicalColumn.of("mixed_field", BasicType.STRING_TYPE, 255L, true, null, "")
        };

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        return CatalogTable.of(
                TableIdentifier.of("test", "test", "test_table"),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "Test table for multimodal embedding");
    }

    @Test
    void testIsMultimodalFieldsDetectionWithTextOnly() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Only text fields - should not be multimodal
        Map<String, Object> vectorizationFields = new HashMap<>();
        vectorizationFields.put("text_vector", "text_field"); // Default to text type

        // Explicitly text type using object format
        Map<String, Object> textFieldConfig = new HashMap<>();
        textFieldConfig.put("field", "mixed_field");
        textFieldConfig.put("modality", "text");
        vectorizationFields.put("text_vector2", textFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);

        Assertions.assertNotNull(transform);
        Assertions.assertFalse(transform.isMultimodalFields());
    }

    @Test
    void testIsMultimodalFieldsDetectionWithImageField() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Include image field - should be multimodal
        Map<String, Object> vectorizationFields = new HashMap<>();
        vectorizationFields.put("text_vector", "text_field");

        // Image type using object format (use specific image format)
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");
        vectorizationFields.put("image_vector", imageFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
        Assertions.assertNotNull(transform);
        Assertions.assertTrue(transform.isMultimodalFields());
    }

    @Test
    void testIsMultimodalFieldsDetectionWithVideoField() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Include video field - should be multimodal
        Map<String, Object> vectorizationFields = new HashMap<>();
        vectorizationFields.put("text_vector", "text_field");

        // Video type using object format (use specific video format)
        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "mp4");
        videoFieldConfig.put("format", "url");
        vectorizationFields.put("video_vector", videoFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
        Assertions.assertNotNull(transform);
        Assertions.assertTrue(transform.isMultimodalFields());
    }

    @Test
    void testIsMultimodalFieldsDetectionWithMixedFields() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Include multiple modality types - should be multimodal
        Map<String, Object> vectorizationFields = new HashMap<>();

        // Text field using object format
        Map<String, Object> textFieldConfig = new HashMap<>();
        textFieldConfig.put("field", "text_field");
        textFieldConfig.put("modality", "text");
        vectorizationFields.put("text_vector", textFieldConfig);

        // Image field using object format (use specific image format)
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "png");
        imageFieldConfig.put("format", "url");
        vectorizationFields.put("image_vector", imageFieldConfig);

        // Video field using object format (use specific video format)
        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "avi");
        videoFieldConfig.put("format", "url");
        vectorizationFields.put("video_vector", videoFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // This should work since DOUBAO supports multimodal
        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
        Assertions.assertNotNull(transform);
        Assertions.assertTrue(transform.isMultimodalFields());
    }

    @Test
    void testMultimodalModelValidationFailure() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        // Use a provider that doesn't support multimodal (e.g., OPENAI text-only models)
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.OPENAI.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "text-embedding-3-small");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.openai.com/v1/embeddings");

        Map<String, Object> vectorizationFields = new HashMap<>();
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "webp");
        imageFieldConfig.put("format", "url");
        vectorizationFields.put("image_vector", imageFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should throw IllegalArgumentException when opening
        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, transform::open);

        Assertions.assertTrue(exception.getMessage().contains("does not support multimodal"));
    }

    @Test
    void testMultimodalDetectionWithDefaultTextType() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.OPENAI.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Fields without explicit type specification default to text
        Map<String, Object> vectorizationFields = new HashMap<>();
        vectorizationFields.put("text_vector1", "text_field");
        vectorizationFields.put("text_vector2", "mixed_field");
        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should not be detected as multimodal since all fields default to text
        EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
        Assertions.assertNotNull(transform);
        Assertions.assertFalse(transform.isMultimodalFields());
    }

    @Test
    void testMultimodalDetectionWithInvalidModalityType() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        Map<String, Object> vectorizationFields = new HashMap<>();

        // Invalid modality type using object format
        Map<String, Object> invalidFieldConfig = new HashMap<>();
        invalidFieldConfig.put("field", "text_field");
        invalidFieldConfig.put("modality", "audio");
        vectorizationFields.put("invalid_vector", invalidFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should throw exception due to unsupported modality type
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new EmbeddingTransform(config, catalogTable));
        Assertions.assertTrue(exception.getMessage().contains("Invalid field spec"));
    }

    @Test
    void testMultimodalDetectionWithNonExistentField() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        Map<String, Object> vectorizationFields = new HashMap<>();

        Map<String, Object> nonExistentFieldConfig = new HashMap<>();
        nonExistentFieldConfig.put("field", "nonexistent_field");
        nonExistentFieldConfig.put("modality", "gif");
        vectorizationFields.put("nonexistent_vector", nonExistentFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class, () -> new EmbeddingTransform(config, catalogTable));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("'Embedding' transform not found in upstream schema"));
    }

    @Test
    void testMultimodalDetectionCaseSensitivity() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Test case insensitive modality type parsing
        Map<String, Object> vectorizationFields = new HashMap<>();

        // Uppercase modality (use specific format)
        Map<String, Object> imageFieldConfig1 = new HashMap<>();
        imageFieldConfig1.put("field", "image_field");
        imageFieldConfig1.put("modality", "JPEG");
        vectorizationFields.put("image_vector1", imageFieldConfig1);

        Map<String, Object> imageFieldConfig2 = new HashMap<>();
        imageFieldConfig2.put("field", "image_field");
        imageFieldConfig2.put("modality", "Png");
        vectorizationFields.put("image_vector2", imageFieldConfig2);

        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "MP4");
        vectorizationFields.put("video_vector", videoFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Should work with case insensitive modality types
        Assertions.assertDoesNotThrow(
                () -> {
                    EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
                });
    }

    @Test
    void testMultimodalDetectionWithWhitespace() {
        CatalogTable catalogTable = createTestCatalogTable();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ModelTransformConfig.MODEL_PROVIDER.key(), ModelProvider.DOUBAO.name());
        configMap.put(ModelTransformConfig.MODEL.key(), "doubao-embedding-vision");
        configMap.put(ModelTransformConfig.API_KEY.key(), "test-api-key");
        configMap.put(ModelTransformConfig.API_PATH.key(), "https://api.test.com/embeddings");

        // Test field specifications with whitespace
        Map<String, Object> vectorizationFields = new HashMap<>();
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", " image_field ");
        imageFieldConfig.put("modality", "bmp");
        vectorizationFields.put("image_vector1", imageFieldConfig);

        // Field with whitespace in modality
        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "  mov  ");
        vectorizationFields.put("video_vector", videoFieldConfig);

        configMap.put(EmbeddingTransformConfig.VECTORIZATION_FIELDS.key(), vectorizationFields);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        Assertions.assertDoesNotThrow(
                () -> {
                    EmbeddingTransform transform = new EmbeddingTransform(config, catalogTable);
                });
    }
}
