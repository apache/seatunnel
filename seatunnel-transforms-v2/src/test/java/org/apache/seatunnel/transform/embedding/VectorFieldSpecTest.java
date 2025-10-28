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

import org.apache.seatunnel.transform.nlpmodel.embedding.SrcFieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.VectorFieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.PayloadFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorFieldSpecTest {

    @Test
    void testMapEntryConstructorWithStringValue() {
        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>("book_intro_vector", "book_intro");
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        Assertions.assertEquals("book_intro_vector", vectorFieldSpec.getFieldName());
        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);
        Assertions.assertEquals("book_intro", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(vectorFieldSpec.isMultimodalField());
        Assertions.assertFalse(srcFieldSpec.isBinary());
    }

    @Test
    void testMapEntryConstructorWithStringValueTrimming() {
        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>("book_intro_vector", "  book_intro  ");
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);
        Assertions.assertEquals("book_intro", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
    }

    @Test
    void testMapEntryConstructorWithNullKey() {
        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(null, "book_intro");
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new VectorFieldSpec(entry));
        Assertions.assertTrue(
                exception.getMessage().contains("Field config name cannot be null or empty"));
    }

    @Test
    void testMapEntryConstructorWithEmpty() {
        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>("book_intro_vector", null);
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new VectorFieldSpec(entry));
        Assertions.assertTrue(exception.getMessage().contains("Field config value cannot be null"));

        Map.Entry<String, Object> entry2 = new AbstractMap.SimpleEntry<>("book_intro_vector", "");
        exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new VectorFieldSpec(entry2));
        Assertions.assertTrue(
                exception.getMessage().contains("Invalid field spec for output field"));
    }

    @Test
    void testMapEntryConstructorWithMapValue() {
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "book_image");
        fieldConfig.put("modality", "jpeg");
        fieldConfig.put("format", "binary");

        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>("book_field", fieldConfig);
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);

        Assertions.assertEquals("book_image", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.JPEG, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.BINARY, srcFieldSpec.getPayloadFormat());
        Assertions.assertTrue(vectorFieldSpec.isMultimodalField());
        Assertions.assertTrue(srcFieldSpec.isBinary());
    }

    @Test
    void testMapEntryConstructorWithMapValueNoModality() {
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "book_intro");
        fieldConfig.put("modality", "text");
        fieldConfig.put("format", "text");

        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>("book_field", fieldConfig);
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);

        Assertions.assertEquals("book_intro", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(vectorFieldSpec.isMultimodalField());
    }

    @Test
    void testMapEntryConstructorWithInvalidListValue() {
        List<String> textFieldConfig = Arrays.asList("text_field_1", "text_field_2");
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");

        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>(
                        "vector_field", Arrays.asList(textFieldConfig, imageFieldConfig));
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new VectorFieldSpec(entry));
        Assertions.assertTrue(
                exception.getMessage().contains("Invalid field spec for output field"));
    }

    @Test
    void testMapEntryConstructorWithSameModalityListValue() {
        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>(
                        "vector_field", Arrays.asList("text_field_1", "text_field_2"));
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        Assertions.assertEquals("vector_field", vectorFieldSpec.getFieldName());
        Assertions.assertTrue(vectorFieldSpec.isMultimodalField());

        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);
        Assertions.assertEquals("text_field_1", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(srcFieldSpec.isBinary());

        srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(1);
        Assertions.assertEquals("text_field_2", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(srcFieldSpec.isBinary());
    }

    @Test
    void testMapEntryConstructorWithDifferentModalityListValue() {
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");

        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "mp4");
        videoFieldConfig.put("format", "url");

        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>(
                        "vector_field",
                        Arrays.asList("text_field", imageFieldConfig, videoFieldConfig));
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        Assertions.assertEquals("vector_field", vectorFieldSpec.getFieldName());
        Assertions.assertTrue(vectorFieldSpec.isMultimodalField());

        SrcFieldSpec srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(0);
        Assertions.assertEquals("text_field", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(srcFieldSpec.isBinary());

        srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(1);
        Assertions.assertEquals("image_field", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.JPEG, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.URL, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(srcFieldSpec.isBinary());

        srcFieldSpec = vectorFieldSpec.getSrcFieldSpecs().get(2);
        Assertions.assertEquals("video_field", srcFieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.MP4, srcFieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.URL, srcFieldSpec.getPayloadFormat());
        Assertions.assertFalse(srcFieldSpec.isBinary());
    }
}
