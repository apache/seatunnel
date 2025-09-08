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

import org.apache.seatunnel.transform.nlpmodel.embedding.FieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.PayloadFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class FieldSpecTest {

    @Test
    void testMapEntryConstructorWithStringValue() {
        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>("book_intro_vector", "book_intro");
        FieldSpec fieldSpec = new FieldSpec(entry);
        Assertions.assertEquals("book_intro", fieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, fieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, fieldSpec.getPayloadFormat());
        Assertions.assertFalse(fieldSpec.isMultimodalField());
        Assertions.assertFalse(fieldSpec.isBinary());
    }

    @Test
    void testMapEntryConstructorWithStringValueTrimming() {
        Map.Entry<String, Object> entry =
                new AbstractMap.SimpleEntry<>("book_intro_vector", "  book_intro  ");
        FieldSpec fieldSpec = new FieldSpec(entry);
        Assertions.assertEquals("book_intro", fieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, fieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, fieldSpec.getPayloadFormat());
    }

    @Test
    void testMapEntryConstructorWithNullKey() {
        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(null, "book_intro");
        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> new FieldSpec(entry));
        Assertions.assertTrue(exception.getMessage().contains("Field spec cannot be null"));
    }

    @Test
    void testMapEntryConstructorWithEmpty() {
        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>("book_intro_vector", null);
        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> new FieldSpec(entry));
        Assertions.assertTrue(
                exception.getMessage().contains("Invalid field spec for output field"));

        Map.Entry<String, Object> entry2 = new AbstractMap.SimpleEntry<>("book_intro_vector", "");
        exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> new FieldSpec(entry2));
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

        FieldSpec fieldSpec = new FieldSpec(entry);

        Assertions.assertEquals("book_image", fieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.JPEG, fieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.BINARY, fieldSpec.getPayloadFormat());
        Assertions.assertTrue(fieldSpec.isMultimodalField());
        Assertions.assertTrue(fieldSpec.isBinary());
    }

    @Test
    void testMapEntryConstructorWithMapValueNoModality() {
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "book_intro");
        fieldConfig.put("modality", "text");
        fieldConfig.put("format", "text");

        Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>("book_field", fieldConfig);

        FieldSpec fieldSpec = new FieldSpec(entry);

        Assertions.assertEquals("book_intro", fieldSpec.getFieldName());
        Assertions.assertEquals(ModalityType.TEXT, fieldSpec.getModalityType());
        Assertions.assertEquals(PayloadFormat.TEXT, fieldSpec.getPayloadFormat());
        Assertions.assertFalse(fieldSpec.isMultimodalField());
    }
}
