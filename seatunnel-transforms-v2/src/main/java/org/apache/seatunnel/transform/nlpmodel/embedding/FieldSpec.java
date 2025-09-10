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

package org.apache.seatunnel.transform.nlpmodel.embedding;

import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.PayloadFormat;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class FieldSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String fieldName;
    private ModalityType modalityType;
    private PayloadFormat payloadFormat;

    public FieldSpec(String fieldName) {
        this.fieldName = fieldName;
        this.modalityType = ModalityType.TEXT;
        this.payloadFormat = PayloadFormat.TEXT;
    }

    public FieldSpec(Map.Entry<String, Object> fieldConfig) {
        String outputFieldName = fieldConfig.getKey();
        if (outputFieldName == null) {
            throw new IllegalArgumentException("Field spec cannot be null");
        }
        Object fieldValue = fieldConfig.getValue();
        try {
            if (fieldValue instanceof String) {
                parseBasicFieldSpec((String) fieldValue);
            } else {
                Map<String, Object> fieldSpecConfig = (Map<String, Object>) fieldValue;
                parseMultimodalFieldSpec(fieldSpecConfig);
            }
        } catch (Exception e) {
            String errorMessage =
                    String.format(
                            "Invalid field spec for output field '%s': %s",
                            outputFieldName, fieldConfig);
            throw new IllegalArgumentException(errorMessage, e);
        }
    }

    /** Parse basic field spec: just the field name, defaults to TEXT modality and default format */
    private void parseBasicFieldSpec(String fieldSpec) {
        if (fieldSpec == null || fieldSpec.trim().isEmpty()) {
            throw new IllegalArgumentException("Field spec cannot be null or empty");
        }
        this.fieldName = fieldSpec.trim();
        this.modalityType = ModalityType.TEXT;
        this.payloadFormat = PayloadFormat.TEXT;
    }

    /**
     * Parse multimodal field spec: field name, modality, and format Supports both formats: 1.
     * Separate modality and format
     */
    private void parseMultimodalFieldSpec(Map<String, Object> fieldConfig) {
        if (fieldConfig == null || fieldConfig.isEmpty()) {
            throw new IllegalArgumentException("Field configuration cannot be null or empty");
        }

        Object fieldNameObj = fieldConfig.get("field");
        if (fieldNameObj == null) {
            throw new IllegalArgumentException(
                    "Field name ('field') is required in field configuration");
        }

        this.fieldName = fieldNameObj.toString().trim();
        if (this.fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be empty");
        }
        Object modalityObj = fieldConfig.get("modality");
        if (modalityObj != null) {
            this.modalityType = ModalityType.ofName(modalityObj.toString());
            Object formatObj = fieldConfig.get("format");
            if (formatObj != null) {
                this.payloadFormat = PayloadFormat.ofName(formatObj.toString());
            }
        } else {
            this.modalityType = ModalityType.TEXT;
            Object formatObj = fieldConfig.get("format");
            if (formatObj != null) {
                this.payloadFormat = PayloadFormat.ofName(formatObj.toString());
            } else {
                this.payloadFormat = PayloadFormat.TEXT;
            }
        }
    }

    public boolean isMultimodalField() {
        return !ModalityType.TEXT.equals(modalityType);
    }

    public boolean isBinary() {
        return PayloadFormat.BINARY.equals(payloadFormat);
    }
}
