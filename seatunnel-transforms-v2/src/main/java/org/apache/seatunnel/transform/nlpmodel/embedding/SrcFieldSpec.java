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
public class SrcFieldSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String fieldName;
    private ModalityType modalityType;
    private PayloadFormat payloadFormat;

    /**
     * Whether the modality type was explicitly configured by the user. When false, the actual
     * modality type can be auto-detected from the runtime value suffix; when true, the configured
     * modality type must be respected and never overridden.
     */
    private boolean modalityTypeExplicitlyConfigured;

    /** Parse basic field spec: just the field name, defaults to TEXT modality and default format */
    public SrcFieldSpec(String fieldName) {
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
        this.modalityType = ModalityType.TEXT;
        this.payloadFormat = PayloadFormat.TEXT;
        this.modalityTypeExplicitlyConfigured = false;
    }

    /**
     * Parse multimodal field spec: field name, modality, and format Supports both formats: 1.
     * Separate modality and format
     */
    public SrcFieldSpec(Map<String, Object> fieldConfig) {
        if (fieldConfig == null || fieldConfig.isEmpty()) {
            throw new IllegalArgumentException("Field config cannot be null or empty");
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
            this.modalityTypeExplicitlyConfigured = true;
            Object formatObj = fieldConfig.get("format");
            if (formatObj != null) {
                this.payloadFormat = PayloadFormat.ofName(formatObj.toString());
            }
        } else {
            this.modalityType = ModalityType.TEXT;
            this.modalityTypeExplicitlyConfigured = false;
            Object formatObj = fieldConfig.get("format");
            if (formatObj != null) {
                this.payloadFormat = PayloadFormat.ofName(formatObj.toString());
            } else {
                this.payloadFormat = PayloadFormat.TEXT;
            }
        }
    }

    public SrcFieldSpec(
            String fieldName,
            ModalityType modalityType,
            PayloadFormat payloadFormat,
            boolean modalityTypeExplicitlyConfigured) {
        this.fieldName = fieldName;
        this.modalityType = modalityType;
        this.payloadFormat = payloadFormat;
        this.modalityTypeExplicitlyConfigured = modalityTypeExplicitlyConfigured;
    }

    public boolean isBinary() {
        return PayloadFormat.BINARY.equals(payloadFormat);
    }

    public boolean isUrl() {
        return PayloadFormat.URL.equals(payloadFormat);
    }
}
