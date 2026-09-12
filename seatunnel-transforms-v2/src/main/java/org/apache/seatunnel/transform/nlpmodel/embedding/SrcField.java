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

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Base64;

@Data
@Slf4j
public class SrcField implements Serializable {

    private static final long serialVersionUID = 1L;

    private SrcFieldSpec fieldSpec;

    private Object fieldValue;

    public SrcField(SrcFieldSpec spec, Object value) {
        // create a new object avoid to mutate original src field spec
        this.fieldSpec =
                new SrcFieldSpec(
                        spec.getFieldName(),
                        spec.getModalityType(),
                        spec.getPayloadFormat(),
                        spec.isModalityTypeExplicitlyConfigured());
        this.fieldValue = value;
        determineModalityType();
    }

    /**
     * Determine the actual modality type based on field spec and value. The configured modality
     * type is always respected when it was explicitly provided by the user. Auto-detection from the
     * value suffix only happens for URL payloads whose modality type was not explicitly configured,
     * so that plain text values are never misclassified as image/video by their content.
     */
    private void determineModalityType() {
        if (fieldSpec.isModalityTypeExplicitlyConfigured() || !fieldSpec.isUrl()) {
            return;
        }
        if (fieldValue != null) {
            String valueStr = fieldValue.toString();
            ModalityType detectedType = ModalityType.fromFileSuffix(valueStr);
            if (detectedType != null) {
                log.debug(
                        "Auto-detected modality type '{}' from value: {}", detectedType, valueStr);
                fieldSpec.setModalityType(detectedType);
            }
        }
    }

    public String toBase64() {
        if (fieldSpec == null || !fieldSpec.isBinary()) {
            throw new IllegalArgumentException("Payload format must be binary");
        }
        if (fieldValue == null) {
            throw new IllegalArgumentException("Binary data cannot be null or empty");
        }
        if (fieldValue instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) fieldValue);
        } else {
            return Base64.getEncoder().encodeToString(fieldValue.toString().getBytes());
        }
    }
}
