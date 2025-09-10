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

package org.apache.seatunnel.transform.nlpmodel.embedding.multimodal;

import org.apache.seatunnel.transform.nlpmodel.embedding.FieldSpec;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Base64;

@Slf4j
@Getter
public class MultimodalFieldValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FieldSpec fieldSpec;
    private final Object value;

    public MultimodalFieldValue(FieldSpec fieldSpec, Object value) {
        this.value = value;
        fieldSpec.setModalityType(determineModalityType(fieldSpec, value));
        this.fieldSpec = fieldSpec;
    }

    /**
     * Determine the actual modality type based on field spec and value If not binary format,
     * analyze the value suffix to determine modality type
     */
    private ModalityType determineModalityType(FieldSpec fieldSpec, Object value) {

        if (fieldSpec.isBinary()) {
            return fieldSpec.getModalityType();
        }
        if (value != null) {
            String valueStr = value.toString();
            ModalityType detectedType = ModalityType.fromFileSuffix(valueStr);
            if (detectedType != null) {
                log.debug(
                        "Auto-detected modality type '{}' from value: {}", detectedType, valueStr);
                return detectedType;
            }
        }
        return fieldSpec.getModalityType();
    }

    public String toBase64() {
        if (value == null) {
            throw new IllegalArgumentException("Binary data cannot be null or empty");
        }
        return Base64.getEncoder().encodeToString(value.toString().getBytes());
    }
}
