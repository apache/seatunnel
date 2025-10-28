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

import org.apache.seatunnel.transform.nlpmodel.embedding.SrcField;
import org.apache.seatunnel.transform.nlpmodel.embedding.SrcFieldSpec;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;

@Slf4j
@Getter
public class MultimodalFieldValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<SrcField> srcFields;

    public MultimodalFieldValue(List<SrcField> srcFields) {
        this.srcFields = srcFields;
        for (SrcField srcField : srcFields) {
            SrcFieldSpec fieldSpec = srcField.getFieldSpec();
            ModalityType modalityType = determineModalityType(fieldSpec, srcField.getFieldValue());
            fieldSpec.setModalityType(modalityType);
        }
    }

    /**
     * Determine the actual modality type based on field spec and value If not binary format,
     * analyze the value suffix to determine modality type
     */
    private ModalityType determineModalityType(SrcFieldSpec fieldSpec, Object fieldValue) {

        if (fieldSpec.isBinary()) {
            return fieldSpec.getModalityType();
        }
        if (fieldValue != null) {
            String valueStr = fieldValue.toString();
            ModalityType detectedType = ModalityType.fromFileSuffix(valueStr);
            if (detectedType != null) {
                log.debug(
                        "Auto-detected modality type '{}' from value: {}", detectedType, valueStr);
                return detectedType;
            }
        }
        return fieldSpec.getModalityType();
    }
}
