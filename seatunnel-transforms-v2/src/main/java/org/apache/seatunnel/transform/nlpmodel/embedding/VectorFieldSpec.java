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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
public class VectorFieldSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private String fieldName;

    private List<SrcFieldSpec> srcFieldSpecs;

    public VectorFieldSpec(Map.Entry<String, Object> fieldConfig) {
        this.fieldName = fieldConfig.getKey();
        if (StringUtils.isBlank(fieldName)) {
            throw new IllegalArgumentException("Field config name cannot be null or empty");
        }
        Object fieldConfigValue = fieldConfig.getValue();
        if (fieldConfigValue == null) {
            throw new IllegalArgumentException(
                    "Field config value cannot be null for field: " + fieldName);
        }

        srcFieldSpecs = new ArrayList<>();
        try {
            if (fieldConfigValue instanceof String) {
                srcFieldSpecs.add(new SrcFieldSpec((String) fieldConfigValue));
            } else if (fieldConfigValue instanceof Map) {
                srcFieldSpecs.add(new SrcFieldSpec((Map<String, Object>) fieldConfigValue));
            } else {
                List<Object> fieldConfigValues = (List<Object>) fieldConfigValue;
                for (Object fieldConfigValueItem : fieldConfigValues) {
                    if (fieldConfigValueItem instanceof String) {
                        srcFieldSpecs.add(new SrcFieldSpec((String) fieldConfigValueItem));
                    } else if (fieldConfigValueItem instanceof Map) {
                        srcFieldSpecs.add(
                                new SrcFieldSpec((Map<String, Object>) fieldConfigValueItem));
                    } else {
                        String errorMessage =
                                String.format(
                                        "Invalid field spec for output field '%s': %s",
                                        fieldName, fieldConfig);
                        throw new IllegalArgumentException(errorMessage);
                    }
                }
            }
        } catch (Exception e) {
            String errorMessage =
                    String.format(
                            "Invalid field spec for output field '%s': %s", fieldName, fieldConfig);
            throw new IllegalArgumentException(errorMessage, e);
        }
    }

    public boolean isMultimodalField() {
        return srcFieldSpecs.size() > 1
                || srcFieldSpecs.stream()
                        .anyMatch(f -> !ModalityType.TEXT.equals(f.getModalityType()));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        VectorFieldSpec that = (VectorFieldSpec) object;
        return Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }
}
