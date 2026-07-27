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

package org.apache.seatunnel.engine.core.dag.actions;

import java.io.Serializable;
import java.util.Objects;

/** One projected output field of a dynamic lookup result. */
public final class DynamicLookupProjectionField implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Declares which side contributes the source field. */
    public enum InputSide {
        FACT,
        DIMENSION
    }

    /** The input side that provides the field value. */
    private final InputSide inputSide;

    /** The field name from the selected input side. */
    private final String sourceFieldName;

    /** Planner-resolved source field ordinal. */
    private final int sourceFieldIndex;

    /** The output field name after optional aliasing. */
    private final String outputFieldName;

    public DynamicLookupProjectionField(
            InputSide inputSide,
            String sourceFieldName,
            int sourceFieldIndex,
            String outputFieldName) {
        this.inputSide = Objects.requireNonNull(inputSide, "inputSide");
        this.sourceFieldName = requireNonBlank(sourceFieldName, "sourceFieldName");
        if (sourceFieldIndex < 0) {
            throw new IllegalArgumentException(
                    "sourceFieldIndex must be non-negative: " + sourceFieldIndex);
        }
        this.sourceFieldIndex = sourceFieldIndex;
        this.outputFieldName = requireNonBlank(outputFieldName, "outputFieldName");
    }

    public InputSide getInputSide() {
        return inputSide;
    }

    public String getSourceFieldName() {
        return sourceFieldName;
    }

    public int getSourceFieldIndex() {
        return sourceFieldIndex;
    }

    public String getOutputFieldName() {
        return outputFieldName;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
