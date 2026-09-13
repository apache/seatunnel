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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Planner-visible lookup contract parsed from the {@code dynamic_lookup} section. */
public final class DynamicLookupDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Supported output behavior when a fact row does or does not find a dimension row. */
    public enum JoinType {
        LEFT,
        INNER
    }

    /** Logical plugin output exposed to later stages. */
    private final String outputId;

    /** Join fact side contract. */
    private final DynamicLookupSideSpec fact;

    /** Join dimension side contract. */
    private final DynamicLookupSideSpec dimension;

    /** Runtime join behavior for unmatched fact rows. */
    private final JoinType joinType;

    /** Ordered output field projection. */
    private final List<DynamicLookupProjectionField> projectionFields;

    public DynamicLookupDescriptor(
            String outputId,
            DynamicLookupSideSpec fact,
            DynamicLookupSideSpec dimension,
            JoinType joinType,
            List<DynamicLookupProjectionField> projectionFields) {
        this.outputId = requireNonBlank(outputId, "outputId");
        this.fact = Objects.requireNonNull(fact, "fact");
        this.dimension = Objects.requireNonNull(dimension, "dimension");
        this.joinType = Objects.requireNonNull(joinType, "joinType");
        if (projectionFields == null || projectionFields.isEmpty()) {
            throw new IllegalArgumentException("projectionFields must not be empty");
        }
        this.projectionFields = Collections.unmodifiableList(new ArrayList<>(projectionFields));
    }

    public String getOutputId() {
        return outputId;
    }

    public DynamicLookupSideSpec getFact() {
        return fact;
    }

    public DynamicLookupSideSpec getDimension() {
        return dimension;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public List<DynamicLookupProjectionField> getProjectionFields() {
        return projectionFields;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
