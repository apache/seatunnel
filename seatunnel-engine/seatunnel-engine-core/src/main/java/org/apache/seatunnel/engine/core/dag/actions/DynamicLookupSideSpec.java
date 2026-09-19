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

/** Planner-visible description of one lookup input side. */
public final class DynamicLookupSideSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The logical plugin output consumed by this side. */
    private final String inputId;

    /** Optional resolved table path name when the config binds one concrete table. */
    private final String tablePath;

    /** Join key field names in declaration order. */
    private final List<String> keyFields;

    /** Planner-resolved source field ordinals for {@link #keyFields}. */
    private final List<Integer> keyFieldIndexes;

    public DynamicLookupSideSpec(
            String inputId,
            String tablePath,
            List<String> keyFields,
            List<Integer> keyFieldIndexes) {
        this.inputId = requireNonBlank(inputId, "inputId");
        this.tablePath = tablePath == null ? null : tablePath.trim();
        if (keyFields == null || keyFields.isEmpty()) {
            throw new IllegalArgumentException("keyFields must not be empty");
        }
        if (keyFieldIndexes == null || keyFieldIndexes.size() != keyFields.size()) {
            throw new IllegalArgumentException("keyFieldIndexes must match keyFields");
        }
        List<String> normalizedKeys = new ArrayList<>(keyFields.size());
        for (String keyField : keyFields) {
            normalizedKeys.add(requireNonBlank(keyField, "keyField"));
        }
        this.keyFields = Collections.unmodifiableList(normalizedKeys);
        List<Integer> normalizedIndexes = new ArrayList<>(keyFieldIndexes.size());
        for (Integer keyFieldIndex : keyFieldIndexes) {
            if (keyFieldIndex == null || keyFieldIndex < 0) {
                throw new IllegalArgumentException(
                        "keyFieldIndex must be non-negative: " + keyFieldIndex);
            }
            normalizedIndexes.add(keyFieldIndex);
        }
        this.keyFieldIndexes = Collections.unmodifiableList(normalizedIndexes);
    }

    public String getInputId() {
        return inputId;
    }

    public String getTablePath() {
        return tablePath;
    }

    public List<String> getKeyFields() {
        return keyFields;
    }

    public List<Integer> getKeyFieldIndexes() {
        return keyFieldIndexes;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
