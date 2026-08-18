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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import java.util.Collection;
import java.util.List;

/**
 * Reserved output columns emitted by the HugeGraph Source. Every reserved column name starts with
 * {@code ~}, which HugeGraph forbids for property key names, so they never collide with user
 * properties.
 *
 * <p>These columns carry the pre-assembled HugeGraph element ids (the vertex id in {@code ~id}, the
 * edge endpoint vertex ids in {@code ~source_id}/{@code ~target_id}). When a Sink mapping sets a
 * single reserved column as its {@code idFields} / endpoint {@code idFields}, the mapper consumes
 * that pre-assembled id directly instead of re-building one from primary-key columns — this is what
 * makes a lossless HugeGraph → HugeGraph clone of edges and of CUSTOMIZE-id vertices possible.
 */
public final class ReservedColumns {

    public static final String PREFIX = "~";

    public static final String ID = "~id";
    public static final String LABEL = "~label";
    public static final String SOURCE_ID = "~source_id";
    public static final String SOURCE_LABEL = "~source_label";
    public static final String TARGET_ID = "~target_id";
    public static final String TARGET_LABEL = "~target_label";

    private ReservedColumns() {}

    /** Whether {@code field} is a reserved Source column (starts with {@code ~}). */
    public static boolean isReserved(String field) {
        return field != null && field.startsWith(PREFIX);
    }

    /**
     * Whether an {@code idFields} list requests raw-id passthrough: exactly one field, and that
     * field is a reserved Source column carrying a pre-assembled id.
     */
    public static boolean isRawIdPassthrough(List<String> idFields) {
        return idFields != null && idFields.size() == 1 && isReserved(idFields.get(0));
    }

    /**
     * Removes reserved column names from the collection in-place, so callers that build a property
     * set from all row fields (e.g. mappers and validators) can strip the non-property passthrough
     * columns with one call. Returns {@code fields} for fluent use.
     */
    public static <T extends Collection<String>> T stripReserved(T fields) {
        fields.removeIf(ReservedColumns::isReserved);
        return fields;
    }
}
