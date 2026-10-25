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

package org.apache.seatunnel.connectors.cdc.base.schema;

import org.apache.seatunnel.api.event.EventType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maps the user-facing canonical names used in {@code schema-changes.include} / {@code
 * schema-changes.exclude} to the internal {@link EventType} enum, so users never need to know
 * internal class or enum names. {@code update.columns} is a group alias for all column-level
 * changes.
 */
public final class SchemaChangeEventType {

    private static final Map<String, EventType> CANONICAL_NAME_TO_EVENT_TYPE;

    static {
        Map<String, EventType> map = new LinkedHashMap<>();
        map.put("add.column", EventType.SCHEMA_CHANGE_ADD_COLUMN);
        map.put("drop.column", EventType.SCHEMA_CHANGE_DROP_COLUMN);
        map.put("modify.column", EventType.SCHEMA_CHANGE_MODIFY_COLUMN);
        map.put("change.column", EventType.SCHEMA_CHANGE_CHANGE_COLUMN);
        map.put("update.columns", EventType.SCHEMA_CHANGE_UPDATE_COLUMNS);
        // NOTE: rename.table (SCHEMA_CHANGE_RENAME_TABLE) is intentionally NOT exposed yet. CDC has
        // no end-to-end handling for table renames: the DDL is never parsed into an
        // AlterTableNameEvent, the schema handlers treat that event as a no-op, and no sink applies
        // it. Exposing it as a filterable name would advertise a capability that does not exist.
        // It should be added back only once table-rename is implemented end-to-end (see the
        // rename-table design follow-up).
        CANONICAL_NAME_TO_EVENT_TYPE = Collections.unmodifiableMap(map);
    }

    private SchemaChangeEventType() {}

    public static String validNames() {
        return String.join(", ", CANONICAL_NAME_TO_EVENT_TYPE.keySet());
    }

    public static EventType fromCanonicalName(String canonicalName) {
        if (canonicalName == null) {
            throw new IllegalArgumentException(
                    "Schema change event type name must not be null. Valid names are: "
                            + validNames());
        }
        String normalized = canonicalName.trim().toLowerCase();
        EventType eventType = CANONICAL_NAME_TO_EVENT_TYPE.get(normalized);
        if (eventType == null) {
            throw new IllegalArgumentException(
                    "Unknown schema change event type '"
                            + canonicalName
                            + "'. Valid names are: "
                            + validNames());
        }
        return eventType;
    }

    public static Set<EventType> fromCanonicalNames(Collection<String> canonicalNames) {
        if (canonicalNames == null || canonicalNames.isEmpty()) {
            return Collections.emptySet();
        }
        return canonicalNames.stream()
                .map(SchemaChangeEventType::fromCanonicalName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Visible for testing: the supported canonical names. */
    static List<String> canonicalNames() {
        return new ArrayList<>(CANONICAL_NAME_TO_EVENT_TYPE.keySet());
    }
}
