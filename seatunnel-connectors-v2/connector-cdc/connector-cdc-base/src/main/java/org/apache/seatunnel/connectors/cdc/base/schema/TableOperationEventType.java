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
import org.apache.seatunnel.api.table.operation.TableOperationType;

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
 * Maps user-facing {@code table-operations.include} / {@code table-operations.exclude} names to
 * {@link EventType}.
 */
public final class TableOperationEventType {

    private static final Map<String, EventType> CANONICAL_NAME_TO_EVENT_TYPE;

    static {
        Map<String, EventType> map = new LinkedHashMap<>();
        map.put(
                TableOperationType.TRUNCATE_TABLE.canonicalName(),
                EventType.TABLE_OPERATION_TRUNCATE);
        CANONICAL_NAME_TO_EVENT_TYPE = Collections.unmodifiableMap(map);
    }

    private TableOperationEventType() {}

    public static String validNames() {
        return String.join(", ", CANONICAL_NAME_TO_EVENT_TYPE.keySet());
    }

    public static EventType fromCanonicalName(String canonicalName) {
        if (canonicalName == null) {
            throw new IllegalArgumentException(
                    "Table operation event type name must not be null. Valid names are: "
                            + validNames());
        }
        String normalized = canonicalName.trim().toLowerCase();
        EventType eventType = CANONICAL_NAME_TO_EVENT_TYPE.get(normalized);
        if (eventType == null) {
            throw new IllegalArgumentException(
                    "Unknown table operation event type '"
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
                .map(TableOperationEventType::fromCanonicalName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    static List<String> canonicalNames() {
        return new ArrayList<>(CANONICAL_NAME_TO_EVENT_TYPE.keySet());
    }
}
