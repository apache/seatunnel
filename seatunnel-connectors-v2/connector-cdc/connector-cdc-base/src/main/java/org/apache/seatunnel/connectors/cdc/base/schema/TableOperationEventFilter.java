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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Include/exclude filter for CDC table-operation events. Applied after events are normalized and
 * before they are sent downstream.
 */
public final class TableOperationEventFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<EventType> includeTypes;
    private final Set<EventType> excludeTypes;

    public TableOperationEventFilter(Set<EventType> includeTypes, Set<EventType> excludeTypes) {
        this.includeTypes = new HashSet<>(includeTypes);
        this.excludeTypes = new HashSet<>(excludeTypes);
    }

    public static TableOperationEventFilter fromConfig(ReadonlyConfig config) {
        List<String> include = config.get(SourceOptions.TABLE_OPERATIONS_INCLUDE);
        List<String> exclude = config.get(SourceOptions.TABLE_OPERATIONS_EXCLUDE);
        return new TableOperationEventFilter(
                TableOperationEventType.fromCanonicalNames(include),
                TableOperationEventType.fromCanonicalNames(exclude));
    }

    public static void validateOptions(ReadonlyConfig config) {
        validateNames(
                SourceOptions.TABLE_OPERATIONS_INCLUDE.key(),
                config.get(SourceOptions.TABLE_OPERATIONS_INCLUDE));
        validateNames(
                SourceOptions.TABLE_OPERATIONS_EXCLUDE.key(),
                config.get(SourceOptions.TABLE_OPERATIONS_EXCLUDE));
    }

    private static void validateNames(String optionKey, List<String> names) {
        try {
            TableOperationEventType.fromCanonicalNames(names);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid value for option '" + optionKey + "'. " + e.getMessage(), e);
        }
    }

    public boolean isNoOp() {
        return includeTypes.isEmpty() && excludeTypes.isEmpty();
    }

    /** @return the event when eligible, or {@code null} when filtered out */
    public TableOperationEvent filter(TableOperationEvent event) {
        if (event == null || isNoOp()) {
            return event;
        }
        return isEligible(event.getEventType()) ? event : null;
    }

    private boolean isEligible(EventType type) {
        boolean included = includeTypes.isEmpty() || includeTypes.contains(type);
        return included && !excludeTypes.contains(type);
    }
}
