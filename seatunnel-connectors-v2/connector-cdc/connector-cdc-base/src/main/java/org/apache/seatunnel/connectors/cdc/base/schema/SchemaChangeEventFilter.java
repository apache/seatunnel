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
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Include/exclude filter for CDC schema change event types.
 *
 * <p>Applied after schema change events are normalized to the SeaTunnel event model, and before
 * they are emitted to downstream schema-change coordination. A filtered-out event is neither
 * forwarded to downstream coordination nor applied to the produced schema, so the produced row
 * shape stays in lockstep with the (filtered) sink schema — this is what keeps a column whose
 * {@code drop.column} was suppressed by {@code exclude} present on both sides.
 *
 * <p>Precedence (deterministic):
 *
 * <ol>
 *   <li>if {@code include} is non-empty, only included event types are eligible;
 *   <li>{@code exclude} is then applied;
 *   <li>{@code exclude} wins when the same type appears in both lists.
 * </ol>
 *
 * <p>{@code update.columns} acts as a group alias for all column-level changes ({@code add.column},
 * {@code drop.column}, {@code modify.column}, {@code change.column}): including it admits the whole
 * group, excluding it suppresses the whole group.
 */
public final class SchemaChangeEventFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<EventType> includeTypes;
    private final Set<EventType> excludeTypes;

    public SchemaChangeEventFilter(Set<EventType> includeTypes, Set<EventType> excludeTypes) {
        this.includeTypes = new HashSet<>(includeTypes);
        this.excludeTypes = new HashSet<>(excludeTypes);
    }

    /** Builds a filter from the {@code schema-changes.include} / {@code exclude} options. */
    public static SchemaChangeEventFilter fromConfig(ReadonlyConfig config) {
        List<String> include = config.get(SourceOptions.SCHEMA_CHANGES_INCLUDE);
        List<String> exclude = config.get(SourceOptions.SCHEMA_CHANGES_EXCLUDE);
        return new SchemaChangeEventFilter(
                SchemaChangeEventType.fromCanonicalNames(include),
                SchemaChangeEventType.fromCanonicalNames(exclude));
    }

    /**
     * Validates the {@code schema-changes.include} / {@code schema-changes.exclude} option values.
     *
     * <p>Invoked at job submission time (from the source factory) so an unknown canonical name —
     * e.g. a typo such as {@code rename.tabble} — fails fast during submission with a message
     * listing the valid names, instead of bypassing submission-time option validation and failing
     * later during source initialization.
     */
    public static void validateOptions(ReadonlyConfig config) {
        validateNames(
                SourceOptions.SCHEMA_CHANGES_INCLUDE.key(),
                config.get(SourceOptions.SCHEMA_CHANGES_INCLUDE));
        validateNames(
                SourceOptions.SCHEMA_CHANGES_EXCLUDE.key(),
                config.get(SourceOptions.SCHEMA_CHANGES_EXCLUDE));
    }

    private static void validateNames(String optionKey, List<String> names) {
        try {
            SchemaChangeEventType.fromCanonicalNames(names);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid value for option '" + optionKey + "'. " + e.getMessage(), e);
        }
    }

    public boolean isNoOp() {
        return includeTypes.isEmpty() && excludeTypes.isEmpty();
    }

    /**
     * Applies the filter to a normalized schema change event.
     *
     * @return the original event when fully eligible, a reduced {@link AlterTableColumnsEvent} when
     *     only some of its column sub-events are eligible, or {@code null} when the whole event is
     *     filtered out.
     */
    public SchemaChangeEvent filter(SchemaChangeEvent event) {
        if (event == null || isNoOp()) {
            return event;
        }

        if (event instanceof AlterTableColumnsEvent) {
            AlterTableColumnsEvent composite = (AlterTableColumnsEvent) event;
            List<AlterTableColumnEvent> survivors =
                    composite.getEvents().stream()
                            .filter(sub -> isColumnLevelEligible(sub.getEventType()))
                            .collect(Collectors.toList());
            if (survivors.isEmpty()) {
                return null;
            }
            if (survivors.size() == composite.getEvents().size()) {
                return composite;
            }
            return rebuildComposite(composite, survivors);
        }

        if (event instanceof AlterTableColumnEvent) {
            // A standalone column-level event (not wrapped in a composite).
            return isColumnLevelEligible(event.getEventType()) ? event : null;
        }

        // Table-level events. No canonical name currently maps to a table-level type (rename.table
        // is not exposed yet), so this is defensive: such events are not produced today, but if one
        // arrives it is filtered consistently with the include/exclude precedence.
        return isEligible(event.getEventType()) ? event : null;
    }

    /** Eligibility for table-level event types (no group alias). */
    private boolean isEligible(EventType type) {
        boolean included = includeTypes.isEmpty() || includeTypes.contains(type);
        return included && !excludeTypes.contains(type);
    }

    /**
     * Eligibility for column-level event types, honoring {@code update.columns} as the group alias
     * for the whole column-change family.
     */
    private boolean isColumnLevelEligible(EventType type) {
        boolean included =
                includeTypes.isEmpty()
                        || includeTypes.contains(type)
                        || includeTypes.contains(EventType.SCHEMA_CHANGE_UPDATE_COLUMNS);
        boolean excluded =
                excludeTypes.contains(type)
                        || excludeTypes.contains(EventType.SCHEMA_CHANGE_UPDATE_COLUMNS);
        return included && !excluded;
    }

    private static AlterTableColumnsEvent rebuildComposite(
            AlterTableColumnsEvent original, List<AlterTableColumnEvent> survivors) {
        AlterTableColumnsEvent reduced =
                new AlterTableColumnsEvent(original.tableIdentifier(), survivors);
        reduced.setStatement(original.getStatement());
        reduced.setSourceDialectName(original.getSourceDialectName());
        reduced.setChangeAfter(original.getChangeAfter());
        reduced.setJobId(original.getJobId());
        return reduced;
    }
}
