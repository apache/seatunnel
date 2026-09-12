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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ordered input columns annotated with identities that survive renames.
 *
 * <p>The plain schema is always produced by {@link AlterTableSchemaEventHandler}, so it is exactly
 * what every sink computes when it applies the same events. On top of it, every column carries an
 * identity: a rename keeps the identity of the renamed column, a drop removes it, an add creates a
 * fresh one and a modify keeps it. Identities let the schema-change translator distinguish a column
 * that was renamed and reused ({@code CHANGE a -> b; ADD a}) or dropped and re-created ({@code DROP
 * a; ADD a}) from a column that was merely modified, which name-based comparison cannot do.
 *
 * <p>Instances are immutable; {@link #apply(AlterTableColumnEvent)} validates that the event
 * applies to the current state and returns the next state. Validation errors are reported as {@link
 * IllegalArgumentException} with a user-facing detail message.
 */
final class SQLLineageSchema {

    /** Plain schema after the applied events, as the shared handler produces it. */
    private final TableSchema schema;

    /** Column name to identity, iterated in column order. */
    private final Map<String, Integer> identityByName;

    /** Next identity to assign to an added column. */
    private final int nextIdentity;

    /** The add event that created each identity that did not exist before the first event. */
    private final Map<Integer, AlterTableAddColumnEvent> creatingHints;

    /** Identities that an event explicitly repositioned with FIRST or AFTER. */
    private final Set<Integer> repositionedIdentities;

    private SQLLineageSchema(
            TableSchema schema,
            Map<String, Integer> identityByName,
            int nextIdentity,
            Map<Integer, AlterTableAddColumnEvent> creatingHints,
            Set<Integer> repositionedIdentities) {
        this.schema = schema;
        this.identityByName = Collections.unmodifiableMap(identityByName);
        this.nextIdentity = nextIdentity;
        this.creatingHints = Collections.unmodifiableMap(creatingHints);
        this.repositionedIdentities = Collections.unmodifiableSet(repositionedIdentities);
    }

    /**
     * Creates the initial lineage where every column carries its position as identity.
     *
     * @param schema pre-event input schema
     * @return the initial lineage
     */
    static SQLLineageSchema initial(TableSchema schema) {
        Map<String, Integer> identityByName = new LinkedHashMap<>();
        String[] fieldNames = schema.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            identityByName.put(fieldNames[i], i);
        }
        return new SQLLineageSchema(
                schema, identityByName, fieldNames.length, new HashMap<>(), new HashSet<>());
    }

    /**
     * Applies one column-level event and returns the next lineage state.
     *
     * @param hint the column-level event
     * @return the lineage after the event
     * @throws IllegalArgumentException when the event does not apply to the current schema
     */
    SQLLineageSchema apply(AlterTableColumnEvent hint) {
        Map<String, Integer> next = new LinkedHashMap<>(identityByName);
        Map<Integer, AlterTableAddColumnEvent> creating = new HashMap<>(creatingHints);
        Set<Integer> repositioned = new HashSet<>(repositionedIdentities);
        int identityCounter = nextIdentity;

        if (hint instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent add = (AlterTableAddColumnEvent) hint;
            String name = add.getColumn().getName();
            requireAfterColumn(add.getAfterColumn(), name);
            if (!next.containsKey(name)) {
                next.put(name, identityCounter);
                creating.put(identityCounter, add);
                identityCounter++;
            } else if (add.isFirst() || add.getAfterColumn() != null) {
                // The shared handler treats an add of an existing name as a modify.
                repositioned.add(next.get(name));
            }
        } else if (hint instanceof AlterTableDropColumnEvent) {
            String name = ((AlterTableDropColumnEvent) hint).getColumn();
            requireColumn(name, "drop");
            next.remove(name);
        } else if (hint instanceof AlterTableChangeColumnEvent) {
            AlterTableChangeColumnEvent change = (AlterTableChangeColumnEvent) hint;
            String oldName = change.getOldColumn();
            String newName = change.getColumn().getName();
            requireColumn(oldName, "rename");
            if (!oldName.equals(newName) && next.containsKey(newName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "column [%s] already exists and cannot become the new name of column [%s]",
                                newName, oldName));
            }
            requireAfterColumn(change.getAfterColumn(), oldName);
            Integer identity = next.remove(oldName);
            next.put(newName, identity);
            if (change.isFirst() || change.getAfterColumn() != null) {
                repositioned.add(identity);
            }
        } else if (hint instanceof AlterTableModifyColumnEvent) {
            AlterTableModifyColumnEvent modify = (AlterTableModifyColumnEvent) hint;
            String name = modify.getColumn().getName();
            requireColumn(name, "modify");
            requireAfterColumn(modify.getAfterColumn(), name);
            if (modify.isFirst() || modify.getAfterColumn() != null) {
                repositioned.add(next.get(name));
            }
        } else if (hint instanceof AlterColumnCommentEvent) {
            requireColumn(((AlterColumnCommentEvent) hint).getColumn(), "comment");
        } else {
            throw new IllegalArgumentException(
                    "unsupported column event " + hint.getClass().getName());
        }

        TableSchema nextSchema = new AlterTableSchemaEventHandler().reset(schema).apply(hint);
        // Follow the handler's column order so positions always mirror what sinks compute.
        Map<String, Integer> ordered = new LinkedHashMap<>();
        for (String fieldName : nextSchema.getFieldNames()) {
            Integer identity = next.get(fieldName);
            if (identity == null) {
                throw new IllegalStateException(
                        String.format(
                                "lineage lost track of column [%s] after event %s",
                                fieldName, hint));
            }
            ordered.put(fieldName, identity);
        }
        if (ordered.size() != next.size()) {
            throw new IllegalStateException(
                    String.format(
                            "lineage columns %s do not match handler columns %s after event %s",
                            next.keySet(), ordered.keySet(), hint));
        }
        return new SQLLineageSchema(nextSchema, ordered, identityCounter, creating, repositioned);
    }

    /** Plain schema of this state. */
    TableSchema getSchema() {
        return schema;
    }

    /**
     * Identity of the column with the given name, or -1 when the name is absent.
     *
     * @param columnName column name in this state
     * @return identity or -1
     */
    int identityOf(String columnName) {
        Integer identity = identityByName.get(columnName);
        return identity == null ? -1 : identity;
    }

    /** Column identities in column order. */
    List<Integer> identities() {
        return new ArrayList<>(identityByName.values());
    }

    /**
     * The add event that created the identity, or null when the column existed before the first
     * event.
     */
    AlterTableAddColumnEvent creatingHint(int identity) {
        return creatingHints.get(identity);
    }

    /** Whether an event explicitly repositioned the identity with FIRST or AFTER. */
    boolean isRepositioned(int identity) {
        return repositionedIdentities.contains(identity);
    }

    private void requireColumn(String name, String action) {
        if (!identityByName.containsKey(name)) {
            throw new IllegalArgumentException(
                    String.format(
                            "column [%s] to %s does not exist in %s",
                            name, action, identityByName.keySet()));
        }
    }

    private void requireAfterColumn(String afterColumn, String self) {
        if (afterColumn == null) {
            return;
        }
        if (!identityByName.containsKey(afterColumn) || afterColumn.equals(self)) {
            throw new IllegalArgumentException(
                    String.format(
                            "AFTER column [%s] is not a valid anchor for column [%s] in %s",
                            afterColumn, self, identityByName.keySet()));
        }
    }
}
