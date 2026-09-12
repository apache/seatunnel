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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.AlterTableSchemaEventHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Translates upstream column-level DDL into events that describe the change of the SQL transform's
 * own output.
 *
 * <p>Sinks advance the produced schema they received at planning time by applying the events they
 * receive, so the emitted events must satisfy one contract: replaying them onto the pre-event
 * produced schema with {@link AlterTableSchemaEventHandler} yields a schema equal to the produced
 * schema after the event, and every output column keeps its data lineage. Attribution therefore
 * works on input column identities (see {@link SQLLineageSchema}) and on output slots (see {@link
 * SQLOutputSlot}), never on names alone.
 *
 * <p>All methods are static and stateless. Validation problems that a user can act on are reported
 * as {@link IllegalArgumentException}; violations of internal invariants as {@link
 * IllegalStateException}.
 */
final class SQLSchemaChangeTranslator {

    private SQLSchemaChangeTranslator() {}

    /**
     * Column-level sub-events of a single or composite event; empty for table-level events.
     *
     * @param event any schema change event
     * @return the ordered column-level events
     */
    static List<AlterTableColumnEvent> flatten(SchemaChangeEvent event) {
        if (event instanceof AlterTableColumnsEvent) {
            return new ArrayList<>(((AlterTableColumnsEvent) event).getEvents());
        }
        if (event instanceof AlterTableColumnEvent) {
            return Collections.singletonList((AlterTableColumnEvent) event);
        }
        return Collections.emptyList();
    }

    /**
     * Identities of the input columns that back the produced primary key, the produced constraint
     * keys and the input partition keys. The shared handler never renames or removes a key column,
     * so dropping or renaming one of these columns cannot be represented by any event.
     *
     * @param preSlots output slots before the event
     * @param preOutput produced schema before the event
     * @param partitionKeys input partition keys, may be null
     * @param initial initial lineage of the input
     * @return protected identities
     */
    static Set<Integer> protectedIdentities(
            List<SQLOutputSlot> preSlots,
            TableSchema preOutput,
            List<String> partitionKeys,
            SQLLineageSchema initial) {
        Set<String> protectedOutputNames = new HashSet<>();
        if (preOutput.getPrimaryKey() != null) {
            protectedOutputNames.addAll(preOutput.getPrimaryKey().getColumnNames());
        }
        if (preOutput.getConstraintKeys() != null) {
            for (ConstraintKey constraintKey : preOutput.getConstraintKeys()) {
                for (ConstraintKey.ConstraintKeyColumn keyColumn : constraintKey.getColumnNames()) {
                    protectedOutputNames.add(keyColumn.getColumnName());
                }
            }
        }
        Set<Integer> identities = new HashSet<>();
        for (SQLOutputSlot slot : preSlots) {
            if (!protectedOutputNames.contains(slot.getName())) {
                continue;
            }
            for (String referenced : slot.getReferencedInputColumns()) {
                int identity = initial.identityOf(referenced);
                if (identity >= 0) {
                    identities.add(identity);
                }
            }
        }
        if (partitionKeys != null) {
            for (String partitionKey : partitionKeys) {
                int identity = initial.identityOf(partitionKey);
                if (identity >= 0) {
                    identities.add(identity);
                }
            }
        }
        return identities;
    }

    /**
     * Rejects a drop or rename of a protected column.
     *
     * @param lineage lineage before the hint is applied
     * @param hint the column-level event about to be applied
     * @param protectedIdentities identities that must keep their name and existence
     * @throws IllegalArgumentException when the hint drops or renames a protected column
     */
    static void rejectProtectedColumnChange(
            SQLLineageSchema lineage,
            AlterTableColumnEvent hint,
            Set<Integer> protectedIdentities) {
        String target = null;
        if (hint instanceof AlterTableDropColumnEvent) {
            target = ((AlterTableDropColumnEvent) hint).getColumn();
        } else if (hint instanceof AlterTableChangeColumnEvent) {
            AlterTableChangeColumnEvent change = (AlterTableChangeColumnEvent) hint;
            if (!change.getOldColumn().equals(change.getColumn().getName())) {
                target = change.getOldColumn();
            }
        }
        if (target != null && protectedIdentities.contains(lineage.identityOf(target))) {
            throw new IllegalArgumentException(
                    String.format(
                            "column [%s] is part of the primary key, a constraint key or the partition keys and cannot be dropped or renamed",
                            target));
        }
    }

    /**
     * Computes the net effect of the change on every output slot and returns the events that
     * reproduce it, ordered so that replaying them never collides on a column name.
     *
     * @param tableId produced table identifier used on the emitted events
     * @param preSlots output slots before the event
     * @param preOutput produced schema before the event
     * @param preLineage initial lineage of the pre-event input
     * @param finalSlots output slots after the event
     * @param finalOutput produced schema after the event
     * @param finalLineage lineage after all hints were applied
     * @return ordered events, empty when the output did not change
     * @throws IllegalArgumentException when renames form a cycle that no event order can replay
     */
    static List<AlterTableColumnEvent> translate(
            TableIdentifier tableId,
            List<SQLOutputSlot> preSlots,
            TableSchema preOutput,
            SQLLineageSchema preLineage,
            List<SQLOutputSlot> finalSlots,
            TableSchema finalOutput,
            SQLLineageSchema finalLineage) {
        List<BoundSlot> pre = bind(preSlots, preOutput, preLineage);
        List<BoundSlot> fin = bind(finalSlots, finalOutput, finalLineage);

        Map<Integer, BoundSlot> preStar = new LinkedHashMap<>();
        Map<Integer, BoundSlot> finalStar = new LinkedHashMap<>();
        Map<String, BoundSlot> preOther = new LinkedHashMap<>();
        Map<String, BoundSlot> finalOther = new LinkedHashMap<>();
        for (BoundSlot bound : pre) {
            if (bound.isStar()) {
                preStar.put(bound.identity(), bound);
            } else {
                preOther.put(bound.slot.pairingKey(), bound);
            }
        }
        for (BoundSlot bound : fin) {
            if (bound.isStar()) {
                finalStar.put(bound.identity(), bound);
            } else {
                finalOther.put(bound.slot.pairingKey(), bound);
            }
        }

        List<AlterTableDropColumnEvent> drops = new ArrayList<>();
        List<AlterTableChangeColumnEvent> changes = new ArrayList<>();
        // Adds and modifies are keyed by final output index so AFTER anchors always exist.
        TreeMap<Integer, AlterTableColumnEvent> adds = new TreeMap<>();
        TreeMap<Integer, AlterTableColumnEvent> modifies = new TreeMap<>();

        for (BoundSlot preBound : preStar.values()) {
            if (!finalStar.containsKey(preBound.identity())) {
                drops.add(new AlterTableDropColumnEvent(tableId, preBound.column.getName()));
            }
        }
        for (BoundSlot finalBound : finalStar.values()) {
            BoundSlot preBound = preStar.get(finalBound.identity());
            if (preBound == null) {
                Position position =
                        addPosition(
                                finalBound,
                                fin,
                                finalLineage.creatingHint(finalBound.identity()),
                                finalLineage);
                adds.put(
                        finalBound.index,
                        new AlterTableAddColumnEvent(
                                tableId, finalBound.column, position.first, position.afterColumn));
                continue;
            }
            Position moved = movedPosition(preBound, finalBound, pre, fin, finalLineage);
            if (!preBound.column.getName().equals(finalBound.column.getName())) {
                changes.add(
                        new AlterTableChangeColumnEvent(
                                tableId,
                                preBound.column.getName(),
                                finalBound.column,
                                moved.first,
                                moved.afterColumn));
            } else if (!preBound.column.equals(finalBound.column) || moved.isSet()) {
                modifies.put(
                        finalBound.index,
                        modifyOrComment(tableId, preBound.column, finalBound.column, moved));
            }
        }

        for (Map.Entry<String, BoundSlot> entry : preOther.entrySet()) {
            BoundSlot preBound = entry.getValue();
            BoundSlot finalBound = finalOther.get(entry.getKey());
            if (finalBound == null) {
                throw new IllegalStateException(
                        String.format(
                                "output column [%s] disappeared from the query output",
                                preBound.column.getName()));
            }
            if (!preBound.signature.equals(finalBound.signature)) {
                // The output column now carries data of a different physical input column.
                drops.add(new AlterTableDropColumnEvent(tableId, preBound.column.getName()));
                Position position = addPosition(finalBound, fin, null, finalLineage);
                adds.put(
                        finalBound.index,
                        new AlterTableAddColumnEvent(
                                tableId, finalBound.column, position.first, position.afterColumn));
            } else if (!preBound.column.equals(finalBound.column)) {
                modifies.put(
                        finalBound.index,
                        modifyOrComment(
                                tableId, preBound.column, finalBound.column, Position.none()));
            }
        }
        for (String key : finalOther.keySet()) {
            if (!preOther.containsKey(key)) {
                throw new IllegalStateException(
                        String.format("output column [%s] appeared without a pre-event slot", key));
            }
        }

        List<AlterTableColumnEvent> out = new ArrayList<>(drops);
        out.addAll(orderChanges(changes, preOutput, drops));
        out.addAll(adds.values());
        out.addAll(modifies.values());
        return out;
    }

    /**
     * Replays the emitted events onto the pre-event produced schema and checks full equality with
     * the produced schema after the event.
     *
     * @param preOutput produced schema before the event
     * @param produced produced schema after the event
     * @param out the events about to be emitted
     * @throws IllegalStateException when a replay step would collide on a name or the result
     *     differs from the produced schema
     */
    static void verifyReplay(
            TableSchema preOutput, TableSchema produced, List<AlterTableColumnEvent> out) {
        TableSchema replayed = preOutput;
        Set<String> names = new HashSet<>(Arrays.asList(preOutput.getFieldNames()));
        for (AlterTableColumnEvent event : out) {
            if (event instanceof AlterTableAddColumnEvent) {
                String name = ((AlterTableAddColumnEvent) event).getColumn().getName();
                if (!names.add(name)) {
                    throw new IllegalStateException("replay would add an existing column " + name);
                }
            } else if (event instanceof AlterTableDropColumnEvent) {
                String name = ((AlterTableDropColumnEvent) event).getColumn();
                if (!names.remove(name)) {
                    throw new IllegalStateException("replay would drop a missing column " + name);
                }
            } else if (event instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent change = (AlterTableChangeColumnEvent) event;
                String newName = change.getColumn().getName();
                if (!names.remove(change.getOldColumn())) {
                    throw new IllegalStateException(
                            "replay would rename a missing column " + change.getOldColumn());
                }
                if (!names.add(newName)) {
                    throw new IllegalStateException(
                            "replay would rename onto an existing column " + newName);
                }
            } else if (event instanceof AlterTableModifyColumnEvent) {
                String name = ((AlterTableModifyColumnEvent) event).getColumn().getName();
                if (!names.contains(name)) {
                    throw new IllegalStateException("replay would modify a missing column " + name);
                }
            } else if (event instanceof AlterColumnCommentEvent) {
                String name = ((AlterColumnCommentEvent) event).getColumn();
                if (!names.contains(name)) {
                    throw new IllegalStateException(
                            "replay would comment a missing column " + name);
                }
            }
            replayed = new AlterTableSchemaEventHandler().reset(replayed).apply(event);
        }
        if (!replayed.equals(produced)) {
            throw new IllegalStateException(
                    String.format(
                            "replayed schema %s does not match the produced schema %s for events %s",
                            replayed, produced, out));
        }
    }

    /**
     * Builds the outgoing event with the shape of the incoming one and copies the metadata sinks
     * rely on.
     *
     * <p>A composite in yields a composite out. A single column event in yields the single
     * translated event when exactly one results and a composite otherwise. {@code jobId}, {@code
     * statement} and {@code changeAfter} are copied to every event; {@code sourceDialectName} is
     * copied to an add, modify or change only when its column carries a {@code sourceType}, because
     * JDBC dialects emit that type verbatim for the same dialect and a derived column has none.
     *
     * @param incoming the upstream event
     * @param tableId produced table identifier
     * @param out translated events, never empty
     * @param produced produced table after the event
     * @return the event to forward downstream
     */
    static SchemaChangeEvent rebuild(
            AlterTableEvent incoming,
            TableIdentifier tableId,
            List<AlterTableColumnEvent> out,
            CatalogTable produced) {
        for (AlterTableColumnEvent event : out) {
            event.setJobId(incoming.getJobId());
            event.setStatement(incoming.getStatement());
            event.setChangeAfter(produced);
            if (carriesSourceDialect(event)) {
                event.setSourceDialectName(incoming.getSourceDialectName());
            }
        }
        if (!(incoming instanceof AlterTableColumnsEvent) && out.size() == 1) {
            return out.get(0);
        }
        AlterTableColumnsEvent composite =
                new AlterTableColumnsEvent(tableId, new ArrayList<>(out));
        composite.setJobId(incoming.getJobId());
        composite.setStatement(incoming.getStatement());
        composite.setSourceDialectName(incoming.getSourceDialectName());
        composite.setChangeAfter(produced);
        return composite;
    }

    private static boolean carriesSourceDialect(AlterTableColumnEvent event) {
        Column column = null;
        if (event instanceof AlterTableAddColumnEvent) {
            column = ((AlterTableAddColumnEvent) event).getColumn();
        } else if (event instanceof AlterTableModifyColumnEvent) {
            column = ((AlterTableModifyColumnEvent) event).getColumn();
        } else if (event instanceof AlterTableChangeColumnEvent) {
            column = ((AlterTableChangeColumnEvent) event).getColumn();
        }
        if (column == null) {
            return true;
        }
        return column.getSourceType() != null && !column.getSourceType().isEmpty();
    }

    private static AlterTableColumnEvent modifyOrComment(
            TableIdentifier tableId, Column before, Column after, Position moved) {
        if (!moved.isSet() && isCommentOnly(before, after)) {
            return AlterColumnCommentEvent.of(
                    tableId, after.getName(), before.getComment(), after.getComment());
        }
        return new AlterTableModifyColumnEvent(tableId, after, moved.first, moved.afterColumn);
    }

    private static boolean isCommentOnly(Column before, Column after) {
        try {
            return before.copyWithComment(after.getComment()).equals(after);
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private static List<AlterTableColumnEvent> orderChanges(
            List<AlterTableChangeColumnEvent> changes,
            TableSchema preOutput,
            List<AlterTableDropColumnEvent> drops) {
        Set<String> occupied = new HashSet<>(Arrays.asList(preOutput.getFieldNames()));
        for (AlterTableDropColumnEvent drop : drops) {
            occupied.remove(drop.getColumn());
        }
        List<AlterTableChangeColumnEvent> remaining = new ArrayList<>(changes);
        List<AlterTableColumnEvent> ordered = new ArrayList<>();
        while (!remaining.isEmpty()) {
            AlterTableChangeColumnEvent next = null;
            for (AlterTableChangeColumnEvent candidate : remaining) {
                if (!occupied.contains(candidate.getColumn().getName())) {
                    next = candidate;
                    break;
                }
            }
            if (next == null) {
                throw new IllegalArgumentException(
                        "column renames form a cycle: "
                                + remaining.stream()
                                        .map(
                                                change ->
                                                        change.getOldColumn()
                                                                + " -> "
                                                                + change.getColumn().getName())
                                        .collect(Collectors.toList()));
            }
            remaining.remove(next);
            ordered.add(next);
            occupied.remove(next.getOldColumn());
            occupied.add(next.getColumn().getName());
        }
        return ordered;
    }

    private static List<BoundSlot> bind(
            List<SQLOutputSlot> slots, TableSchema output, SQLLineageSchema lineage) {
        List<Column> columns = output.getColumns();
        if (slots.size() != columns.size()) {
            throw new IllegalStateException(
                    String.format(
                            "%d output slots do not match %d produced columns",
                            slots.size(), columns.size()));
        }
        List<BoundSlot> bound = new ArrayList<>(slots.size());
        for (int i = 0; i < slots.size(); i++) {
            SQLOutputSlot slot = slots.get(i);
            List<Integer> signature = new ArrayList<>();
            for (String referenced : slot.getReferencedInputColumns()) {
                int identity = lineage.identityOf(referenced);
                if (identity < 0) {
                    throw new IllegalStateException(
                            String.format(
                                    "output column [%s] references unknown input column [%s]",
                                    slot.getName(), referenced));
                }
                signature.add(identity);
            }
            Collections.sort(signature);
            bound.add(new BoundSlot(slot, columns.get(i), i, signature));
        }
        return bound;
    }

    private static Position addPosition(
            BoundSlot added,
            List<BoundSlot> fin,
            AlterTableAddColumnEvent creatingHint,
            SQLLineageSchema finalLineage) {
        int index = added.index;
        if (index == 0) {
            return Position.first();
        }
        String predecessor = fin.get(index - 1).column.getName();
        if (index == fin.size() - 1) {
            if (creatingHint != null
                    && creatingHint.getAfterColumn() != null
                    && creatingHint.getAfterColumn().equals(predecessor)) {
                return Position.after(predecessor);
            }
            return Position.none();
        }
        boolean appendedUpstream =
                creatingHint != null
                        && !creatingHint.isFirst()
                        && creatingHint.getAfterColumn() == null;
        if (appendedUpstream && onlyAppendedStarSlotsFollow(fin, index, finalLineage)) {
            // The upstream appended this column and every column after it in the output was
            // appended by the same event, so a plain append reproduces the layout exactly.
            return Position.none();
        }
        return Position.after(predecessor);
    }

    private static boolean onlyAppendedStarSlotsFollow(
            List<BoundSlot> fin, int index, SQLLineageSchema finalLineage) {
        for (int i = index + 1; i < fin.size(); i++) {
            BoundSlot following = fin.get(i);
            if (!following.isStar()) {
                return false;
            }
            AlterTableAddColumnEvent hint = finalLineage.creatingHint(following.identity());
            if (hint == null || hint.isFirst() || hint.getAfterColumn() != null) {
                return false;
            }
        }
        return true;
    }

    private static Position movedPosition(
            BoundSlot preBound,
            BoundSlot finalBound,
            List<BoundSlot> pre,
            List<BoundSlot> fin,
            SQLLineageSchema finalLineage) {
        if (!finalLineage.isRepositioned(finalBound.identity())) {
            return Position.none();
        }
        if (starPredecessorIdentity(pre, preBound.index)
                == starPredecessorIdentity(fin, finalBound.index)) {
            return Position.none();
        }
        if (finalBound.index == 0) {
            return Position.first();
        }
        return Position.after(fin.get(finalBound.index - 1).column.getName());
    }

    private static int starPredecessorIdentity(List<BoundSlot> bound, int index) {
        for (int i = index - 1; i >= 0; i--) {
            if (bound.get(i).isStar()) {
                return bound.get(i).identity();
            }
        }
        return -1;
    }

    /** One output slot bound to the identities of the input columns it depends on. */
    private static final class BoundSlot {
        private final SQLOutputSlot slot;
        private final Column column;
        private final int index;
        private final List<Integer> signature;

        private BoundSlot(SQLOutputSlot slot, Column column, int index, List<Integer> signature) {
            this.slot = slot;
            this.column = column;
            this.index = index;
            this.signature = signature;
        }

        private boolean isStar() {
            return slot.getKind() == SQLOutputSlot.Kind.STAR;
        }

        private int identity() {
            return signature.get(0);
        }
    }

    /** Position flags of an emitted event, expressed in output column names. */
    private static final class Position {
        private final boolean first;
        private final String afterColumn;

        private Position(boolean first, String afterColumn) {
            this.first = first;
            this.afterColumn = afterColumn;
        }

        private static Position none() {
            return new Position(false, null);
        }

        private static Position first() {
            return new Position(true, null);
        }

        private static Position after(String afterColumn) {
            return new Position(false, afterColumn);
        }

        private boolean isSet() {
            return first || afterColumn != null;
        }
    }
}
