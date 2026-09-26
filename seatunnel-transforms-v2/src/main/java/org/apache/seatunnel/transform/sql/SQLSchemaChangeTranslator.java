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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * <p>Events are emitted in an order that is valid step by step for a replaying sink: drops first,
 * then the final layout from left to right while the physical column order of the sink is simulated
 * (see {@link ReplayLayout}). A composite upstream event may add a column and anchor a rename or a
 * move on it, or rename a column and reuse its old name for an add at a lower index; both orders of
 * dependency are honoured because anchors and name freedom are decided against the simulated state
 * instead of against a fixed event-kind order.
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
     * reproduce it, in an order that replays onto the pre-event produced schema step by step.
     *
     * <p>Every final output column is paired with the pre-event column it continues: star columns
     * by input identity, other columns by slot key as long as the physical input columns behind
     * them did not change. Pre-event columns without a partner are dropped first. The final layout
     * is then walked from left to right on a simulated replay of the sink: a new column is added, a
     * continued column is renamed when its name changed, moved when it is out of place and modified
     * when its definition changed. The AFTER anchor of every positioned event is the final column
     * to its left, which the walk has already placed under its final name, and a name that an add
     * or a rename needs is freed just before by renaming its current holder in place. Renames that
     * block each other form a cycle no event order can replay and are rejected.
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
        BoundSlot[] partners = pair(pre, fin);

        boolean[] continued = new boolean[pre.size()];
        for (BoundSlot partner : partners) {
            if (partner != null) {
                continued[partner.index] = true;
            }
        }
        List<AlterTableColumnEvent> out = new ArrayList<>();
        // Drops depend on nothing and free their names for everything that follows.
        for (BoundSlot preBound : pre) {
            if (!continued[preBound.index]) {
                out.add(new AlterTableDropColumnEvent(tableId, preBound.column.getName()));
            }
        }
        ReplayLayout layout = new ReplayLayout(tableId, pre, fin, partners, finalLineage);
        for (int index = 0; index < fin.size(); index++) {
            layout.place(index, out);
        }
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

    /**
     * Pairs every final output column with the pre-event column it continues, or null when the
     * column is new to the output. Star columns pair by input identity. Other columns pair by slot
     * key and count as new when the physical input columns behind them changed, because the output
     * column then carries different data and has to be dropped and re-added.
     */
    private static BoundSlot[] pair(List<BoundSlot> pre, List<BoundSlot> fin) {
        Map<Integer, BoundSlot> preStar = new HashMap<>();
        Map<String, BoundSlot> preOther = new HashMap<>();
        for (BoundSlot preBound : pre) {
            if (preBound.isStar()) {
                preStar.put(preBound.identity(), preBound);
            } else {
                preOther.put(preBound.slot.pairingKey(), preBound);
            }
        }
        BoundSlot[] partners = new BoundSlot[fin.size()];
        Set<String> continuedKeys = new HashSet<>();
        for (BoundSlot finalBound : fin) {
            if (finalBound.isStar()) {
                partners[finalBound.index] = preStar.get(finalBound.identity());
                continue;
            }
            String key = finalBound.slot.pairingKey();
            BoundSlot preBound = preOther.get(key);
            if (preBound == null) {
                throw new IllegalStateException(
                        String.format("output column [%s] appeared without a pre-event slot", key));
            }
            continuedKeys.add(key);
            partners[finalBound.index] =
                    preBound.signature.equals(finalBound.signature) ? preBound : null;
        }
        for (BoundSlot preBound : pre) {
            if (!preBound.isStar() && !continuedKeys.contains(preBound.slot.pairingKey())) {
                throw new IllegalStateException(
                        String.format(
                                "output column [%s] disappeared from the query output",
                                preBound.column.getName()));
            }
        }
        return partners;
    }

    private static AlterTableColumnEvent modifyOrComment(
            TableIdentifier tableId, Column before, Column after) {
        if (isCommentOnly(before, after)) {
            return AlterColumnCommentEvent.of(
                    tableId, after.getName(), before.getComment(), after.getComment());
        }
        return new AlterTableModifyColumnEvent(tableId, after, false, null);
    }

    private static boolean isCommentOnly(Column before, Column after) {
        try {
            return before.copyWithComment(after.getComment()).equals(after);
        } catch (UnsupportedOperationException e) {
            return false;
        }
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

    /**
     * Physical column order of a sink that replays the emitted events, tracked by final output
     * index.
     *
     * <p>Continued columns start in their pre-event order under their pre-event names. Every
     * emitted event is applied to this order exactly as {@link AlterTableSchemaEventHandler}
     * applies it, so the AFTER anchor of an event and the freedom of the name it needs are decided
     * against the state the sink is in when it receives that event. Walking the final layout from
     * left to right keeps every placed column in its final relative order, which makes the column
     * to the left the anchor of any positioned event.
     */
    private static final class ReplayLayout {

        private final TableIdentifier tableId;

        /** Final output slots in produced order. */
        private final List<BoundSlot> fin;

        /** Pre-event slot continued by each final slot, null for a column new to the output. */
        private final BoundSlot[] partners;

        /** Lineage after all hints, for the hints that created new columns. */
        private final SQLLineageSchema finalLineage;

        /** Final indices of the columns currently present, in physical order. */
        private final List<Integer> physical = new ArrayList<>();

        /** Current name of every final column; null while the column is not present yet. */
        private final String[] currentName;

        /** Final columns already walked; they sit in their final relative order. */
        private final boolean[] placed;

        /** Continued columns already renamed by a CHANGE that carried the final definition. */
        private final boolean[] renamed;

        /**
         * Continued columns the upstream explicitly repositioned. While such a column is still
         * waiting it does not pin the columns physically after it, because it is moved on its own
         * turn if it is out of place.
         */
        private final boolean[] repositioned;

        private ReplayLayout(
                TableIdentifier tableId,
                List<BoundSlot> pre,
                List<BoundSlot> fin,
                BoundSlot[] partners,
                SQLLineageSchema finalLineage) {
            this.tableId = tableId;
            this.fin = fin;
            this.partners = partners;
            this.finalLineage = finalLineage;
            this.currentName = new String[fin.size()];
            this.placed = new boolean[fin.size()];
            this.renamed = new boolean[fin.size()];
            this.repositioned = new boolean[fin.size()];
            int[] finalIndexOfPre = new int[pre.size()];
            Arrays.fill(finalIndexOfPre, -1);
            for (int index = 0; index < partners.length; index++) {
                BoundSlot partner = partners[index];
                if (partner == null) {
                    continue;
                }
                finalIndexOfPre[partner.index] = index;
                currentName[index] = partner.column.getName();
                BoundSlot target = fin.get(index);
                repositioned[index] =
                        target.isStar() && finalLineage.isRepositioned(target.identity());
            }
            for (BoundSlot preBound : pre) {
                if (finalIndexOfPre[preBound.index] >= 0) {
                    physical.add(finalIndexOfPre[preBound.index]);
                }
            }
        }

        /**
         * Emits the events that give the column at {@code index} its final name, position and
         * definition, and records their effect on the simulated order.
         *
         * @param index final output index to place
         * @param out event list to append to
         */
        private void place(int index, List<AlterTableColumnEvent> out) {
            BoundSlot target = fin.get(index);
            BoundSlot partner = partners[index];
            String finalName = target.column.getName();
            if (partner == null) {
                freeName(index, finalName, new ArrayDeque<>(), out);
                Position position = addPosition(index);
                out.add(
                        new AlterTableAddColumnEvent(
                                tableId, target.column, position.first, position.afterColumn));
                insert(index, position);
                placed[index] = true;
                return;
            }
            boolean rename = !finalName.equals(currentName[index]);
            if (rename) {
                Deque<Integer> chain = new ArrayDeque<>();
                chain.addLast(index);
                freeName(index, finalName, chain, out);
            }
            Position position;
            if (inPlace(index)) {
                position = Position.none();
            } else if (index == 0) {
                position = Position.first();
            } else {
                position = Position.after(fin.get(index - 1).column.getName());
            }
            if (rename) {
                out.add(
                        new AlterTableChangeColumnEvent(
                                tableId,
                                currentName[index],
                                target.column,
                                position.first,
                                position.afterColumn));
                currentName[index] = finalName;
                renamed[index] = true;
            } else if (position.isSet()) {
                out.add(
                        new AlterTableModifyColumnEvent(
                                tableId, target.column, position.first, position.afterColumn));
            } else if (!renamed[index] && !partner.column.equals(target.column)) {
                // A CHANGE emitted earlier for this column already carried its final definition.
                out.add(modifyOrComment(tableId, partner.column, target.column));
            }
            if (position.isSet()) {
                physical.remove(Integer.valueOf(index));
                insert(index, position);
            }
            placed[index] = true;
        }

        /**
         * Frees {@code name} for the column at {@code requester} by renaming, in place, the waiting
         * column that still holds it, after freeing that column's own final name the same way.
         *
         * @param requester final index of the column that needs the name
         * @param name the name needed
         * @param chain columns whose renames are pending on this path, for cycle detection
         * @param out event list to append to
         * @throws IllegalArgumentException when the renames form a cycle
         */
        private void freeName(
                int requester, String name, Deque<Integer> chain, List<AlterTableColumnEvent> out) {
            int holder = holderOf(name, requester);
            if (holder < 0) {
                return;
            }
            if (chain.contains(holder)) {
                chain.addLast(holder);
                throw new IllegalArgumentException(
                        "column renames form a cycle: " + describeRenames(chain));
            }
            chain.addLast(holder);
            Column column = fin.get(holder).column;
            freeName(holder, column.getName(), chain, out);
            chain.removeLast();
            out.add(
                    new AlterTableChangeColumnEvent(
                            tableId, currentName[holder], column, false, null));
            currentName[holder] = column.getName();
            renamed[holder] = true;
        }

        /**
         * Final index of the waiting column other than {@code requester} that currently carries
         * {@code name}, or -1. Placed columns carry their final names, which are unique, so only a
         * waiting column can hold a name another column needs.
         */
        private int holderOf(String name, int requester) {
            for (int index : physical) {
                if (index != requester && !placed[index] && name.equals(currentName[index])) {
                    return index;
                }
            }
            return -1;
        }

        private String describeRenames(Deque<Integer> chain) {
            List<String> steps = new ArrayList<>();
            for (int index : chain) {
                steps.add(currentName[index] + " -> " + fin.get(index).column.getName());
            }
            return steps.toString();
        }

        /**
         * Whether the column already sits where the final layout needs it: after every placed
         * column and before every waiting column that will not move on its own turn.
         */
        private boolean inPlace(int index) {
            int position = physical.indexOf(index);
            for (int k = 0; k < physical.size(); k++) {
                int other = physical.get(k);
                if (other == index) {
                    continue;
                }
                if (k < position) {
                    if (!placed[other] && !repositioned[other]) {
                        return false;
                    }
                } else if (placed[other]) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Position of a new column. Mirrors the upstream hint where that reproduces the layout:
         * FIRST at index 0, a plain append when the upstream appended the column and nothing that
         * would end up after it is still waiting, AFTER the final column to its left otherwise.
         */
        private Position addPosition(int index) {
            if (index == 0) {
                return Position.first();
            }
            BoundSlot target = fin.get(index);
            String predecessor = fin.get(index - 1).column.getName();
            AlterTableAddColumnEvent hint =
                    target.isStar() ? finalLineage.creatingHint(target.identity()) : null;
            boolean waiting = false;
            for (int present : physical) {
                if (!placed[present]) {
                    waiting = true;
                    break;
                }
            }
            if (index == fin.size() - 1) {
                if (hint != null && predecessor.equals(hint.getAfterColumn())) {
                    return Position.after(predecessor);
                }
                return waiting ? Position.after(predecessor) : Position.none();
            }
            boolean appendedUpstream =
                    hint != null && !hint.isFirst() && hint.getAfterColumn() == null;
            if (!waiting && appendedUpstream && onlyAppendedStarSlotsFollow(index)) {
                // The upstream appended this column and every column after it in the output was
                // appended by the same event, so a plain append reproduces the layout exactly.
                return Position.none();
            }
            return Position.after(predecessor);
        }

        private boolean onlyAppendedStarSlotsFollow(int index) {
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

        /** Inserts a column the way the shared handler does for the given position flags. */
        private void insert(int index, Position position) {
            if (position.first) {
                physical.add(0, index);
            } else if (position.afterColumn != null) {
                physical.add(physical.indexOf(index - 1) + 1, index);
            } else {
                physical.add(index);
            }
        }
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
