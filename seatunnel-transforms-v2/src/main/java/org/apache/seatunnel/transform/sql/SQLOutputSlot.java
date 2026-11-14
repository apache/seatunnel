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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Describes one output column of a SQL query in produced-column order: how the column is derived
 * from the input (star expansion, direct column reference, expression or lateral view alias) and
 * which input columns it depends on.
 *
 * <p>Schema-change translation binds these descriptors to input column identities to decide whether
 * an output column was renamed, replaced by a different physical column, modified or left untouched
 * by an upstream DDL. The descriptor itself carries no types; the produced {@link
 * org.apache.seatunnel.api.table.catalog.Column} at the same index does.
 */
public final class SQLOutputSlot implements Serializable {

    private static final long serialVersionUID = 1L;

    /** How the output column is derived from the input. */
    public enum Kind {
        /**
         * One column produced by a {@code select *} expansion; bound to exactly one input column.
         */
        STAR,
        /** A direct reference such as {@code select c} or {@code select c AS d}. */
        REFERENCE,
        /** Any other select item expression, bound to every input column it references. */
        EXPRESSION,
        /** A lateral view alias appended to the output, bound to the generator arguments. */
        LATERAL_VIEW
    }

    /** Derivation kind of this output column. */
    private final Kind kind;

    /** Index of the select item that produced this slot; -1 for appended lateral view aliases. */
    private final int selectItemIndex;

    /** Produced column name after escape cleanup. */
    private final String name;

    /** Input column names this slot depends on, in the order they were resolved. */
    private final List<String> referencedInputColumns;

    private SQLOutputSlot(
            Kind kind, int selectItemIndex, String name, List<String> referencedInputColumns) {
        this.kind = kind;
        this.selectItemIndex = selectItemIndex;
        this.name = name;
        this.referencedInputColumns =
                Collections.unmodifiableList(Objects.requireNonNull(referencedInputColumns));
    }

    /**
     * Creates a slot for one column of a star expansion.
     *
     * @param selectItemIndex index of the {@code *} select item
     * @param name produced column name
     * @param inputColumn the input column the slot copies
     * @return the slot
     */
    public static SQLOutputSlot star(int selectItemIndex, String name, String inputColumn) {
        return new SQLOutputSlot(
                Kind.STAR, selectItemIndex, name, Collections.singletonList(inputColumn));
    }

    /**
     * Creates a slot for a direct column reference, with or without alias.
     *
     * @param selectItemIndex index of the select item
     * @param name produced column name (the alias, or the column name)
     * @param inputColumn the referenced input column
     * @return the slot
     */
    public static SQLOutputSlot reference(int selectItemIndex, String name, String inputColumn) {
        return new SQLOutputSlot(
                Kind.REFERENCE, selectItemIndex, name, Collections.singletonList(inputColumn));
    }

    /**
     * Creates a slot for an arbitrary expression.
     *
     * @param selectItemIndex index of the select item
     * @param name produced column name
     * @param referencedInputColumns every input column the expression references
     * @return the slot
     */
    public static SQLOutputSlot expression(
            int selectItemIndex, String name, List<String> referencedInputColumns) {
        return new SQLOutputSlot(Kind.EXPRESSION, selectItemIndex, name, referencedInputColumns);
    }

    /**
     * Creates a slot for a lateral view alias that is appended to the output.
     *
     * @param alias produced column name
     * @param referencedInputColumns every input column the generator arguments reference
     * @return the slot
     */
    public static SQLOutputSlot lateralView(String alias, List<String> referencedInputColumns) {
        return new SQLOutputSlot(Kind.LATERAL_VIEW, -1, alias, referencedInputColumns);
    }

    public Kind getKind() {
        return kind;
    }

    public int getSelectItemIndex() {
        return selectItemIndex;
    }

    public String getName() {
        return name;
    }

    public List<String> getReferencedInputColumns() {
        return referencedInputColumns;
    }

    /**
     * Stable key that pairs a non-star slot before and after a schema change. Star slots are paired
     * by input column identity instead, because their names follow the input.
     *
     * @return pairing key
     */
    public String pairingKey() {
        return kind + ":" + selectItemIndex + ":" + name;
    }

    @Override
    public String toString() {
        return "SQLOutputSlot{"
                + "kind="
                + kind
                + ", selectItemIndex="
                + selectItemIndex
                + ", name='"
                + name
                + '\''
                + ", referencedInputColumns="
                + referencedInputColumns
                + '}';
    }
}
