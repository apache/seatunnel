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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.factory.SupportMultipleTable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * SeaTunnel row type.
 */
public final class SeaTunnelRow implements Serializable {
    private static final long serialVersionUID = -1L;
    /** Table identifier, used for the source connector that {@link SupportMultipleTable}. */
    private int tableId = -1;
    /** The kind of change that a row describes in a changelog. */
    private RowKind kind = RowKind.INSERT;
    /** The array to store the actual internal format values. */
    private final Object[] fields;

    public SeaTunnelRow(int arity) {
        this.fields = new Object[arity];
    }

    public SeaTunnelRow(Object[] fields) {
        this.fields = fields;
    }

    public void setField(int pos, Object value) {
        this.fields[pos] = value;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public void setRowKind(RowKind kind) {
        this.kind = kind;
    }

    public int getArity() {
        return fields.length;
    }

    public int getTableId() {
        return tableId;
    }

    public RowKind getRowKind() {
        return this.kind;
    }

    public Object[] getFields() {
        return fields;
    }

    public Object getField(int pos) {
        return this.fields[pos];
    }

    public SeaTunnelRow copy() {
        Object[] newFields = new Object[this.getArity()];
        System.arraycopy(this.getFields(), 0, newFields, 0, newFields.length);
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    public SeaTunnelRow copy(int[] indexMapping) {
        Object[] newFields = new Object[indexMapping.length];
        for (int i = 0; i < indexMapping.length; i++) {
            newFields[i] = this.fields[indexMapping[i]];
        }
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(this.getRowKind());
        newRow.setTableId(this.getTableId());
        return newRow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SeaTunnelRow)) {
            return false;
        }
        SeaTunnelRow that = (SeaTunnelRow) o;
        return tableId == that.tableId && kind == that.kind && Arrays.deepEquals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId, kind);
        result = 31 * result + Arrays.deepHashCode(fields);
        return result;
    }

    @Override
    public String toString() {
        return "SeaTunnelRow{" +
            "tableId=" + tableId +
            ", kind=" + kind.shortString() +
            ", fields=" + Arrays.toString(fields) +
            '}';
    }
}
