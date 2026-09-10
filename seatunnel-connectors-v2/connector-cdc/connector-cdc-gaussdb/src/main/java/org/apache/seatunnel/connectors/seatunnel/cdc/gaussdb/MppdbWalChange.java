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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import lombok.Getter;

import java.util.Collections;
import java.util.List;

/** A normalized transaction or row-change record emitted by {@code mppdb_decoding}. */
@Getter
final class MppdbWalChange {

    /** Supported mppdb logical decoding record kinds. */
    enum Type {
        BEGIN,
        COMMIT,
        INSERT,
        UPDATE,
        DELETE
    }

    /** A decoded column value together with the server type identity. */
    @Getter
    static final class ColumnValue {

        /** Column name exactly as emitted by the decoding plugin. */
        private final String name;

        /** PostgreSQL-compatible type OID, or zero when only a type name is available. */
        private final int typeOid;

        /** PostgreSQL-compatible type name, or null when the binary record only contains an OID. */
        private final String typeName;

        /** Text representation after mppdb SQL literal unquoting. */
        private final String value;

        /** Whether the source value is SQL NULL. */
        private final boolean nullValue;

        /** Creates a decoded column value. */
        ColumnValue(String name, int typeOid, String typeName, String value, boolean nullValue) {
            this.name = name;
            this.typeOid = typeOid;
            this.typeName = typeName;
            this.value = value;
            this.nullValue = nullValue;
        }
    }

    /** Record LSN represented with Debezium's unsigned long convention. */
    private final long lsn;

    /** Transaction id when the server includes it, otherwise zero. */
    private final long transactionId;

    /** Logical record kind. */
    private final Type type;

    /** Changed schema for data records. */
    private final String schema;

    /** Changed table for data records. */
    private final String table;

    /** Replica identity values supplied for UPDATE and DELETE. */
    private final List<ColumnValue> oldColumns;

    /** New row values supplied for INSERT and UPDATE. */
    private final List<ColumnValue> newColumns;

    /** Creates an immutable normalized WAL record. */
    MppdbWalChange(
            long lsn,
            long transactionId,
            Type type,
            String schema,
            String table,
            List<ColumnValue> oldColumns,
            List<ColumnValue> newColumns) {
        this.lsn = lsn;
        this.transactionId = transactionId;
        this.type = type;
        this.schema = schema;
        this.table = table;
        this.oldColumns =
                oldColumns == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(oldColumns);
        this.newColumns =
                newColumns == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(newColumns);
    }

    /** Returns whether this record carries a table row change. */
    boolean isDataChange() {
        return type == Type.INSERT || type == Type.UPDATE || type == Type.DELETE;
    }
}
