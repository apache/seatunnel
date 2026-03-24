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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import javax.annotation.Nullable;

import java.util.Objects;

/** Runtime context for zeta udf execution. */
public class ZetaUDFContext {
    private static final Object[] EMPTY_FIELDS = new Object[0];

    @Nullable private String rawTableId;
    private boolean tableIdIsNull;
    @Nullable private String database;
    @Nullable private String schema;
    @Nullable private String table;
    @Nullable private IllegalArgumentException tablePathParseException;
    private boolean tablePathResolved;
    private RowKind rowKind = RowKind.INSERT;
    private Object[] allFields = EMPTY_FIELDS;

    public ZetaUDFContext update(SeaTunnelRow row) {
        return update(row.getFields(), row);
    }

    public ZetaUDFContext update(Object[] fields, SeaTunnelRow row) {
        this.allFields = fields == null ? EMPTY_FIELDS : fields;
        this.rowKind = row.getRowKind();
        updateTableId(row.getTableId());
        return this;
    }

    private void updateTableId(String tableId) {
        if (Objects.equals(this.rawTableId, tableId)) {
            return;
        }
        this.rawTableId = tableId;
        this.tableIdIsNull = tableId == null;
        this.database = null;
        this.schema = null;
        this.table = null;
        this.tablePathParseException = null;
        this.tablePathResolved = false;
    }

    private void resolveTablePathIfNeeded() {
        if (tablePathResolved) {
            if (tablePathParseException != null) {
                throw tablePathParseException;
            }
            return;
        }
        tablePathResolved = true;

        if (tableIdIsNull) {
            return;
        }

        try {
            TablePath tablePath = TablePath.of(rawTableId);
            this.database = tablePath.getDatabaseName();
            this.schema = tablePath.getSchemaName();
            this.table = tablePath.getTableName();
        } catch (IllegalArgumentException exception) {
            this.tablePathParseException = exception;
            throw exception;
        }
    }

    @Nullable public String getRawTableId() {
        return rawTableId;
    }

    @Nullable public String getDatabase() {
        resolveTablePathIfNeeded();
        return database;
    }

    @Nullable public String getSchema() {
        resolveTablePathIfNeeded();
        return schema;
    }

    @Nullable public String getTable() {
        resolveTablePathIfNeeded();
        return table;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public Object[] getAllFields() {
        return allFields;
    }
}
