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

package org.apache.seatunnel.transform.calcite.udf;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Thread-local row context for Calcite UDFs. Since Calcite code generation calls {@code eval} as a
 * static method, this context is the only way for UDFs to access row-level metadata.
 *
 * <p>Usage in a custom UDF:
 *
 * <pre>{@code
 * public static String eval(String input) {
 *     CalciteUdfContext ctx = CalciteUdfContext.current();
 *     RowKind kind = ctx.getRowKind();
 *     // ...
 * }
 * }</pre>
 */
@Slf4j
public final class CalciteUdfContext {

    private static final ThreadLocal<CalciteUdfContext> HOLDER = new ThreadLocal<>();

    private @Nullable String rawTableId;
    private boolean tablePathResolved;
    private @Nullable String database;
    private @Nullable String schema;
    private @Nullable String table;
    private @Nullable RowKind rowKind;

    /**
     * Returns the context for the current row being processed. Returns {@code null} if called
     * outside of Calcite Transform execution.
     */
    @Nullable public static CalciteUdfContext current() {
        return HOLDER.get();
    }

    public static void set(CalciteUdfContext ctx) {
        HOLDER.set(ctx);
    }

    public static void clear() {
        HOLDER.remove();
    }

    public void update(String tableId, RowKind rowKind) {
        this.rowKind = rowKind;
        updateTableId(tableId);
    }

    private void updateTableId(String tableId) {
        if (Objects.equals(this.rawTableId, tableId)) {
            return;
        }
        this.rawTableId = tableId;
        this.database = null;
        this.schema = null;
        this.table = null;
        this.tablePathResolved = false;
    }

    private void resolveTablePathIfNeeded() {
        if (tablePathResolved) {
            return;
        }
        tablePathResolved = true;
        if (rawTableId == null) {
            return;
        }
        try {
            TablePath tablePath = TablePath.of(rawTableId);
            this.database = tablePath.getDatabaseName();
            this.schema = tablePath.getSchemaName();
            this.table = tablePath.getTableName();
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Failed to parse tableId '{}' as TablePath, "
                            + "getDatabase()/getSchema()/getTable() will return null",
                    rawTableId,
                    e);
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

    @Nullable public RowKind getRowKind() {
        return rowKind;
    }
}
