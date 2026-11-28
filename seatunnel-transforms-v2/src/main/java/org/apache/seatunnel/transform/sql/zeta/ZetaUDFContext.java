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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

/** Runtime context for zeta udf execution. */
public class ZetaUDFContext {
    private static final Object[] EMPTY_FIELDS = new Object[0];

    /**
     * Serialized input metadata snapshot that must not be mutable by a UDF.
     *
     * <p>Serialization prevents nested schema values from sharing mutable references with UDF code.
     */
    @Nullable private final byte[] inputCatalogTableSnapshot;

    @Nullable private String rawTableId;
    private boolean tableIdIsNull = true;
    @Nullable private String database;
    @Nullable private String schema;
    @Nullable private String table;
    @Nullable private IllegalArgumentException tablePathParseException;
    private boolean tablePathResolved;
    private RowKind rowKind = RowKind.INSERT;
    private Object[] allFields = EMPTY_FIELDS;

    /**
     * Creates an empty UDF context.
     *
     * <p>The row-level fields remain unbound until {@link #update(SeaTunnelRow)} is called.
     */
    public ZetaUDFContext() {
        this((CatalogTable) null);
    }

    /**
     * Creates a UDF context with an isolated input table metadata snapshot.
     *
     * @param inputCatalogTable input table metadata, or null when unavailable
     */
    public ZetaUDFContext(@Nullable CatalogTable inputCatalogTable) {
        this.inputCatalogTableSnapshot =
                inputCatalogTable == null ? null : serializeCatalogTable(inputCatalogTable);
    }

    private ZetaUDFContext(ZetaUDFContext source) {
        this.inputCatalogTableSnapshot = source.inputCatalogTableSnapshot;
    }

    ZetaUDFContext initializationContext() {
        return new ZetaUDFContext(this);
    }

    /**
     * Updates the row-level context for UDF evaluation.
     *
     * @param row input row
     * @return this context
     */
    public ZetaUDFContext update(SeaTunnelRow row) {
        return update(row.getFields(), row);
    }

    /**
     * Updates the row-level context for UDF evaluation.
     *
     * @param fields row fields
     * @param row input row
     * @return this context
     */
    public ZetaUDFContext update(Object[] fields, SeaTunnelRow row) {
        this.allFields = fields == null ? EMPTY_FIELDS : fields;
        this.rowKind = row.getRowKind();
        updateTableId(row.getTableId());
        return this;
    }

    private void updateTableId(String tableId) {
        boolean newTableIdIsNull = tableId == null;
        if (Objects.equals(this.rawTableId, tableId) && this.tableIdIsNull == newTableIdIsNull) {
            return;
        }
        this.rawTableId = tableId;
        this.tableIdIsNull = newTableIdIsNull;
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

    /**
     * Gets the raw table identifier of the current row.
     *
     * @return raw table identifier, or null before a row is bound
     */
    @Nullable public String getRawTableId() {
        return rawTableId;
    }

    /**
     * Gets the database name of the current row.
     *
     * @return database name, or null when no table identifier is bound
     */
    @Nullable public String getDatabase() {
        resolveTablePathIfNeeded();
        return database;
    }

    /**
     * Gets the schema name of the current row.
     *
     * @return schema name, or null when no table identifier is bound
     */
    @Nullable public String getSchema() {
        resolveTablePathIfNeeded();
        return schema;
    }

    /**
     * Gets the table name of the current row.
     *
     * @return table name, or null when no table identifier is bound
     */
    @Nullable public String getTable() {
        resolveTablePathIfNeeded();
        return table;
    }

    /**
     * Gets the row kind of the current row.
     *
     * @return row kind
     */
    public RowKind getRowKind() {
        return rowKind;
    }

    /**
     * Gets the field values of the current row.
     *
     * @return current row fields
     */
    public Object[] getAllFields() {
        return allFields;
    }

    /**
     * Gets a deeply isolated snapshot of the input catalog table.
     *
     * @return input catalog table, or null when table metadata is unavailable
     */
    @Nullable public CatalogTable getInputCatalogTable() {
        return inputCatalogTableSnapshot == null
                ? null
                : deserializeCatalogTable(inputCatalogTableSnapshot);
    }

    private static byte[] serializeCatalogTable(CatalogTable catalogTable) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(catalogTable);
            objectOutput.flush();
            return output.toByteArray();
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to snapshot input catalog table", exception);
        }
    }

    private static CatalogTable deserializeCatalogTable(byte[] snapshot) {
        try (ObjectInputStream objectInput =
                new ObjectInputStream(new ByteArrayInputStream(snapshot))) {
            return (CatalogTable) objectInput.readObject();
        } catch (IOException | ClassNotFoundException exception) {
            throw new IllegalArgumentException(
                    "Unable to read input catalog table snapshot", exception);
        }
    }
}
