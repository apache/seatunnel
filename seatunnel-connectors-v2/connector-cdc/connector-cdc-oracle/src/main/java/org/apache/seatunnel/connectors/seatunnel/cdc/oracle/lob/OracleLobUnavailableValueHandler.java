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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.OracleLobUnavailableValueHandling;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stops Debezium's unavailable-value placeholder from being delivered as Oracle LOB data.
 *
 * <p>DELETE and UPDATE_BEFORE images null the placeholder. INSERT and UPDATE_AFTER images re-select
 * the column by primary key when that is enabled. If re-select cannot run, {@link
 * OracleLobUnavailableValueHandling} decides whether to null the value, fail the task, or keep the
 * placeholder. A {@link java.sql.SQLException} from the lookup uses that same choice and fails the
 * task only when the mode is {@link OracleLobUnavailableValueHandling#FAIL}.
 *
 * <p>SQL NULL LOB columns are not re-selected. This handler runs only when Debezium {@code
 * lob.enabled} is true, so a null is a real null. Selecting it again would overwrite an explicit
 * NULL, including the case where {@code EMPTY_CLOB()} and a later {@code LOB_WRITE} were merged
 * into one event. {@code XMLTYPE} and {@code SYS.XMLTYPE} are handled like CLOB strings.
 */
@Slf4j
public class OracleLobUnavailableValueHandler implements Serializable, AutoCloseable {

    private static final long serialVersionUID = 1L;
    static final String COMMIT_SCN_KEY = "commit_scn";

    private final String placeholder;
    private final byte[] placeholderBytes;
    private final OracleLobUnavailableValueHandling handling;
    private final boolean reselectEnabled;
    private final OracleLobColumnReselector reselector;
    private final Set<String> warnings =
            Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public OracleLobUnavailableValueHandler(
            String placeholder,
            OracleLobUnavailableValueHandling handling,
            boolean reselectEnabled,
            OracleLobColumnReselector reselector) {
        if (placeholder == null || placeholder.isEmpty()) {
            throw new IllegalArgumentException(
                    "LOB unavailable-value placeholder must not be empty");
        }
        this.placeholder = placeholder;
        this.placeholderBytes = placeholder.getBytes();
        this.handling = handling == null ? OracleLobUnavailableValueHandling.NULL : handling;
        this.reselectEnabled = reselectEnabled;
        this.reselector = reselector;
    }

    /**
     * Rewrites placeholder LOB fields on one converted row. Non-LOB columns that happen to contain
     * the same text are left unchanged.
     */
    public SeaTunnelRow handle(SourceRecord record, SeaTunnelRow row, List<CatalogTable> tables) {
        if (row == null || row.getRowKind() == null) {
            return row;
        }
        CatalogTable table = findTable(row.getTableId(), tables);
        if (table == null) {
            return row;
        }
        List<LobField> lobFields = lobFields(table);
        if (lobFields.isEmpty()) {
            return row;
        }
        List<LobField> placeholders = new ArrayList<LobField>();
        for (LobField field : lobFields) {
            if (field.index < row.getArity() && isPlaceholder(row.getField(field.index))) {
                placeholders.add(field);
            }
        }
        if (placeholders.isEmpty()) {
            return row;
        }

        RowKind rowKind = row.getRowKind();
        if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
            for (LobField field : placeholders) {
                row.setField(field.index, null);
            }
            return row;
        }

        if (reselectEnabled && reselector != null) {
            try {
                if (reselect(record, table, row, placeholders)) {
                    return row;
                }
            } catch (Exception e) {
                warnOnce(
                        warningKey(table, "reselect-failed"),
                        "Failed to re-select unavailable Oracle LOB values for table {}. {}",
                        table.getTablePath(),
                        e.toString());
                log.debug("Oracle LOB re-select failed for table {}", table.getTablePath(), e);
            }
        }
        applyHandling(table, row, placeholders);
        return row;
    }

    @Override
    public void close() {
        if (reselector != null) {
            reselector.close();
        }
    }

    private boolean reselect(
            SourceRecord record, CatalogTable table, SeaTunnelRow row, List<LobField> placeholders)
            throws SQLException {
        List<String> primaryKeyColumns = primaryKeyColumns(table);
        if (primaryKeyColumns.isEmpty()) {
            warnOnce(
                    warningKey(table, "no-pk"),
                    "Table {} has unavailable LOB values but no primary key, so they cannot be re-selected. "
                            + "Configure table-names-config primaryKeys or set lob.unavailable-value.handling.",
                    table.getTablePath());
            return false;
        }
        List<Object> primaryKeyValues = new ArrayList<Object>(primaryKeyColumns.size());
        for (String column : primaryKeyColumns) {
            int index = columnIndex(table, column);
            if (index < 0 || index >= row.getArity()) {
                warnOnce(
                        warningKey(table, "pk-missing-" + column),
                        "Primary key column {} was not found on table {} while re-selecting LOB values.",
                        column,
                        table.getTablePath());
                return false;
            }
            Object value = row.getField(index);
            if (value == null) {
                warnOnce(
                        warningKey(table, "pk-null-" + column),
                        "Primary key column {} is null on table {}, so the LOB value cannot be re-selected.",
                        column,
                        table.getTablePath());
                return false;
            }
            primaryKeyValues.add(value);
        }

        TableId sourceTable = sourceTable(record);
        String schema = sourceTable == null ? null : sourceTable.schema();
        String tableName = sourceTable == null ? null : sourceTable.table();
        if (tableName == null || tableName.trim().isEmpty()) {
            schema = table.getTablePath().getSchemaName();
            tableName = table.getTablePath().getTableName();
        }
        List<String> columnNames = new ArrayList<String>(placeholders.size());
        for (LobField field : placeholders) {
            columnNames.add(field.name);
        }
        OracleLobColumnReselector.ReselectResult result =
                reselector.reselect(
                        schema,
                        tableName,
                        columnNames,
                        primaryKeyColumns,
                        primaryKeyValues,
                        commitScn(record));
        if (!result.isFound()) {
            warnOnce(
                    warningKey(table, "row-missing"),
                    "LOB re-select found no row for table {} using the event primary key.",
                    table.getTablePath());
            return false;
        }
        Map<String, Object> values = result.getValues();
        boolean replacedAll = true;
        for (LobField field : placeholders) {
            if (values.containsKey(field.name)) {
                row.setField(field.index, adapt(values.get(field.name), field.dataType));
            } else {
                replacedAll = false;
            }
        }
        return replacedAll;
    }

    private void applyHandling(CatalogTable table, SeaTunnelRow row, List<LobField> placeholders) {
        List<LobField> remaining = new ArrayList<LobField>();
        for (LobField field : placeholders) {
            if (isPlaceholder(row.getField(field.index))) {
                remaining.add(field);
            }
        }
        if (remaining.isEmpty()) {
            return;
        }
        if (handling == OracleLobUnavailableValueHandling.WARN_AND_KEEP) {
            warnOnce(
                    warningKey(table, "keep"),
                    "Keeping the unavailable-value placeholder for LOB columns {} on table {}. "
                            + "The sink can overwrite the real LOB with this sentinel.",
                    names(remaining),
                    table.getTablePath());
            return;
        }
        if (handling == OracleLobUnavailableValueHandling.FAIL) {
            throw new SeaTunnelException(
                    String.format(
                            "Oracle CDC LOB column(s) %s on table %s contain the unavailable-value placeholder"
                                    + " and could not be re-selected. Grant FLASHBACK on the table, provide a primary key,"
                                    + " or set lob.unavailable-value.handling to null or warn_and_keep.",
                            names(remaining), table.getTablePath()));
        }
        for (LobField field : remaining) {
            row.setField(field.index, null);
        }
    }

    private boolean isPlaceholder(Object value) {
        if (value instanceof String) {
            return placeholder.equals(value);
        }
        if (value instanceof byte[]) {
            return Arrays.equals(placeholderBytes, (byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return Arrays.equals(placeholderBytes, bytes);
        }
        return false;
    }

    private static Object adapt(Object value, SeaTunnelDataType<?> dataType) {
        if (value == null || dataType == null) {
            return value;
        }
        SqlType sqlType = dataType.getSqlType();
        if (sqlType == SqlType.STRING && value instanceof byte[]) {
            return new String((byte[]) value);
        }
        if (sqlType == SqlType.BYTES && value instanceof String) {
            return ((String) value).getBytes();
        }
        if (sqlType == SqlType.BYTES && value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        return value;
    }

    private static CatalogTable findTable(String tableId, List<CatalogTable> tables) {
        if (tables == null || tables.isEmpty()) {
            return null;
        }
        if (tableId != null) {
            for (CatalogTable table : tables) {
                if (table.getTablePath().toString().equals(tableId)) {
                    return table;
                }
            }
        }
        if (tables.size() == 1) {
            return tables.get(0);
        }
        return null;
    }

    private static List<LobField> lobFields(CatalogTable table) {
        List<LobField> fields = new ArrayList<LobField>();
        int index = 0;
        for (Column column : table.getTableSchema().getColumns()) {
            if (!column.isPhysical()) {
                continue;
            }
            if (isLobSourceType(column.getSourceType())) {
                fields.add(new LobField(index, column.getName(), column.getDataType()));
            }
            index++;
        }
        return fields;
    }

    static boolean isLobSourceType(String sourceType) {
        if (sourceType == null) {
            return false;
        }
        String normalized = sourceType.trim().toUpperCase(Locale.ROOT);
        int parenthesis = normalized.indexOf('(');
        if (parenthesis >= 0) {
            normalized = normalized.substring(0, parenthesis).trim();
        }
        return "CLOB".equals(normalized)
                || "NCLOB".equals(normalized)
                || "BLOB".equals(normalized)
                || "XMLTYPE".equals(normalized)
                || "SYS.XMLTYPE".equals(normalized);
    }

    private static List<String> primaryKeyColumns(CatalogTable table) {
        PrimaryKey primaryKey = table.getTableSchema().getPrimaryKey();
        if (primaryKey == null || primaryKey.getColumnNames() == null) {
            return Collections.emptyList();
        }
        return primaryKey.getColumnNames();
    }

    private static int columnIndex(CatalogTable table, String columnName) {
        List<Column> columns = table.getTableSchema().getColumns();
        int index = 0;
        int caseInsensitive = -1;
        for (Column column : columns) {
            if (!column.isPhysical()) {
                continue;
            }
            if (column.getName().equals(columnName)) {
                return index;
            }
            if (caseInsensitive < 0 && column.getName().equalsIgnoreCase(columnName)) {
                caseInsensitive = index;
            }
            index++;
        }
        return caseInsensitive;
    }

    private static TableId sourceTable(SourceRecord record) {
        if (record == null || !(record.value() instanceof Struct)) {
            return null;
        }
        try {
            return SourceRecordUtils.getTableId(record);
        } catch (Exception e) {
            return null;
        }
    }

    private static String commitScn(SourceRecord record) {
        if (record == null || !(record.value() instanceof Struct)) {
            return null;
        }
        Struct value = (Struct) record.value();
        if (value.schema().field(Envelope.FieldName.SOURCE) == null) {
            return null;
        }
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null || source.schema().field(COMMIT_SCN_KEY) == null) {
            return null;
        }
        Object commitScn = source.get(COMMIT_SCN_KEY);
        return commitScn == null ? null : commitScn.toString();
    }

    private void warnOnce(String key, String message, Object... arguments) {
        if (warnings.add(key)) {
            log.warn(message, arguments);
        }
    }

    private static String warningKey(CatalogTable table, String reason) {
        return table.getTablePath() + ":" + reason;
    }

    private static List<String> names(List<LobField> fields) {
        List<String> names = new ArrayList<String>(fields.size());
        for (LobField field : fields) {
            names.add(field.name);
        }
        return names;
    }

    private static final class LobField {
        private final int index;
        private final String name;
        private final SeaTunnelDataType<?> dataType;

        private LobField(int index, String name, SeaTunnelDataType<?> dataType) {
            this.index = index;
            this.name = name;
            this.dataType = dataType;
        }
    }
}
