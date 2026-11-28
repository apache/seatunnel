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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer;

import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.meta.Collation;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType;
import org.tikv.common.types.MySQLType;
import org.tikv.common.types.StringType;

import java.util.Locale;

/**
 * Restores omitted clustered primary-key columns after the TiKV row-value decoder has run. Already
 * restored row values remain authoritative, including original strings under new collations.
 */
final class CommonHandleDecoder {

    // TiKV record prefix: one table marker, eight table-ID bytes, and the two-byte "_r" marker.
    private static final int RECORD_KEY_PREFIX_LENGTH = 11;

    private CommonHandleDecoder() {}

    /**
     * Decodes every key component to keep composite-key offsets aligned. Only missing value slots
     * are filled so row-value collation originals are never overwritten.
     */
    static void restorePrimaryKeyColumns(
            byte[] recordKey, Object[] values, TiTableInfo tableInfo, boolean clusteredPrimaryKey) {
        if (!clusteredPrimaryKey || tableInfo.isPkHandle()) {
            return;
        }

        TiIndexInfo primaryIndex = primaryIndex(tableInfo);
        if (primaryIndex == null) {
            throw new IllegalArgumentException(
                    "Missing clustered primary index metadata for TiDB table "
                            + tableInfo.getName());
        }
        if (primaryKeyValuesPresent(values, primaryIndex)) {
            return;
        }

        if (recordKey.length <= RECORD_KEY_PREFIX_LENGTH
                || recordKey[0] != 't'
                || recordKey[9] != '_'
                || recordKey[10] != 'r') {
            throw new IllegalArgumentException(
                    "Invalid TiDB common-handle record key for table " + tableInfo.getName());
        }
        CodecDataInput input = new CodecDataInput(recordKey);
        input.skipBytes(RECORD_KEY_PREFIX_LENGTH);
        for (TiIndexColumn indexColumn : primaryIndex.getIndexColumns()) {
            TiColumnInfo column = tableInfo.getColumn(indexColumn.getOffset());
            Object decoded = column.getType().decode(input);
            // New-collation string originals may already be restored from the row value.
            if (values[column.getOffset()] == null) {
                values[column.getOffset()] = decoded;
            }
        }
    }

    /**
     * Returns the first primary-key column whose key encoding cannot reproduce the stored value
     * when a delete row carries no row image, or {@code null} when every key component is
     * reversible.
     *
     * <p>A string column is only reversible when its key bytes are the stored value itself. A
     * collation that orders by weights stores those weights, and even a {@code *_bin} collation is
     * not byte-preserving for a variable-length string, so decoding the key alone can yield a value
     * the stored row never had.
     */
    static TiColumnInfo unrecoverableKeyOnlyColumn(TiTableInfo tableInfo) {
        TiIndexInfo primaryIndex = primaryIndex(tableInfo);
        if (primaryIndex == null) {
            return null;
        }
        for (TiIndexColumn indexColumn : primaryIndex.getIndexColumns()) {
            TiColumnInfo column = tableInfo.getColumn(indexColumn.getOffset());
            if (column.getType() instanceof StringType
                    && !isReversibleStringKey(column.getType())) {
                return column;
            }
        }
        return null;
    }

    /**
     * Resolves whether the key encoding of a string column reproduces the stored value exactly, so
     * an image-less delete can rebuild the row identity from the key alone.
     *
     * <p>The {@code binary} collation stores the value bytes themselves and is reversible. A {@code
     * *_bin} collation is a padding collation in TiDB: its comparison key drops trailing spaces, so
     * a stored {@code VARCHAR} such as {@code "bob "} is encoded like {@code "bob"} and the
     * original can no longer be recovered from the key. Emitting the trimmed value would make a
     * downstream target with a different equality contract delete or miss the wrong row, so a
     * variable-length string is rejected here. {@code CHAR} is exempt because its stored value is
     * space-trimmed by the type itself, which is the same type distinction TiDB makes when it
     * decides whether a binary-collated column needs restored row data. Unknown or unspecified
     * collations are treated as not reversible.
     *
     * @param columnType type of the string column, including its collation and length semantics
     * @return true only when the key bytes are the stored value
     */
    static boolean isReversibleStringKey(DataType columnType) {
        String collationName = Collation.translate(columnType.getCollationCode());
        if (collationName == null || collationName.isEmpty()) {
            return false;
        }
        String normalized = collationName.toLowerCase(Locale.ROOT);
        if ("binary".equals(normalized)) {
            return true;
        }
        if (!normalized.endsWith("_bin")) {
            return false;
        }
        return columnType.getType() == MySQLType.TypeString;
    }

    private static TiIndexInfo primaryIndex(TiTableInfo tableInfo) {
        for (TiIndexInfo index : tableInfo.getIndices()) {
            if (index.isPrimary()) {
                return index;
            }
        }
        return null;
    }

    private static boolean primaryKeyValuesPresent(Object[] values, TiIndexInfo primaryIndex) {
        for (TiIndexColumn indexColumn : primaryIndex.getIndexColumns()) {
            if (values[indexColumn.getOffset()] == null) {
                return false;
            }
        }
        return true;
    }
}
