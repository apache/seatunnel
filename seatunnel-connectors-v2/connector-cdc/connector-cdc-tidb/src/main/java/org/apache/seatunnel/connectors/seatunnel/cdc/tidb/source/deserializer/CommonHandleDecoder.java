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
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiIndexColumn;
import org.tikv.common.meta.TiIndexInfo;
import org.tikv.common.meta.TiTableInfo;

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
