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

final class CommonHandleDecoder {

    private static final int RECORD_KEY_PREFIX_LENGTH = 11;

    private CommonHandleDecoder() {}

    static void restorePrimaryKeyColumns(byte[] recordKey, Object[] values, TiTableInfo tableInfo) {
        if (tableInfo.isPkHandle()) {
            return;
        }

        TiIndexInfo primaryIndex = primaryIndex(tableInfo);
        if (primaryIndex == null || primaryKeyValuesPresent(values, primaryIndex)) {
            return;
        }

        CodecDataInput input = new CodecDataInput(recordKey);
        input.skipBytes(RECORD_KEY_PREFIX_LENGTH);
        for (TiIndexColumn indexColumn : primaryIndex.getIndexColumns()) {
            TiColumnInfo column = tableInfo.getColumn(indexColumn.getOffset());
            values[column.getOffset()] = column.getType().decode(input);
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
