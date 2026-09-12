/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.utils;

import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Utils to obtain the keyRange of table. */
public class TableKeyRangeUtils {
    public static KeyRange getTableKeyRange(final long tableId) {
        return KeyRangeUtils.makeCoprocRange(
                RowKey.createMin(tableId).toByteString(),
                RowKey.createBeyondMax(tableId).toByteString());
    }

    public static List<KeyRange> getTableKeyRanges(final long tableId, final int num) {
        if (num <= 0) {
            throw new IllegalArgumentException("Illegal value of num");
        }

        if (num == 1) {
            return Collections.singletonList(getTableKeyRange(tableId));
        }

        final long delta =
                BigInteger.valueOf(Long.MAX_VALUE)
                        .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
                        .divide(BigInteger.valueOf(num))
                        .longValueExact();
        final List<KeyRange> keyRanges = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            final RowKey startKey =
                    (i == 0)
                            ? RowKey.createMin(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * i);
            final RowKey endKey =
                    (i == num - 1)
                            ? RowKey.createBeyondMax(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * (i + 1));
            keyRanges.add(
                    KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
        }
        return Collections.unmodifiableList(keyRanges);
    }

    public static boolean isRecordKey(final byte[] key) {
        return key.length > 10 && key[9] == '_' && key[10] == 'r';
    }
}
