/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.util;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {

    public static byte[] convertRowKey(String rowKey, boolean isBinary) {
        if (StringUtils.isEmpty(rowKey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        }

        if (isBinary) {
            return Bytes.toBytesBinary(rowKey);
        } else {
            return Bytes.toBytes(rowKey);
        }
    }

    public static void validateRowKeyRange(byte[] startRowKey, byte[] endRowKey) {
        if (startRowKey.length > 0 && endRowKey.length > 0) {
            if (Bytes.compareTo(startRowKey, endRowKey) > 0) {
                throw new IllegalArgumentException("startRowkey can't be bigger than endRowkey");
            }
        }
    }
}
