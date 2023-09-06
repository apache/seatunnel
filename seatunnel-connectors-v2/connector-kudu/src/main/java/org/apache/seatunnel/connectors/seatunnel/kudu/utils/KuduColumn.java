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

package org.apache.seatunnel.connectors.seatunnel.kudu.utils;

import org.apache.kudu.Type;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class KuduColumn {
    public static int KeyColCompare(Object colVal1, Object colVal2, Type colType) {
        switch (colType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return (int) colVal1 - (int) colVal2;
            case UNIXTIME_MICROS:
                return ((Timestamp) colVal1).compareTo((Timestamp) colVal2);
            case STRING:
                return (String.valueOf(colVal1)).compareTo(String.valueOf(colVal2));
            case DECIMAL:
                return ((BigDecimal) colVal1).compareTo((BigDecimal) colVal2);
            case BINARY:
                return compareBinary((byte[]) colVal1, (byte[]) colVal2);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + colType);
        }
    }

    private static int compareBinary(byte[] byteArray1, byte[] byteArray2) {
        int result = 0;
        if (byteArray1.length != byteArray2.length) {
            result = byteArray1.length - byteArray2.length;
        } else {
            for (int i = 0; i < byteArray1.length; i++) {
                if (byteArray1[i] != byteArray2[i]) {
                    result = byteArray1[i] - byteArray2[i];
                    break;
                }
            }
        }
        return result;
    }

    public static Serializable[] getSerializable(Type colType, Object... colVals) {
        Serializable[] serializes = new Serializable[colVals.length];
        int i = 0;
        for (Object value : colVals) {
            switch (colType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    serializes[i] = (Long) value;
                    break;
                case UNIXTIME_MICROS:
                    serializes[i] = (Timestamp) value;
                    break;
                case STRING:
                    serializes[i] = String.valueOf(value);
                    break;
                case DECIMAL:
                    serializes[i] = (BigDecimal) value;
                    break;
                case BINARY:
                    serializes[i] = (byte[]) value;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + colType);
            }
            i++;
        }
        return serializes;
    }

    public static Object addValue(Type colType, Object colVal) {
        switch (colType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return ((Long) colVal) + 1;
            case UNIXTIME_MICROS:
                LocalDateTime localDateTime = ((Timestamp) colVal).toLocalDateTime().plusSeconds(1);
                return Timestamp.valueOf(localDateTime);
            case STRING:
                return String.valueOf(colVal) + '1';
            case DECIMAL:
                return ((BigDecimal) colVal).add(new BigDecimal(1));
            case BINARY:
                byte[] colValByte = (byte[]) colVal;
                byte[] addByte = new byte[colValByte.length + 1];
                System.arraycopy(colValByte, 0, addByte, 0, colValByte.length);
                addByte[colValByte.length] = 1;
                return addByte;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + colType);
        }
    }
}
