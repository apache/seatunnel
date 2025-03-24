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

package org.apache.seatunnel.connectors.seatunnel.common.source.arrow;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

public class SeaTunnelDataTypeHolder {
    private final String filedName;
    private final int flag;

    public SeaTunnelDataTypeHolder(String filedName, int flag) {
        this.filedName = filedName;
        this.flag = flag;
    }

    public String getFiledName() {
        return filedName;
    }

    public int getFlag() {
        return flag;
    }

    public SeaTunnelDataType getSeatunnelDataType() {
        switch (filedName) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "int":
                return BasicType.INT_TYPE;
            case "long":
                return BasicType.LONG_TYPE;
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
                return BasicType.DOUBLE_TYPE;
            case "string1":
            case "string2":
            case "string3":
                return BasicType.STRING_TYPE;
            case "decimal":
                return new DecimalType(10, 2);
            case "timestamp1":
            case "timestamp2":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "time":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "date1":
            case "date2":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "array1":
                return ArrayType.INT_ARRAY_TYPE;
            case "array2":
                return ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE;
            case "map":
                return new MapType(BasicType.STRING_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE);
            default:
                return null;
        }
    }
}
