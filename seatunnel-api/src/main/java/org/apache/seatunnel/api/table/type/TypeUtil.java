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

package org.apache.seatunnel.api.table.type;

public class TypeUtil {

    /** Check if the data type can be converted to another data type. */
    public static boolean canConvert(SeaTunnelDataType<?> from, SeaTunnelDataType<?> to) {
        // any type can be converted to string
        if (from == to || to.getSqlType() == SqlType.STRING) {
            return true;
        }
        if (from.getSqlType() == SqlType.TINYINT) {
            return to.getSqlType() == SqlType.SMALLINT
                    || to.getSqlType() == SqlType.INT
                    || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.SMALLINT) {
            return to.getSqlType() == SqlType.INT || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.INT) {
            return to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.FLOAT) {
            return to.getSqlType() == SqlType.DOUBLE;
        }
        return false;
    }
}
