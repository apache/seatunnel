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

import java.util.Objects;

public class BasicType<T> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 2L;

    public static final BasicType<String> STRING_TYPE = new BasicType<>(String.class, SqlType.STRING);
    public static final BasicType<Boolean> BOOLEAN_TYPE = new BasicType<>(Boolean.class, SqlType.BOOLEAN);
    public static final BasicType<Byte> BYTE_TYPE = new BasicType<>(Byte.class, SqlType.TINYINT);
    public static final BasicType<Short> SHORT_TYPE = new BasicType<>(Short.class, SqlType.SMALLINT);
    public static final BasicType<Integer> INT_TYPE = new BasicType<>(Integer.class, SqlType.INT);
    public static final BasicType<Long> LONG_TYPE = new BasicType<>(Long.class, SqlType.BIGINT);
    public static final BasicType<Float> FLOAT_TYPE = new BasicType<>(Float.class, SqlType.FLOAT);
    public static final BasicType<Double> DOUBLE_TYPE = new BasicType<>(Double.class, SqlType.DOUBLE);
    public static final BasicType<Void> VOID_TYPE = new BasicType<>(Void.class, SqlType.NULL);

    // --------------------------------------------------------------------------------------------

    /**
     * The physical type class.
     */
    private final Class<T> typeClass;
    private final SqlType sqlType;

    protected BasicType(Class<T> typeClass, SqlType sqlType) {
        this.typeClass = typeClass;
        this.sqlType = sqlType;
    }

    @Override
    public Class<T> getTypeClass() {
        return this.typeClass;
    }

    @Override
    public SqlType getSqlType() {
        return this.sqlType;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BasicType)) {
            return false;
        }
        BasicType<?> that = (BasicType<?>) obj;
        return Objects.equals(typeClass, that.typeClass) && Objects.equals(sqlType, that.sqlType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass, sqlType);
    }

    @Override
    public String toString() {
        return sqlType.toString();
    }
}
