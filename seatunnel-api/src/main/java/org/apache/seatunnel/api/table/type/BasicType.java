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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class BasicType<T> implements SeaTunnelDataType<T> {
    private static final long serialVersionUID = 1L;

    public static final BasicType<String> STRING_TYPE = new BasicType<>(String.class);
    public static final BasicType<Boolean> BOOLEAN_TYPE = new BasicType<>(Boolean.class);
    public static final BasicType<Byte> BYTE_TYPE = new BasicType<>(Byte.class);
    public static final BasicType<Short> SHORT_TYPE = new BasicType<>(Short.class);
    public static final BasicType<Integer> INT_TYPE = new BasicType<>(Integer.class);
    public static final BasicType<Long> LONG_TYPE = new BasicType<>(Long.class);
    public static final BasicType<Float> FLOAT_TYPE = new BasicType<>(Float.class);
    public static final BasicType<Double> DOUBLE_TYPE = new BasicType<>(Double.class);
    public static final BasicType<Character> CHAR_TYPE = new BasicType<>(Character.class);

    public static final BasicType<BigInteger> BIG_INT_TYPE = new BasicType<>(BigInteger.class);
    public static final BasicType<BigDecimal> BIG_DECIMAL_TYPE = new BasicType<>(BigDecimal.class);
    public static final BasicType<Instant> INSTANT_TYPE = new BasicType<>(Instant.class);
    public static final BasicType<Void> VOID_TYPE = new BasicType<>(Void.class);
    public static final BasicType<Date> DATE_TYPE = new BasicType<>(Date.class);

    /**
     * The physical type class.
     */
    private final Class<T> physicalTypeClass;

    private BasicType(Class<T> physicalTypeClass) {
        if (physicalTypeClass == null) {
            throw new IllegalArgumentException("physicalTypeClass cannot be null");
        }
        this.physicalTypeClass = physicalTypeClass;
    }

    @Override
    public Class<T> getTypeClass() {
        return this.physicalTypeClass;
    }

    @Override
    public int hashCode() {
        return this.physicalTypeClass.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        BasicType<?> other = (BasicType<?>) obj;
        return this.physicalTypeClass.equals(other.physicalTypeClass);
    }

    @Override
    public String toString() {
        return "BasicType{" +
            "physicalTypeClass=" + physicalTypeClass +
            '}';
    }
}
