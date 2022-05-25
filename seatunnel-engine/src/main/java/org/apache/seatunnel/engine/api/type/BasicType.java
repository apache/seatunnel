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

package org.apache.seatunnel.engine.api.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class BasicType<T> implements SeaTunnelDataType<T> {

    public static final BasicType<Boolean> BOOLEAN = new BasicType<>(Boolean.class);
    public static final BasicType<String> STRING = new BasicType<>(String.class);
    public static final BasicType<Date> DATE = new BasicType<>(Date.class);
    public static final BasicType<Double> DOUBLE = new BasicType<>(Double.class);
    public static final BasicType<Integer> INTEGER = new BasicType<>(Integer.class);
    public static final BasicType<Long> LONG = new BasicType<>(Long.class);
    public static final BasicType<Float> FLOAT = new BasicType<>(Float.class);
    public static final BasicType<Byte> BYTE = new BasicType<>(Byte.class);
    public static final BasicType<Short> SHORT = new BasicType<>(Short.class);
    public static final BasicType<Character> CHARACTER = new BasicType<>(Character.class);
    public static final BasicType<BigInteger> BIG_INTEGER = new BasicType<>(BigInteger.class);
    public static final BasicType<BigDecimal> BIG_DECIMAL = new BasicType<>(BigDecimal.class);
    public static final BasicType<Instant> INSTANT = new BasicType<>(Instant.class);
    public static final BasicType<Void> NULL = new BasicType<>(Void.class);

    /**
     * The physical type class.
     */
    private final Class<T> physicalTypeClass;

    public BasicType(Class<T> physicalTypeClass) {
        if (physicalTypeClass == null) {
            throw new IllegalArgumentException("physicalTypeClass cannot be null");
        }
        this.physicalTypeClass = physicalTypeClass;
    }

    public Class<T> getPhysicalTypeClass() {
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
