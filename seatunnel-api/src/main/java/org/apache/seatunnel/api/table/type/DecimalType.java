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
import java.util.Objects;

public final class DecimalType extends BasicType<BigDecimal> {
    private static final long serialVersionUID = 1L;

    private final int precision;

    private final int scale;

    public DecimalType(int precision, int scale) {
        super(BigDecimal.class, SqlType.DECIMAL);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DecimalType)) {
            return false;
        }
        DecimalType that = (DecimalType) o;
        return this.precision == that.precision && this.scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(precision, scale);
    }

    @Override
    public String toString() {
        return String.format("Decimal(%d, %d)", precision, scale);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
}
