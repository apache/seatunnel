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

package org.apache.seatunnel.config.impl;

import org.apache.seatunnel.config.ConfigException;
import org.apache.seatunnel.config.ConfigOrigin;

import java.io.ObjectStreamException;
import java.io.Serializable;

abstract class ConfigNumber extends AbstractConfigValue implements Serializable {

    private static final long serialVersionUID = 2L;

    // This is so when we concatenate a number into a string (say it appears in
    // a sentence) we always have it exactly as the person typed it into the
    // config file. It's purely cosmetic; equals/hashCode don't consider this
    // for example.
    protected final String originalText;

    protected ConfigNumber(ConfigOrigin origin, String originalText) {
        super(origin);
        this.originalText = originalText;
    }

    @Override
    public abstract Number unwrapped();

    @Override
    String transformToString() {
        return originalText;
    }

    int intValueRangeChecked(String path) {
        long l = longValue();
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new ConfigException.WrongType(origin(), path, "32-bit integer",
                    "out-of-range value " + l);
        }
        return (int) l;
    }

    protected abstract long longValue();

    protected abstract double doubleValue();

    private boolean isWhole() {
        long asLong = longValue();
        return asLong == doubleValue();
    }

    @Override
    protected boolean canEqual(Object other) {
        return other instanceof ConfigNumber;
    }

    @Override
    public boolean equals(Object other) {
        // note that "origin" is deliberately NOT part of equality
        if (other instanceof ConfigNumber && canEqual(other)) {
            ConfigNumber n = (ConfigNumber) other;
            if (isWhole()) {
                return n.isWhole() && this.longValue() == n.longValue();
            }
            return (!n.isWhole()) && this.doubleValue() == n.doubleValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        // note that "origin" is deliberately NOT part of equality

        // this matches what standard Long.hashCode and Double.hashCode
        // do, though I don't think it really matters.
        long asLong;
        if (isWhole()) {
            asLong = longValue();
        } else {
            asLong = Double.doubleToLongBits(doubleValue());
        }
        return (int) (asLong ^ (asLong >>> 32));
    }

    static ConfigNumber newNumber(ConfigOrigin origin, long number,
                                  String originalText) {
        if (number <= Integer.MAX_VALUE && number >= Integer.MIN_VALUE) {
            return new ConfigInt(origin, (int) number, originalText);
        }
        return new ConfigLong(origin, number, originalText);
    }

    static ConfigNumber newNumber(ConfigOrigin origin, double number,
                                  String originalText) {
        long asLong = (long) number;
        if (asLong == number) {
            return newNumber(origin, asLong, originalText);
        }
        return new ConfigDouble(origin, number, originalText);
    }

    // serialization all goes through SerializedConfigValue
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedConfigValue(this);
    }
}
